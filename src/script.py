#!/usr/bin/env python3

from __future__ import annotations

import argparse
import asyncio
import collections
import dataclasses
import json
import logging
import os
import pathlib
import urllib.parse
from typing import Dict, List, Sequence, Set

from plants.committer import Committer
from plants.environment import Environment
from plants.external import allow_external_calls
from playlist_types import (
    PublishedPlaylist,
    PublishedPlaylistID,
    ScrapedPlaylist,
    ScrapedPlaylistID,
)
from spotify import Spotify

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger: logging.Logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class PlaylistMapping:
    scraped_playlist_id: ScrapedPlaylistID
    published_playlist_ids: Sequence[PublishedPlaylistID]


@dataclasses.dataclass(frozen=True)
class PlaylistMappings:
    mappings: Sequence[PlaylistMapping]

    @classmethod
    def from_json(cls, content: str) -> PlaylistMappings:
        playlists = json.loads(content)
        assert isinstance(playlists, dict)

        mappings: List[PlaylistMapping] = []
        assert isinstance(playlists["mappings"], list)
        for mapping in playlists["mappings"]:
            assert isinstance(mapping, dict)

            scraped_playlist_id = mapping["scraped_playlist_id"]
            assert isinstance(scraped_playlist_id, str)
            scraped_playlist_id = ScrapedPlaylistID(scraped_playlist_id)

            published_playlist_ids: List[PublishedPlaylistID] = []
            assert isinstance(mapping["published_playlist_ids"], list)
            for published_playlist_id in mapping["published_playlist_ids"]:
                assert isinstance(published_playlist_id, str)
                published_playlist_ids.append(
                    PublishedPlaylistID(published_playlist_id)
                )

            mappings.append(
                PlaylistMapping(
                    scraped_playlist_id=scraped_playlist_id,
                    published_playlist_ids=published_playlist_ids,
                )
            )

        cls._ensure_no_overlaps(mappings=mappings)
        return PlaylistMappings(mappings=mappings)

    @classmethod
    def _ensure_no_overlaps(cls, mappings: Sequence[PlaylistMapping]) -> None:
        counter = collections.Counter()
        for mapping in mappings:
            counter.update(mapping.published_playlist_ids)
        overlaps = sorted(x for x, count in counter.items() if count > 1)
        if overlaps:
            raise Exception(f"Some published IDs appear more than once: {overlaps}")

    def to_json(self) -> str:
        return json.dumps(
            dataclasses.asdict(self),
            indent=2,
            sort_keys=True,
        )


def get_scraped_playlists(playlists_dir: pathlib.Path) -> List[ScrapedPlaylist]:
    logger.info(f"Reading playlists from {playlists_dir}")

    json_files: List[pathlib.Path] = []
    for path in (playlists_dir / "cumulative").iterdir():
        if str(path).endswith(".json"):
            json_files.append(path)

    scraped_playlists: List[ScrapedPlaylist] = []
    for path in json_files:
        with open(path, "r") as f:
            playlist = json.load(f)
        track_ids: Set[str] = set()
        for track in playlist["tracks"]:
            track_id = track["url"].split("/")[-1]
            track_ids.add(track_id)
        scraped_playlists.append(
            ScrapedPlaylist(
                playlist_id=ScrapedPlaylistID(playlist["url"].split("/")[-1]),
                name=playlist["name"] + " (Cumulative)",
                description=playlist["description"],
                track_ids=track_ids,
            )
        )

    return scraped_playlists


async def publish(playlists_dir: pathlib.Path, prod: bool) -> None:

    # Check nonempty to fail fast
    client_id = Environment.get_env("SPOTIFY_CLIENT_ID")
    client_secret = Environment.get_env("SPOTIFY_CLIENT_SECRET")
    refresh_token = Environment.get_env("SPOTIFY_REFRESH_TOKEN")
    assert client_id and client_secret and refresh_token

    # Initialize Spotify client
    access_token = await Spotify.get_user_access_token(
        client_id, client_secret, refresh_token
    )
    spotify = Spotify(access_token)
    try:
        await publish_impl(spotify, playlists_dir, prod)
    finally:
        await spotify.shutdown()
    if prod:
        Committer.commit_and_push_if_github_actions()


async def publish_impl(
    spotify: Spotify, playlists_dir: pathlib.Path, prod: bool
) -> None:
    # Read playlists.json, preserve existing mappings
    repo_dir = Environment.get_repo_root()
    json_path = repo_dir / "playlists.json"
    with open(json_path, "r") as f:
        content = f.read()
    prev_playlist_mappings = PlaylistMappings.from_json(content)

    # Read scraped playlists from local storage
    scraped_playlists = get_scraped_playlists(playlists_dir)

    # When testing, only fetch one playlist to avoid rate limits
    if prod:
        published_playlists = [p async for p in spotify.get_published_playlists()]
    else:
        published_playlists = [
            p async for p in spotify.get_published_playlists(at_most=1)
        ]
        # Find the corresponding scraped playlist
        name = published_playlists[0].name
        while scraped_playlists[0].name != name:
            scraped_playlists = scraped_playlists[1:]
        scraped_playlists = scraped_playlists[:1]

    # Key playlists by name for quick retrieval
    scraped_playlists_dict = {p.name: p for p in scraped_playlists}
    published_playlists_dict = {p.name: p for p in published_playlists}

    playlists_to_create = set(scraped_playlists_dict) - set(published_playlists_dict)
    playlists_to_delete = set(published_playlists_dict) - set(scraped_playlists_dict)

    # Create missing playlists
    for name in sorted(playlists_to_create):
        logger.info(f"Creating playlist: {name}")
        if prod:
            playlist_id = await spotify.create_playlist(name)
        else:
            # When testing, just use a fake playlist ID
            playlist_id = PublishedPlaylistID(f"playlist_id:{name}")
        published_playlists_dict[name] = PublishedPlaylist(
            playlist_id=playlist_id,
            name=name,
            description="",
            track_ids=set(),
        )

    # Update existing playlists
    for name, scraped_playlist in scraped_playlists_dict.items():
        scraped_track_ids = scraped_playlist.track_ids

        published_playlist = published_playlists_dict[name]
        playlist_id = published_playlist.playlist_id
        published_track_ids = published_playlist.track_ids

        tracks_to_add = list(scraped_track_ids - published_track_ids)
        tracks_to_remove = list(published_track_ids - scraped_track_ids)

        if tracks_to_add:
            logger.info(f"Adding tracks to playlist: {name}")
            if prod:
                await spotify.add_items(playlist_id, tracks_to_add)

        if tracks_to_remove:
            logger.info(f"Removing tracks from playlist: {name}")
            if prod:
                await spotify.remove_items(playlist_id, tracks_to_remove)

    # Remove extra playlists
    for name in playlists_to_delete:
        playlist_id = published_playlists_dict[name].playlist_id
        logger.info(f"Unsubscribing from playlist: {name}")
        if prod:
            await spotify.unsubscribe_from_playlist(playlist_id)

    # Dump JSON
    scraped_to_published: Dict[
        ScrapedPlaylistID, List[PublishedPlaylistID]
    ] = collections.defaultdict(list)
    for name, scraped_playlist in scraped_playlists_dict.items():
        scraped_to_published[scraped_playlist.playlist_id].append(
            published_playlists_dict[name].playlist_id
        )
    playlists = PlaylistMappings(
        mappings=[
            PlaylistMapping(
                scraped_playlist_id=scraped_id,
                published_playlist_ids=published_ids,
            )
            for scraped_id, published_ids in sorted(scraped_to_published.items())
        ]
    )
    with open(json_path, "w") as f:
        f.write(playlists.to_json())


async def login() -> None:
    # Login OAuth flow.
    #
    # 1. Opens the authorize url in the default browser (on Linux).
    # 2. Sets up an HTTP server on port 8000 to listen for the callback.
    # 3. Requests a refresh token for the user and prints it.

    # Build the target URL
    client_id = Environment.get_env("SPOTIFY_CLIENT_ID")
    client_secret = Environment.get_env("SPOTIFY_CLIENT_SECRET")
    assert client_id and client_secret
    query_params = {
        "client_id": client_id,
        "response_type": "code",
        "redirect_uri": Spotify.REDIRECT_URI,
        "scope": "playlist-modify-public",
    }
    target_url = "https://accounts.spotify.com/authorize?{}".format(
        urllib.parse.urlencode(query_params)
    )

    # Print and try to open the URL in the default browser.
    print("Opening the following URL in a browser (at least trying to):")
    print(target_url)
    os.system("xdg-open '{}'".format(target_url))

    # Set up a temporary HTTP server and listen for the callback
    import socketserver
    from http import HTTPStatus
    from http.server import BaseHTTPRequestHandler

    authorization_code: str = ""

    class RequestHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            nonlocal authorization_code
            request_url = urllib.parse.urlparse(self.path)
            q = urllib.parse.parse_qs(request_url.query)
            authorization_code = q["code"][0]

            self.send_response(HTTPStatus.OK)
            self.end_headers()
            self.wfile.write(b"OK!")

    PORT = 8000
    httpd = socketserver.TCPServer(("", PORT), RequestHandler)
    httpd.handle_request()
    httpd.server_close()

    # Request a refresh token for given the authorization code
    refresh_token = await Spotify.get_user_refresh_token(
        client_id=client_id,
        client_secret=client_secret,
        authorization_code=authorization_code,
    )

    print("Refresh token, store this somewhere safe and use for the export feature:")
    print(refresh_token)


def argparse_directory(arg: str) -> pathlib.Path:
    path = pathlib.Path(arg)
    if path.is_dir():
        return path
    raise argparse.ArgumentTypeError(f"{arg} is not a valid directory")


async def main() -> None:
    parser = argparse.ArgumentParser(description="Publish playlists to Spotify")
    subparsers = parser.add_subparsers(dest="action", required=True)

    publish_parser = subparsers.add_parser(
        "publish",
        help="Fetch and publish playlists and tracks",
    )
    publish_parser.add_argument(
        "--playlists",
        required=True,
        type=argparse_directory,
        help="Path to the local playlists directory",
    )
    publish_parser.add_argument(
        "--prod",
        action="store_true",
        help="Actually publish changes to Spotify",
    )
    publish_parser.set_defaults(func=lambda args: publish(args.playlists, args.prod))

    login_parser = subparsers.add_parser(
        "login",
        help="Obtain a refresh token through the OAuth flow",
    )
    login_parser.set_defaults(func=lambda args: login())

    args = parser.parse_args()
    await args.func(args)


if __name__ == "__main__":
    allow_external_calls()
    asyncio.run(main())
