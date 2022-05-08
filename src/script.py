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
from typing import Dict, List, Mapping, Sequence, Set, Tuple

from plants.committer import Committer
from plants.environment import Environment
from plants.external import allow_external_calls
from plants.markdown import MarkdownEscapedString
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
class Playlists:
    mappings: Sequence[PlaylistMapping]

    @classmethod
    def from_json(cls, content: str) -> Playlists:
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
        return Playlists(mappings=mappings)

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


def get_scraped_playlists(
    playlists_dir: pathlib.Path,
) -> Dict[ScrapedPlaylistID, ScrapedPlaylist]:
    logger.info(f"Reading playlists from {playlists_dir}")

    json_files: List[pathlib.Path] = []
    for path in (playlists_dir / "cumulative").iterdir():
        if str(path).endswith(".json"):
            json_files.append(path)

    scraped_playlists: Dict[ScrapedPlaylistID, ScrapedPlaylist] = {}
    for path in json_files:
        with open(path, "r") as f:
            playlist = json.load(f)
        track_ids: Set[str] = set()
        for track in playlist["tracks"]:
            track_id = track["url"].split("/")[-1]
            track_ids.add(track_id)
        playlist_id = ScrapedPlaylistID(path.name[: -len(".json")])
        description = f"Link to archive: https://tinyurl.com/4mvw765u/{playlist_id}.md"
        scraped_playlists[playlist_id] = ScrapedPlaylist(
            playlist_id=playlist_id,
            name=playlist["name"] + " (Cumulative)",
            description=description,
            track_ids=track_ids,
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
    # Read playlists.json to preserve existing mappings
    repo_dir = Environment.get_repo_root()
    json_path = repo_dir / "playlists.json"
    with open(json_path, "r") as f:
        content = f.read()
    prev_playlists = Playlists.from_json(content)

    # Read scraped playlists from local storage
    scraped_playlists = get_scraped_playlists(playlists_dir)

    # Fetch published playlists from Spotify
    if prod:
        published_playlists = {
            playlist.playlist_id: playlist
            async for playlist in spotify.get_published_playlists()
        }
    else:
        scraped_playlists, published_playlists = await get_test_playlists(
            spotify=spotify,
            prev_playlists=prev_playlists,
            scraped_playlists=scraped_playlists,
        )

    # Auxiliary data structure for quick lookups
    prev_playlists_dict = {
        mapping.scraped_playlist_id: mapping.published_playlist_ids
        for mapping in prev_playlists.mappings
    }

    # Accumulates the next version of the mapping
    next_playlists_dict: Dict[
        ScrapedPlaylistID, List[PublishedPlaylistID]
    ] = collections.defaultdict(list)

    unpublished_scraped_playlists: Set[ScrapedPlaylistID] = set()
    published_playlists_to_retain: Set[PublishedPlaylistID] = set()
    for scraped_playlist_id in scraped_playlists:
        published_playlist_ids = prev_playlists_dict.get(scraped_playlist_id)
        if not published_playlist_ids:
            unpublished_scraped_playlists.add(scraped_playlist_id)
            continue
        # TODO: Support large playlists - allow more than one published
        # playlist, and salvage as many as possible, even if one of the
        # published playlists is invalid
        if not len(published_playlist_ids) == 1:
            unpublished_scraped_playlists.add(scraped_playlist_id)
            continue
        published_playlist_id = published_playlist_ids[0]
        if published_playlist_id not in published_playlists:
            unpublished_scraped_playlists.add(scraped_playlist_id)
            continue
        published_playlists_to_retain.add(published_playlist_id)
        next_playlists_dict[scraped_playlist_id].append(published_playlist_id)

    # Delete all published playlists without a valid reference
    published_playlists_to_delete = (
        set(published_playlists) - published_playlists_to_retain
    )

    # Create missing playlists
    for scraped_playlist_id in sorted(unpublished_scraped_playlists):
        name = scraped_playlists[scraped_playlist_id].name
        logger.info(f"Creating playlist: {name}")
        if prod:
            published_playlist_id = await spotify.create_playlist(name)
        else:
            # When testing, just use a fake playlist ID
            published_playlist_id = PublishedPlaylistID(f"playlist_id:{name}")
        published_playlists[published_playlist_id] = PublishedPlaylist(
            playlist_id=published_playlist_id,
            name=name,
            description="",
            track_ids=set(),
        )
        next_playlists_dict[scraped_playlist_id].append(published_playlist_id)

    # Update existing playlists
    for scraped_playlist_id, scraped_playlist in sorted(scraped_playlists.items()):
        scraped_name = scraped_playlist.name
        scraped_track_ids = scraped_playlist.track_ids
        scraped_description = scraped_playlist.description

        # TODO: Support large playlists - don't just grab the first published
        # playlist; instead calclate how many published playlists are required,
        # create them, and then update them
        published_playlist_ids = next_playlists_dict[scraped_playlist_id]
        assert len(published_playlist_ids) == 1
        published_playlist_id = published_playlist_ids[0]
        published_playlist = published_playlists[published_playlist_id]
        published_name = published_playlist.name
        published_track_ids = published_playlist.track_ids
        published_description = published_playlist.description

        details_to_change = {}
        if published_name != scraped_name:
            details_to_change["name"] = scraped_name
        if published_description != scraped_description:
            details_to_change["description"] = scraped_description
        if details_to_change:
            logger.info(f"Updating playlist details: {details_to_change}")
            if prod:
                await spotify.change_playlist_details(
                    published_playlist_id, details_to_change
                )

        tracks_to_add = list(scraped_track_ids - published_track_ids)
        if tracks_to_add:
            count = len(tracks_to_add)
            logger.info(f"Adding {count} track(s) to playlist: {scraped_name}")
            if prod:
                await spotify.add_items(published_playlist_id, tracks_to_add)

        tracks_to_remove = list(published_track_ids - scraped_track_ids)
        if tracks_to_remove:
            count = len(tracks_to_remove)
            logger.info(f"Removing {count} track(s) from playlist: {scraped_name}")
            if prod:
                await spotify.remove_items(published_playlist_id, tracks_to_remove)

    # Remove extra playlists
    for published_playlist_id in sorted(published_playlists_to_delete):
        name = published_playlists[published_playlist_id].name
        logger.info(f"Unsubscribing from playlist {published_playlist_id}: {name}")
        if prod:
            await spotify.unsubscribe_from_playlist(published_playlist_id)

    # Dump JSON
    playlists = Playlists(
        mappings=[
            PlaylistMapping(
                scraped_playlist_id=scraped_id,
                published_playlist_ids=published_ids,
            )
            for scraped_id, published_ids in sorted(next_playlists_dict.items())
        ]
    )
    with open(json_path, "w") as f:
        f.write(playlists.to_json() + "\n")

    # Update README.md, sort by name without suffix
    suffix = " (Cumulative)"
    playlist_tuples: List[Tuple[str, str]] = []
    for mapping in playlists.mappings:
        # TODO: Support large playlists - include all published playlists in
        # the README, not just the first one for each scraped playlist
        published_playlist_id = mapping.published_playlist_ids[0]
        published_playlist = published_playlists[published_playlist_id]
        name_stripped = published_playlist.name.strip()
        assert name_stripped.endswith(suffix), name_stripped
        name_no_suffix = name_stripped[: -len(suffix)]
        text = MarkdownEscapedString(name_stripped)
        link = f"https://open.spotify.com/playlist/{published_playlist.playlist_id}"
        playlist_tuples.append((name_no_suffix, f"- [{text}]({link})"))
    readme_path = repo_dir / "README.md"
    with open(readme_path, "r") as f:
        old_lines = f.read().splitlines()
    index = old_lines.index("## Playlists")
    playlist_lines = [text for key, text in sorted(playlist_tuples)]
    new_lines = old_lines[: index + 1] + [""] + playlist_lines
    with open(readme_path, "w") as f:
        f.write("\n".join(new_lines) + "\n")


async def get_test_playlists(
    spotify: Spotify,
    prev_playlists: Playlists,
    scraped_playlists: Mapping[ScrapedPlaylistID, ScrapedPlaylist],
) -> Tuple[
    Dict[ScrapedPlaylistID, ScrapedPlaylist],
    Dict[PublishedPlaylistID, PublishedPlaylist],
]:
    # Invert playlists for easy reverse lookup
    published_to_scraped = {
        published_playlist_id: mapping.scraped_playlist_id
        for mapping in prev_playlists.mappings
        for published_playlist_id in mapping.published_playlist_ids
    }

    # When testing, only fetch a few playlists to avoid rate limits
    async for published_playlist in spotify.get_published_playlists(at_most=5):
        published_playlist_id = published_playlist.playlist_id
        scraped_playlist_id = published_to_scraped.get(published_playlist_id)
        if not scraped_playlist_id:
            # Published playlists out of sync with mappings
            continue
        scraped_playlist = scraped_playlists.get(scraped_playlist_id)
        if not scraped_playlist:
            # Mappings out of sync with scraped playlists
            continue
        return (
            {scraped_playlist_id: scraped_playlist},
            # TODO: Support large playlists - this dictionary should contain
            # all published playlists for a given scraped playlist, not just
            # the first one encountered
            {published_playlist_id: published_playlist},
        )

    raise Exception("No suitable test playlists found")


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
