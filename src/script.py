#!/usr/bin/env python3

import argparse
import asyncio
import collections
import dataclasses
import json
import logging
import os
import pathlib
import urllib.parse
from contextlib import asynccontextmanager
from typing import AbstractSet, Dict, List, Optional, Sequence, Set

import aiohttp

from plants.committer import Committer
from plants.environment import Environment
from plants.external import allow_external_calls

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger: logging.Logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class GitHubPlaylist:
    # Playlist ID of the scraped playlist
    playlist_id: str
    name: str
    description: str
    track_ids: AbstractSet[str]


@dataclasses.dataclass(frozen=True)
class SpotifyPlaylist:
    # Playlist ID of the published playlist
    playlist_id: str
    name: str
    description: str
    track_ids: AbstractSet[str]


@dataclasses.dataclass(frozen=True)
class Playlist:
    scraped_playlist_id: str
    published_playlist_ids: List[str]


@dataclasses.dataclass(frozen=True)
class Playlists:
    playlists: List[Playlist]

    def to_json(self) -> str:
        return json.dumps(
            dataclasses.asdict(self),
            indent=2,
            sort_keys=True,
        )


class GitHub:
    @classmethod
    async def get_playlists(cls, playlists_dir: pathlib.Path) -> List[GitHubPlaylist]:
        logger.info(f"Reading playlists from {playlists_dir}")

        json_files: List[pathlib.Path] = []
        for path in (playlists_dir / "cumulative").iterdir():
            if str(path).endswith(".json"):
                json_files.append(path)

        github_playlists: List[GitHubPlaylist] = []
        for path in json_files:
            with open(path, "r") as f:
                playlist = json.load(f)
            track_ids: Set[str] = set()
            for track in playlist["tracks"]:
                track_id = track["url"].split("/")[-1]
                track_ids.add(track_id)
            github_playlists.append(
                GitHubPlaylist(
                    playlist_id=playlist["url"].split("/")[-1],
                    name=playlist["name"] + " (Cumulative)",
                    description=playlist["description"],
                    track_ids=track_ids,
                )
            )

        return github_playlists


class Spotify:

    BASE_URL = "https://api.spotify.com/v1"
    REDIRECT_URI = "http://localhost:8000"
    USER_ID = "w6hfc0hfa4s53j4l44mqn2ppe"

    def __init__(self, access_token: str) -> None:
        headers = {"Authorization": f"Bearer {access_token}"}
        self._session = aiohttp.ClientSession(headers=headers)
        # Handle rate limiting by retrying
        self._retry_budget_seconds: int = 30
        self._session.get = self._make_retryable(self._session.get)
        self._session.put = self._make_retryable(self._session.get)
        self._session.post = self._make_retryable(self._session.post)
        self._session.delete = self._make_retryable(self._session.delete)

    def _make_retryable(self, func):  # pyre-fixme[2,3]
        @asynccontextmanager
        async def wrapper(*args, **kwargs):  # pyre-fixme[2,3,53]
            while True:
                response = await func(*args, **kwargs)
                if response.status == 429:
                    # Add an extra second, just to be safe
                    # https://stackoverflow.com/a/30557896/3176152
                    backoff_seconds = int(response.headers["Retry-After"]) + 1
                    reason = "Rate limited"
                elif response.status in [500, 502, 504]:
                    backoff_seconds = 1
                    reason = "Server error"
                else:
                    yield response
                    return
                self._retry_budget_seconds -= backoff_seconds
                if self._retry_budget_seconds <= 0:
                    raise Exception("Retry budget exceeded")
                else:
                    logger.warning(f"{reason}, will retry after {backoff_seconds}s")
                    await asyncio.sleep(backoff_seconds)

        return wrapper

    async def shutdown(self) -> None:
        await self._session.close()
        # Sleep to allow underlying connections to close
        # https://docs.aiohttp.org/en/stable/client_advanced.html#graceful-shutdown
        await asyncio.sleep(0)

    async def get_playlists(self, limit: Optional[int] = None) -> List[SpotifyPlaylist]:
        playlist_ids = await self._get_playlist_ids(limit)
        coros = [self._get_playlist(p) for p in playlist_ids]
        # TODO: Can't gather due to rate limits
        # return await asyncio.gather(*coros)
        return [await c for c in coros]

    async def _get_playlist_ids(self, limit: Optional[int]) -> Set[str]:
        playlist_ids: Set[str] = set()
        fetch_limit = limit or 50
        total = 1  # just need something nonzero to enter the loop
        while len(playlist_ids) < (limit or total):
            offset = len(playlist_ids)
            href = (
                self.BASE_URL
                + f"/users/{self.USER_ID}/playlists?limit={fetch_limit}&offset={offset}"
            )
            async with self._session.get(href) as response:
                data = await response.json()
            error = data.get("error")
            if error:
                raise Exception(f"Failed to get playlist IDs: {error}")
            playlist_ids |= {item["id"] for item in data["items"]}
            total = data["total"]  # total number of public playlists
        return playlist_ids

    async def _get_playlist(self, playlist_id: str) -> SpotifyPlaylist:
        href = self.BASE_URL + f"/playlists/{playlist_id}?fields=name,description"
        async with self._session.get(href) as response:
            data = await response.json(content_type=None)
        error = data.get("error")
        if error:
            raise Exception(f"Failed to get playlist: {error}")
        name = data["name"]
        description = data["description"]
        logger.info(f"Fetching playlist from Spotify: {name}")
        track_ids = await self._get_track_ids(playlist_id)
        logger.info(f"Done fetching Spotify playlist: {name}")
        return SpotifyPlaylist(
            playlist_id=playlist_id,
            name=name,
            description=description,
            track_ids=track_ids,
        )

    async def _get_track_ids(self, playlist_id: str) -> Set[str]:
        track_ids = set()
        href = (
            self.BASE_URL
            + f"/playlists/{playlist_id}/tracks?fields=next,items.track(id)"
        )
        while href:
            async with self._session.get(href) as response:
                data = await response.json(content_type=None)
            error = data.get("error")
            if error:
                raise Exception(f"Failed to get track IDs: {error}")
            for item in data["items"]:
                track = item["track"]
                if not track:
                    continue
                track_ids.add(track["id"])
            href = data["next"]
        return track_ids

    async def create_playlist(self, name: str) -> str:
        href = self.BASE_URL + f"/users/{self.USER_ID}/playlists"
        async with self._session.post(
            href,
            json={
                "name": name,
                "public": True,
                "collaborative": False,
            },
        ) as response:
            data = await response.json()
        error = data.get("error")
        if error:
            raise Exception(f"Failed to create playlist: {error}")
        return data["id"]

    async def unsubscribe_from_playlist(self, playlist_id: str) -> None:
        href = self.BASE_URL + f"/playlists/{playlist_id}/followers"
        async with self._session.delete(href) as response:
            if response.status != 200:
                text = await response.text()
                raise Exception(f"Failed to unsubscribe from playlist: {text}")

    async def add_items(self, playlist_id: str, track_ids: Sequence[str]) -> None:
        # Group the tracks in batches of 100, since that's the limit.
        for i in range(0, len(track_ids), 100):
            track_uris = [
                "spotify:track:{}".format(track_id)
                for track_id in track_ids[i : i + 100]  # noqa
            ]
            async with self._session.post(
                self.BASE_URL + f"/playlists/{playlist_id}/tracks",
                json={"uris": track_uris},
            ) as response:
                data = await response.json()
            error = data.get("error")
            if error:
                # This is hacky... if there's a bad ID in the archive,
                # skip it by trying successively smaller batch sizes
                if error.get("message") == "Payload contains a non-existing ID":
                    if len(track_ids) > 1:
                        mid = len(track_ids) // 2
                        coros = [
                            self.add_items(playlist_id, track_ids[:mid]),
                            self.add_items(playlist_id, track_ids[mid:]),
                        ]
                        await asyncio.gather(*coros)
                    else:
                        logger.warning(f"Skipping bad track ID: {track_ids[0]}")
                elif error.get("message").startswith("Playlist size limit reached"):
                    logger.error(f"Playlist is too big, aborting: {playlist_id}")
                else:
                    raise Exception(f"Failed to add tracks to playlist: {error}")

    async def remove_items(self, playlist_id: str, track_ids: Sequence[str]) -> None:
        # Group the tracks in batches of 100, since that's the limit.
        for i in range(0, len(track_ids), 100):
            track_uris = [
                {"uri": "spotify:track:{}".format(track_id)}
                for track_id in track_ids[i : i + 100]  # noqa
            ]
            async with self._session.delete(
                self.BASE_URL + f"/playlists/{playlist_id}/tracks",
                json={"tracks": track_uris},
            ) as response:
                data = await response.json()
            error = data.get("error")
            if error:
                raise Exception(f"Failed to remove tracks from playlist: {error}")

    @classmethod
    async def get_user_refresh_token(
        cls,
        client_id: str,
        client_secret: str,
        authorization_code: str,
    ) -> str:
        """Called during login flow to get one-time refresh token"""

        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://accounts.spotify.com/api/token",
                data={
                    "grant_type": "authorization_code",
                    "code": authorization_code,
                    "redirect_uri": cls.REDIRECT_URI,
                },
                auth=aiohttp.BasicAuth(client_id, client_secret),
            ) as response:
                data = await response.json()

        error = data.get("error")
        if error:
            raise Exception("Failed to get access token: {}".format(error))

        refresh_token = data.get("refresh_token")
        if not refresh_token:
            raise Exception("Invalid refresh token: {}".format(refresh_token))

        token_type = data.get("token_type")
        if token_type != "Bearer":
            raise Exception("Invalid token type: {}".format(token_type))

        return refresh_token

    @classmethod
    async def get_user_access_token(
        cls, client_id: str, client_secret: str, refresh_token: str
    ) -> str:
        """Called during publish flow to get expiring access token"""

        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://accounts.spotify.com/api/token",
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token,
                },
                auth=aiohttp.BasicAuth(client_id, client_secret),
            ) as response:
                data = await response.json()

        error = data.get("error")
        if error:
            raise Exception("Failed to get access token: {}".format(error))

        access_token = data.get("access_token")
        if not access_token:
            raise Exception("Invalid access token: {}".format(access_token))

        token_type = data.get("token_type")
        if token_type != "Bearer":
            raise Exception("Invalid token type: {}".format(token_type))

        return access_token


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
    # Always read all GitHub playlists from local storage
    playlists_in_github = await GitHub.get_playlists(playlists_dir)

    # When testing, only fetch one playlist to avoid rate limits
    if prod:
        playlists_in_spotify = await spotify.get_playlists()
    else:
        playlists_in_spotify = await spotify.get_playlists(limit=1)
        # Find the corresponding GitHub playlist
        name = playlists_in_spotify[0].name
        while playlists_in_github[0].name != name:
            playlists_in_github = playlists_in_github[1:]
        playlists_in_github = playlists_in_github[:1]

    # Key playlists by name for quick retrieval
    github_playlists = {p.name: p for p in playlists_in_github}
    spotify_playlists = {p.name: p for p in playlists_in_spotify}

    playlists_to_create = set(github_playlists) - set(spotify_playlists)
    playlists_to_delete = set(spotify_playlists) - set(github_playlists)

    # Create missing playlists
    for name in sorted(playlists_to_create):
        logger.info(f"Creating playlist: {name}")
        if prod:
            playlist_id = await spotify.create_playlist(name)
        else:
            # When testing, just use a fake playlist ID
            playlist_id = f"playlist_id:{name}"
        spotify_playlists[name] = SpotifyPlaylist(
            playlist_id=playlist_id,
            name=name,
            description="",
            track_ids=set(),
        )

    # Update existing playlists
    for name, github_playlist in github_playlists.items():
        github_track_ids = github_playlist.track_ids

        spotify_playlist = spotify_playlists[name]
        playlist_id = spotify_playlist.playlist_id
        spotify_track_ids = spotify_playlist.track_ids

        tracks_to_add = list(github_track_ids - spotify_track_ids)
        tracks_to_remove = list(spotify_track_ids - github_track_ids)

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
        playlist_id = spotify_playlists[name].playlist_id
        logger.info(f"Unsubscribing from playlist: {name}")
        if prod:
            await spotify.unsubscribe_from_playlist(playlist_id)

    # Dump JSON
    scraped_to_published: Dict[str, List[str]] = collections.defaultdict(list)
    for name, github_playlist in github_playlists.items():
        scraped_to_published[github_playlist.playlist_id].append(
            spotify_playlists[name].playlist_id
        )
    playlists = Playlists(
        playlists=[
            Playlist(
                scraped_playlist_id=scraped_id,
                published_playlist_ids=published_ids,
            )
            for scraped_id, published_ids in scraped_to_published.items()
        ]
    )
    repo_dir = Environment.get_repo_root()
    json_path = repo_dir / "playlists.json"
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
