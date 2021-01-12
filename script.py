#!/usr/bin/env python3

import argparse
import requests
import os
import re
import urllib.parse
from typing import List, Mapping, NamedTuple, Sequence, Set


class GitHubPlaylist(NamedTuple):
    name: str
    description: str
    track_ids: Set[str]


class SpotifyPlaylist(NamedTuple):
    name: str
    playlist_id: str
    description: str
    track_ids: Set[str]


class GitHub:

    ARCHIVE_REPO = "mackorone/spotify-playlist-archive"
    PUBLISHER_REPO = "mackorone/spotify-playlist-publisher"
    TRACK_PATTERN = re.compile(r"\(https://open.spotify.com/track/(.+?)\)")

    @classmethod
    def get_playlists(cls) -> List[GitHubPlaylist]:
        response = requests.get(
            f"https://api.github.com/repos/{cls.ARCHIVE_REPO}/"
            "contents/playlists/cumulative"
        )
        items = response.json()
        if not (isinstance(items, list) and len(items) > 0):
            raise Exception(f"Failed to fetch GitHub playlist names")
        return [cls._get_playlist(f) for f in items]

    @classmethod
    def _get_playlist(cls, github_file: Mapping[str, str]) -> GitHubPlaylist:
        name = github_file["name"][: -len(".md")] + " (Cumulative)"
        print(f"Fetching playlist from GitHub: {name}")
        content = requests.get(github_file["download_url"]).text
        lines = content.splitlines()
        return GitHubPlaylist(
            name=name,
            description=cls._get_description(lines),
            track_ids=cls._get_track_ids(lines),
        )

    @classmethod
    def _get_description(cls, lines: List[str]) -> str:
        for line in lines:
            if line.startswith("> "):
                return line[len("> ") :].strip()
        raise Exception("Failed to extract description")

    @classmethod
    def _get_track_ids(cls, lines: List[str]) -> Set[str]:
        track_ids = set()
        for line in lines[lines.index("|---|---|---|---|---|---|") + 1 :]:
            match = cls.TRACK_PATTERN.search(line)
            if not match:
                raise Exception(f"Unable to extract track ID: {line}")
            track_ids.add(match.group(1))
        return track_ids


class Spotify:

    BASE_URL = "https://api.spotify.com/v1"
    REDIRECT_URI = "http://localhost:8000"
    USER_ID = "w6hfc0hfa4s53j4l44mqn2ppe"

    def __init__(self, client_id: str, client_secret: str, refresh_token: str) -> None:
        token = self._get_user_access_token(client_id, client_secret, refresh_token)
        self._session = requests.Session()
        self._session.headers["Authorization"] = f"Bearer {token}"

    def get_playlists(self) -> List[SpotifyPlaylist]:
        playlist_ids = self._get_playlist_ids()
        return [self._get_playlist(p) for p in playlist_ids]

    def _get_playlist_ids(self) -> Set[str]:
        playlist_ids: Set[str] = set()
        limit = 50
        total = 1  # just need something nonzero to enter the loop
        while len(playlist_ids) < total:
            offset = len(playlist_ids)
            href = (
                self.BASE_URL
                + f"/users/{self.USER_ID}/playlists?limit={limit}&offset={offset}"
            )
            response = self._session.get(href).json()
            error = response.get("error")
            if error:
                raise Exception(f"Failed to get playlist IDs: {error}")
            playlist_ids |= {item["id"] for item in response["items"]}
            total = response["total"]  # total number of public playlists
        return playlist_ids

    def _get_playlist(self, playlist_id: str) -> SpotifyPlaylist:
        href = self.BASE_URL + f"/playlists/{playlist_id}?fields=name,description"
        response = self._session.get(href).json()
        error = response.get("error")
        if error:
            raise Exception(f"Failed to get playlist: {error}")
        name = response["name"]
        description = response["description"]
        print(f"Fetching playlist from Spotify: {name}")
        track_ids = self._get_track_ids(playlist_id)
        return SpotifyPlaylist(
            name=name,
            playlist_id=playlist_id,
            description=description,
            track_ids=track_ids,
        )

    def _get_track_ids(self, playlist_id: str) -> Set[str]:
        track_ids = set()
        href = (
            self.BASE_URL
            + f"/playlists/{playlist_id}/tracks?fields=next,items.track(id)"
        )
        while href:
            response = self._session.get(href).json()
            error = response.get("error")
            if error:
                raise Exception(f"Failed to get track IDs: {error}")
            for item in response["items"]:
                track = item["track"]
                if not track:
                    continue
                track_ids.add(track["id"])
            href = response["next"]
        return track_ids

    def create_playlist(self, name: str) -> str:
        href = self.BASE_URL + f"/users/{self.USER_ID}/playlists"
        response = self._session.post(
            href,
            json={
                "name": name,
                "public": True,
                "collaborative": False,
            },
        ).json()
        error = response.get("error")
        if error:
            raise Exception(f"Failed to create playlist: {error}")
        return response["id"]

    def unsubscribe_from_playlist(self, playlist_id: str) -> None:
        href = self.BASE_URL + f"/playlists/{playlist_id}/followers"
        response = self._session.delete(href)
        if response.status_code != 200:
            raise Exception(f"Failed to unsubscribe from playlist: {response.text}")

    def add_items(self, playlist_id: str, track_ids: Sequence[str]) -> None:
        # Group the tracks in batches of 100, since that's the limit.
        for i in range(0, len(track_ids), 100):
            track_uris = [
                "spotify:track:{}".format(track_id)
                for track_id in track_ids[i : i + 100]
            ]
            response = self._session.post(
                self.BASE_URL + f"/playlists/{playlist_id}/tracks",
                json={"uris": track_uris},
            ).json()
            error = response.get("error")
            if error:
                # This is hacky... if there's a bad ID in the archive,
                # skip it by trying successively smaller batch sizes
                if error.get("message") == "Payload contains a non-existing ID":
                    if len(track_ids) > 1:
                        mid = len(track_ids) // 2
                        self.add_items(playlist_id, track_ids[:mid])
                        self.add_items(playlist_id, track_ids[mid:])
                    else:
                        print(f"Skipping bad track ID: {track_ids[0]}")
                else:
                    raise Exception(f"Failed to add tracks to playlist: {error}")

    def remove_items(self, playlist_id: str, track_ids: Sequence[str]) -> None:
        # Group the tracks in batches of 100, since that's the limit.
        for i in range(0, len(track_ids), 100):
            track_uris = [
                {"uri": "spotify:track:{}".format(track_id)}
                for track_id in track_ids[i : i + 100]
            ]
            response = self._session.delete(
                self.BASE_URL + f"/playlists/{playlist_id}/tracks",
                json={"tracks": track_uris},
            ).json()
            error = response.get("error")
            if error:
                raise Exception(f"Failed to remove tracks from playlist: {error}")

    @classmethod
    def get_user_refresh_token(cls, client_id, client_secret, authorization_code):
        """Called during login flow to get one-time refresh token"""

        response = requests.post(
            "https://accounts.spotify.com/api/token",
            data={
                "grant_type": "authorization_code",
                "code": authorization_code,
                "redirect_uri": cls.REDIRECT_URI,
            },
            auth=(client_id, client_secret),
        ).json()

        error = response.get("error")
        if error:
            raise Exception("Failed to get access token: {}".format(error))

        refresh_token = response.get("refresh_token")
        if not refresh_token:
            raise Exception("Invalid refresh token: {}".format(refresh_token))

        token_type = response.get("token_type")
        if token_type != "Bearer":
            raise Exception("Invalid token type: {}".format(token_type))

        return refresh_token

    @classmethod
    def _get_user_access_token(
        cls, client_id: str, client_secret: str, refresh_token: str
    ) -> str:
        """Called during publish flow to get expiring access token"""

        response = requests.post(
            "https://accounts.spotify.com/api/token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
            },
            auth=(client_id, client_secret),
        ).json()

        error = response.get("error")
        if error:
            raise Exception("Failed to get access token: {}".format(error))

        access_token = response.get("access_token")
        if not access_token:
            raise Exception("Invalid access token: {}".format(access_token))

        token_type = response.get("token_type")
        if token_type != "Bearer":
            raise Exception("Invalid token type: {}".format(token_type))

        return access_token

    def change_playlist_details(self, playlist_id, name, description):
        response = self._session.put(
            self.BASE_URL + f"/playlists/{playlist_id}",
            json={
                "name": name,
                "description": description,
                "public": True,
                "collaborative": False,
            },
        )
        return response.status_code == 200


def publish() -> None:
    playlists_in_github = GitHub.get_playlists()

    # Check nonempty to fail fast
    client_id = os.getenv("SPOTIFY_CLIENT_ID")
    client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
    refresh_token = os.getenv("SPOTIFY_REFRESH_TOKEN")
    assert client_id and client_secret and refresh_token

    spotify = Spotify(
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
    )
    playlists_in_spotify = spotify.get_playlists()

    # Key playlists by name for quick retrieval
    github_playlists = {p.name: p for p in playlists_in_github}
    spotify_playlists = {p.name: p for p in playlists_in_spotify}

    playlists_to_create = set(github_playlists) - set(spotify_playlists)
    playlists_to_delete = set(spotify_playlists) - set(github_playlists)

    # Create missing playlists
    for name in sorted(playlists_to_create):
        print(f"Creating playlist: {name}")
        playlist_id = spotify.create_playlist(name)
        spotify_playlists[name] = SpotifyPlaylist(
            name=name,
            playlist_id=playlist_id,
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
            print(f"Adding tracks to playlist: {name}")
            spotify.add_items(playlist_id, tracks_to_add)

        if tracks_to_remove:
            print(f"Removing tracks from playlist: {name}")
            spotify.remove_items(playlist_id, tracks_to_remove)

    # Remove extra playlists
    for name in playlists_to_delete:
        playlist_id = spotify_playlists[name].playlist_id
        print(f"Unsubscribing from playlist: {name}")
        spotify.unsubscribe_from_playlist(playlist_id)


def login() -> None:
    # Login OAuth flow.
    #
    # 1. Opens the authorize url in the default browser (on Linux).
    # 2. Sets up an HTTP server on port 8000 to listen for the callback.
    # 3. Requests a refresh token for the user and prints it.

    # Build the target URL
    client_id = os.getenv("SPOTIFY_CLIENT_ID")
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
    from http import HTTPStatus
    from http.server import BaseHTTPRequestHandler
    import socketserver

    code = []

    class RequestHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            request_url = urllib.parse.urlparse(self.path)
            q = urllib.parse.parse_qs(request_url.query)
            code.append(q["code"][0])

            self.send_response(HTTPStatus.OK)
            self.end_headers()
            self.wfile.write(b"OK!")

    PORT = 8000
    httpd = socketserver.TCPServer(("", PORT), RequestHandler)
    httpd.handle_request()
    httpd.server_close()

    code = code[0]

    # Request a refresh token given the authorization code.
    refresh_token = Spotify.get_user_refresh_token(
        client_id=os.getenv("SPOTIFY_CLIENT_ID"),
        client_secret=os.getenv("SPOTIFY_CLIENT_SECRET"),
        authorization_code=code,
    )

    print("Refresh token, store this somewhere safe and use for the export feature:")
    print(refresh_token)


def main():
    parser = argparse.ArgumentParser(
        description="Publish archived playlists back to Spotify!"
    )

    subparsers = parser.add_subparsers(dest="action", required=True)

    publish_parser = subparsers.add_parser(
        "publish",
        help="Fetch and publish playlists and tracks",
    )
    login_parser = subparsers.add_parser(
        "login",
        help="Obtain a refresh token through the OAuth flow",
    )

    args = parser.parse_args()

    if args.action == "publish":
        publish()
    elif args.action == "login":
        login()
    else:
        raise NotImplementedError()

    print("Done")


if __name__ == "__main__":
    main()
