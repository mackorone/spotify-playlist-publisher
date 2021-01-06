#!/usr/bin/env python3

import argparse
import base64
import collections
import datetime
import io
import os
import re
import subprocess
import tarfile
import tempfile
import urllib.parse
from pathlib import Path
from typing import Optional

import requests

ARCHIVE_REPO = "mackorone/spotify-playlist-archive"
PUBLISH_REPO = "mackorone/spotify-playlist-publisher"


Playlist = collections.namedtuple(
    "Playlist",
    [
        "id",
        "url",
        "name",
        "description",
        "tracks",
    ],
)

Track = collections.namedtuple(
    "Track",
    [
        "id",
        "url",
        "duration_ms",
        "name",
        "album",
        "artists",
    ],
)

Album = collections.namedtuple("Album", ["url", "name"])

Artist = collections.namedtuple("Artist", ["url", "name"])

aliases_dir = Path("playlists/aliases")
plain_dir = Path("playlists/plain")
pretty_dir = Path("playlists/pretty")
cumulative_dir = Path("playlists/cumulative")
export_dir = Path("playlists/export")


class InvalidAccessTokenError(Exception):
    pass


class InvalidPlaylistError(Exception):
    pass


class PrivatePlaylistError(Exception):
    pass


class Spotify:

    BASE_URL = "https://api.spotify.com/v1"
    REDIRECT_URI = "http://localhost:8000"

    def __init__(self, client_id, client_secret, user_token=None):
        if user_token:
            self._token = self._get_user_access_token(
                client_id, client_secret, user_token
            )
        else:
            self._token = self._get_access_token(client_id, client_secret)
            self._user_id = None

        self._session = requests.Session()
        self._session.headers = {
            "Authorization": "Bearer {}".format(self._token)
        }

        if user_token:
            self._user_id = self.get_current_user_id()
        else:
            self._user_id = None

    def add_items(self, playlist_id, track_ids):
        # Group the tracks in batches of 100, since that's the limit.
        for i in range(0, len(track_ids), 100):
            track_uris = [
                "spotify:track:{}".format(track_id)
                for track_id in track_ids[i:i+100]
            ]
            response = self._session.post(
                self._post_tracks_href(playlist_id),
                json={
                    "uris": track_uris
                }
            ).json()
            error = response.get("error")
            if error:
                raise Exception(
                    "Failed to add tracks to playlist, error: {}".formt(error)
                )

    def change_playlist_details(self, playlist_id, name, description):
        response = self._session.put(
            self._change_playlist_details_href(playlist_id),
            json={
                "name": name,
                "description": description,
                "public": True,
                "collaborative": False,
            }
        )
        return response.status_code == 200

    def create_playlist(self, name):
        if self._user_id is None:
            raise Exception(
                "Creating playlists requires logging in!"
            )

        response = self._session.post(
            self._create_playlist_href(self._user_id),
            json={
                "name": name,
                "public": True,
                "collaborative": False,
            }
        ).json()

        print(response)


        return Playlist(
            id=response["id"],
            name=response["name"],
            description=None,
            url=self._get_url(response["external_urls"]),
            tracks=[]
        )

    @classmethod
    def _get_access_token(cls, client_id, client_secret):
        joined = "{}:{}".format(client_id, client_secret)
        encoded = base64.b64encode(joined.encode()).decode()

        response = requests.post(
            "https://accounts.spotify.com/api/token",
            data={"grant_type": "client_credentials"},
            headers={"Authorization": "Basic {}".format(encoded)},
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

    @classmethod
    def _get_user_access_token(cls, client_id, client_secret, refresh_token):
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
            print(response)
            raise Exception("Failed to get access token: {}".format(error))

        access_token = response.get("access_token")
        if not access_token:
            raise Exception("Invalid access token: {}".format(access_token))

        token_type = response.get("token_type")
        if token_type != "Bearer":
            raise Exception("Invalid token type: {}".format(token_type))

        return access_token

    @classmethod
    def get_user_refresh_token(cls, client_id, client_secret, authorization_code):
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
            print(response)
            raise Exception("Failed to get access token: {}".format(error))

        refresh_token = response.get("refresh_token")
        if not refresh_token:
            raise Exception("Invalid refresh token: {}".format(refresh_token))

        token_type = response.get("token_type")
        if token_type != "Bearer":
            raise Exception("Invalid token type: {}".format(token_type))

        return refresh_token

    def get_playlist(self, playlist_id, aliases):
        playlist_href = self._get_playlist_href(playlist_id)
        response = self._session.get(playlist_href).json()

        error = response.get("error")
        if error:
            if error.get("status") == 401:
                raise InvalidAccessTokenError
            elif error.get("status") == 403:
                raise PrivatePlaylistError
            elif error.get("status") == 404:
                raise InvalidPlaylistError
            else:
                raise Exception("Failed to get playlist: {}".format(error))

        url = self._get_url(response["external_urls"])

        # If the playlist has an alias, use it
        if playlist_id in aliases:
            name = aliases[playlist_id]
        else:
            name = response["name"]

        # Playlist names can't have "/" so use "\" instead
        id = response["id"]
        name = name.replace("/", "\\")
        description = response["description"]
        tracks = self._get_tracks(playlist_id)

        return Playlist(
            id=id,
            url=url,
            name=name,
            description=description,
            tracks=tracks
        )

    def get_current_user_id(self):
        response = self._session.get(self._get_current_user_href()).json()
        return response["id"]

    def _get_tracks(self, playlist_id):
        tracks = []
        tracks_href = self._get_tracks_href(playlist_id)

        while tracks_href:
            response = self._session.get(tracks_href).json()

            error = response.get("error")
            if error:
                raise Exception("Failed to get tracks: {}".format(error))

            for item in response["items"]:
                track = item["track"]
                if not track:
                    continue

                id_ = track["id"]
                url = self._get_url(track["external_urls"])
                duration_ms = track["duration_ms"]
                name = track["name"]
                album = Album(
                    url=self._get_url(track["album"]["external_urls"]),
                    name=track["album"]["name"],
                )

                artists = []
                for artist in track["artists"]:
                    artists.append(
                        Artist(
                            url=self._get_url(artist["external_urls"]),
                            name=artist["name"],
                        )
                    )

                tracks.append(
                    Track(
                        id=id_,
                        url=url,
                        duration_ms=duration_ms,
                        name=name,
                        album=album,
                        artists=artists,
                    )
                )

            tracks_href = response["next"]

        return tracks

    @classmethod
    def _get_url(cls, external_urls):
        return (external_urls or {}).get("spotify")

    @classmethod
    def _get_current_user_href(cls):
        return cls.BASE_URL + "/me"

    @classmethod
    def _create_playlist_href(cls, user_id):
        return cls.BASE_URL + "/users/{}/playlists".format(user_id)

    @classmethod
    def _change_playlist_details_href(cls, playlist_id):
        return cls.BASE_URL + "/playlists/{}".format(playlist_id)

    @classmethod
    def _get_playlist_href(cls, playlist_id):
        rest = "/playlists/{}?fields=id,external_urls,name,description"
        template = cls.BASE_URL + rest
        return template.format(playlist_id)

    @classmethod
    def _post_tracks_href(cls, playlist_id):
        return cls.BASE_URL + "/playlists/{}/tracks".format(playlist_id)

    @classmethod
    def _get_tracks_href(cls, playlist_id):
        rest = (
            "/playlists/{}/tracks?fields=next,items.track(id,external_urls,"
            "duration_ms,name,album(external_urls,name),artists)"
        )
        template = cls.BASE_URL + rest
        return template.format(playlist_id)


class Archive:
    @classmethod
    def fetch_archive(cls) -> Path:
        tempdir = tempfile.mkdtemp()

        # Fetch archive from repo
        r = requests.get(
            "https://github.com/{}/archive/master.tar.gz".format(ARCHIVE_REPO)
        )

        # Extract only the cumulative playlists
        with io.BytesIO(r.content) as data, \
             tarfile.open(name=None, mode="r:gz", fileobj=data) as tar:
            to_extract = [
                member
                for member in tar
                if cls._is_cumulative_playlist(member)
            ]
            tar.extractall(path=tempdir, members=to_extract)

        prefix = "spotify-playlist-archive-master"
        return Path(tempdir) / prefix

    @classmethod
    def _is_cumulative_playlist(cls, info: tarfile.TarInfo) -> bool:
        return (
            info.isfile() and
            str(cumulative_dir) in info.name
        )

def publish_playlists(now: datetime.datetime):
    spotify = Spotify(
        client_id=os.getenv("SPOTIFY_CLIENT_ID"),
        client_secret=os.getenv("SPOTIFY_CLIENT_SECRET"),
        user_token=os.getenv("SPOTIFY_USER_TOKEN")
    )

    print(
        "Fetching current playlist archive from Github... ",
        end="",
        flush=True
    )
    archive_dir = Archive.fetch_archive()
    print("Done!")

    exported = []
    for playlist_file in (archive_dir / cumulative_dir).iterdir():
        if playlist := export_playlist(playlist_file, spotify):
            exported.append(playlist)

    # Lastly, update README.md
    with open("README.md") as f:
        readme = [
            line.strip()
            for line in f
        ]

    index = readme.index("## Playlists")

    readme_lines = [
        "- [{}]({})".format(
            playlist.name,
            playlist.url,
        )
        for playlist in sorted(exported, key=lambda pl: pl.name.lower())
    ]

    lines = (
        readme[: index + 1] + [""] + sorted(readme_lines, key=lambda line: line.lower())
    )
    with open("README.md", "w") as f:
        f.write("\n".join(lines) + "\n")


def export_playlist(playlist_file: Path, spotify: Spotify) -> Optional[Playlist]:
    print(f"Exporting playlist \"{playlist_file.name}\"")
    with playlist_file.open() as f:
        contents = list(f)

    if len(contents) < 5:
        print(f"Playlist {playlist_file.name} is empty, skipping...")
        return

    if m := re.match(r"^### \[(.+?)\]\(.*/playlist/(.+)\)", contents[2]):
        # TODO: Should the name be unescaped in any way?
        playlist_name, playlist_id = m.groups()
    else:
        raise Exception(
            f"Failed to parse playlist header for {playlist_file.name}"
        )

    track_pattern = re.compile(
        r"\(https://open.spotify.com/track/(.+?)\)"
    )
    tracks = {
        m.group(1)
        for line in contents
        if (m := track_pattern.search(line))
    }

    export_path = export_dir / playlist_id
    if export_path.exists():
        # Read the exported playlist id from playlists/exported/<id>.
        with export_path.open() as f:
            export_id = f.read().strip()
        export_playlist = spotify.get_playlist(export_id, [])
    else:
        # We haven't exported this playlist before, let's create a new
        # one!
        export_playlist = spotify.create_playlist(playlist_name)

        # Write the id to playlists/exported/<id>.
        export_id = export_playlist.id
        export_dir.mkdir(exist_ok=True, parents=True)
        with export_path.open("w") as f:
            f.write(export_id)


    # TODO: Improve with more information:
    #       - last updated
    #       - first updated
    #       - link back to archive repo
    description = "Playlist containing all tracks from {}.".format(
        playlist_name
    )
    name = "{} (archive)".format(playlist_name)

    print(
        "Updating playlist details for: {} (exported as {})... ".format(
            playlist_id,
            export_playlist.id
        ),
        end="",
        flush=True
    )
    spotify.change_playlist_details(
        export_playlist.id,
        name,
        description
    )
    print("Done!")

    old_tracks = {
        t.id for t in export_playlist.tracks
    }
    tracks_to_add = tracks - old_tracks

    # TODO: What to do with these?
    tracks_to_remove = old_tracks - tracks

    if tracks_to_add:
        print(
            "Adding {} track(s) to {}... ".format(
                len(tracks_to_add),
                export_playlist.id
            ),
            end="",
            flush=True
        )
        spotify.add_items(export_playlist.id, list(tracks_to_add))
    else:
        print(
            "No new tracks for {} (exported as {})!".format(
                playlist_id,
                export_playlist.id
            )
        )

    return export_playlist


def run(args):
    print("- Running: {}".format(args))
    result = subprocess.run(
        args=args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    print("- Exited with: {}".format(result.returncode))
    return result


def push_updates(now):
    diff = run(["git", "status", "-s"])
    has_changes = bool(diff.stdout)

    if not has_changes:
        print("No changes, not pushing")
        return

    print("Configuring git")

    config = ["git", "config", "--global"]
    config_name = run(config + ["user.name", "Mack Ward (Bot Account)"])
    config_email = run(config + ["user.email", "mackorone.bot@gmail.com"])

    if config_name.returncode != 0:
        raise Exception("Failed to configure name")
    if config_email.returncode != 0:
        raise Exception("Failed to configure email")

    print("Staging changes")

    add = run(["git", "add", "-A"])
    if add.returncode != 0:
        raise Exception("Failed to stage changes")

    print("Committing changes")

    build = os.getenv("TRAVIS_BUILD_NUMBER")
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    message = "[skip ci] Build #{} ({})".format(build, now_str)
    commit = run(["git", "commit", "-m", message])
    if commit.returncode != 0:
        raise Exception("Failed to commit changes")

    print("Rebasing onto master")
    rebase = run(["git", "rebase", "HEAD", "master"])
    if commit.returncode != 0:
        raise Exception("Failed to rebase onto master")

    print("Removing origin")
    remote_rm = run(["git", "remote", "rm", "origin"])
    if remote_rm.returncode != 0:
        raise Exception("Failed to remove origin")

    print("Adding new origin")
    # It's ok to print the token, Travis will hide it
    token = os.getenv("GITHUB_ACCESS_TOKEN")
    url = "https://mackorone-bot:{}@github.com/{}.git".format(
        token,
        PUBLISH_REPO
    )
    remote_add = run(["git", "remote", "add", "origin", url])
    if remote_add.returncode != 0:
        raise Exception("Failed to add new origin")

    print("Pushing changes")
    push = run(["git", "push", "origin", "master"])
    if push.returncode != 0:
        raise Exception("Failed to push changes")


def login(now):
    # Login OAuth flow.
    #
    # 1. Opens the authorize url in the default browser (on Linux).
    # 2. Sets up an HTTP server on port 8000 to listen for the
    #    callback.
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
            code.append(q['code'][0])

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

    print(
        "Refresh token, store this somewhere safe and use for "
        "the export feature:"
    )
    print(refresh_token)


def main():
    parser = argparse.ArgumentParser(
        description="Publish archived Spotify playlists back to Spotify!"
    )

    subparsers = parser.add_subparsers(dest="action")

    publish_parser = subparsers.add_parser(
        "publish",
        help=(
            "Fetch and publish playlists and tracks"
        )
    )
    publish_parser.add_argument(
        "--push", "-p",
        action="store_true",
        help="Commit and push updated playlists to Github ({})".format(
            PUBLISH_REPO
        )
    )

    login_parser = subparsers.add_parser(
        "login",
        help=(
            "Obtain a user token through the OAuth flow"
        )
    )


    args = parser.parse_args()
    now = datetime.datetime.now()

    if args.action == "publish":
        publish_playlists(now)
        if args.push:
            push_updates(now)
    elif args.action == "login":
        login(now)
    else:
        parser.error("No action specified!")

    print("Done")


if __name__ == "__main__":
    main()
