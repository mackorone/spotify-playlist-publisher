#!/usr/bin/env python3

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator, Mapping, Optional, Sequence, Set

import aiohttp

from plants.external import external
from playlist_types import PublishedPlaylist, PublishedPlaylistID

logger: logging.Logger = logging.getLogger(__name__)


class RetryBudgetExceededError(Exception):
    pass


class Spotify:

    BASE_URL = "https://api.spotify.com/v1"
    REDIRECT_URI = "http://localhost:8000"
    USER_ID = "w6hfc0hfa4s53j4l44mqn2ppe"

    def __init__(self, access_token: str) -> None:
        headers = {"Authorization": f"Bearer {access_token}"}
        self._session: aiohttp.ClientSession = self._get_session(headers=headers)
        # Handle rate limiting by retrying
        self._retry_budget_seconds: int = 300
        self._session.get = self._make_retryable(self._session.get)
        self._session.put = self._make_retryable(self._session.put)
        self._session.post = self._make_retryable(self._session.post)
        self._session.delete = self._make_retryable(self._session.delete)

    def _make_retryable(self, func):  # pyre-fixme[2,3]
        @asynccontextmanager
        async def wrapper(*args, **kwargs):  # pyre-fixme[2,3,53]
            while True:
                try:
                    response = await func(*args, **kwargs)
                except (
                    aiohttp.client_exceptions.ClientConnectionError,
                    asyncio.exceptions.TimeoutError,
                ):
                    backoff_seconds = 1
                    reason = "Connection problem"
                else:
                    status = response.status
                    if status == 429:
                        # Add an extra second, just to be safe
                        # https://stackoverflow.com/a/30557896/3176152
                        backoff_seconds = int(response.headers["Retry-After"]) + 1
                        reason = "Rate limited"
                    elif status // 100 == 5:
                        backoff_seconds = 1
                        reason = f"Server error ({status})"
                    else:
                        yield response
                        return
                self._retry_budget_seconds -= backoff_seconds
                if self._retry_budget_seconds <= 0:
                    raise RetryBudgetExceededError("Retry budget exceeded")
                else:
                    logger.warning(f"{reason}, will retry after {backoff_seconds}s")
                    await self._sleep(backoff_seconds)

        return wrapper

    async def shutdown(self) -> None:
        await self._session.close()
        # Sleep to allow underlying connections to close
        # https://docs.aiohttp.org/en/stable/client_advanced.html#graceful-shutdown
        await self._sleep(0)

    async def get_published_playlists(
        self, at_most: Optional[int] = None
    ) -> AsyncIterator[PublishedPlaylist]:
        playlist_ids = await self._get_playlist_ids(limit=at_most)
        sorted_ids = sorted(playlist_ids)

        # To avoid rate limits, fetch playlists in small batches
        batch_size = 3
        for i in range(0, len(sorted_ids), batch_size):
            logger.info(f"Progress so far: {i} / {len(sorted_ids)}")
            coros = []
            for j in range(0, min(batch_size, len(sorted_ids) - i)):
                coros.append(self._get_playlist(sorted_ids[i + j]))
            results = await asyncio.gather(*coros)
            for result in results:
                yield result

    async def _get_playlist_ids(self, limit: Optional[int]) -> Set[PublishedPlaylistID]:
        playlist_ids: Set[PublishedPlaylistID] = set()
        fetch_limit = limit or 50
        total = 1  # just need something nonzero to enter the loop
        while len(playlist_ids) < (limit or total):
            offset = len(playlist_ids)
            href = (
                self.BASE_URL
                + f"/users/{self.USER_ID}/playlists?limit={fetch_limit}&offset={offset}"
            )
            async with self._session.get(href) as response:
                data = await response.json(content_type=None)
            error = data.get("error")
            if error:
                raise Exception(f"Failed to get playlist IDs: {error}")
            playlist_ids |= {PublishedPlaylistID(item["id"]) for item in data["items"]}
            total = data["total"]  # total number of public playlists
        return playlist_ids

    async def _get_playlist(
        self, playlist_id: PublishedPlaylistID
    ) -> PublishedPlaylist:
        href = self.BASE_URL + f"/playlists/{playlist_id}?fields=name,description"
        async with self._session.get(href) as response:
            data = await response.json(content_type=None)
        error = data.get("error")
        if error:
            raise Exception(f"Failed to get playlist: {error}")
        name = data["name"]
        description = data["description"]
        logger.info(f"Fetching playlist content: {name}")
        track_ids = await self._get_track_ids(playlist_id)
        logger.info(f"Done fetching playlist: {name}")
        return PublishedPlaylist(
            playlist_id=playlist_id,
            name=name,
            description=description,
            track_ids=track_ids,
        )

    async def _get_track_ids(self, playlist_id: PublishedPlaylistID) -> Set[str]:
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

    async def create_playlist(self, name: str) -> PublishedPlaylistID:
        href = self.BASE_URL + f"/users/{self.USER_ID}/playlists"
        async with self._session.post(
            href,
            json={
                "name": name,
                "public": True,
                "collaborative": False,
            },
        ) as response:
            data = await response.json(content_type=None)
        error = data.get("error")
        if error:
            raise Exception(f"Failed to create playlist: {error}")
        return PublishedPlaylistID(data["id"])

    async def unsubscribe_from_playlist(self, playlist_id: PublishedPlaylistID) -> None:
        href = self.BASE_URL + f"/playlists/{playlist_id}/followers"
        async with self._session.delete(href) as response:
            if response.status != 200:
                text = await response.text()
                raise Exception(f"Failed to unsubscribe from playlist: {text}")

    async def change_playlist_details(
        self, playlist_id: PublishedPlaylistID, details_to_change: Mapping[str, str]
    ) -> None:
        description = details_to_change.get("description")
        if description and "\n" in description:
            raise Exception(f"Newlines in description are not allowed: {description}")
        async with self._session.put(
            self.BASE_URL + f"/playlists/{playlist_id}",
            json=details_to_change,
        ) as response:
            if response.status != 200:
                text = await response.text()
                raise Exception(f"Failed to change playlist details: {text}")

    async def add_items(
        self, playlist_id: PublishedPlaylistID, track_ids: Sequence[str]
    ) -> None:
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
                data = await response.json(content_type=None)
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

    async def remove_items(
        self, playlist_id: PublishedPlaylistID, track_ids: Sequence[str]
    ) -> None:
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
                data = await response.json(content_type=None)
            error = data.get("error")
            if error:
                raise Exception(f"Failed to remove tracks from playlist: {error}")

    @classmethod
    @external
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
                data = await response.json(content_type=None)

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
    @external
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
                data = await response.json(content_type=None)

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

    @classmethod
    @external
    def _get_session(
        cls, headers: Optional[Mapping[str, str]] = None
    ) -> aiohttp.ClientSession:
        return aiohttp.ClientSession(headers=headers)

    @classmethod
    @external
    async def _sleep(cls, seconds: float) -> None:
        await asyncio.sleep(seconds)
