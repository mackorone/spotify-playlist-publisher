#!/usr/bin/env python3

import asyncio
import contextlib
import dataclasses
import enum
import logging
from typing import Any, AsyncIterator, Dict, Mapping, Optional, Sequence, Set

import aiohttp

from plants.external import external
from playlist_types import PublishedPlaylist, PublishedPlaylistID

logger: logging.Logger = logging.getLogger(__name__)


#
# Errors that propagate to the caller
#


class RequestFailedError(Exception):
    pass


class RetryBudgetExceededError(Exception):
    pass


#
# Errors that are transparently retried
#


@dataclasses.dataclass
class RetryableError(Exception):
    message: str
    sleep_seconds: float = 1
    refresh_access_token: bool = False


class InvalidAccessTokenError(Exception):
    pass


@dataclasses.dataclass
class RateLimitedError(Exception):
    retry_after: int


@dataclasses.dataclass
class ServerError(Exception):
    status: int


class UnexpectedEmptyResponseError(Exception):
    pass


class HttpMethod(enum.Enum):
    GET = enum.auto()
    PUT = enum.auto()
    POST = enum.auto()
    DELETE = enum.auto()


class ResponseType(enum.Enum):
    JSON = enum.auto()
    EMPTY = enum.auto()


class Spotify:

    # Can't use base_url in ClientSession because Spotify pagination always
    # returns absolute URLs
    BASE_URL = "https://api.spotify.com/v1"
    REDIRECT_URI = "http://localhost:8000"
    USER_ID = "w6hfc0hfa4s53j4l44mqn2ppe"

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        retry_budget_seconds: float,
    ) -> None:
        self._client_id: str = client_id
        self._client_secret: str = client_secret
        self._refresh_token: str = refresh_token
        self._access_token: Optional[str] = None
        self._retry_budget_seconds: float = retry_budget_seconds
        self._session: aiohttp.ClientSession = self._get_session()

    async def _make_retryable_request(
        self,
        method: HttpMethod,
        url: str,
        json: Optional[Mapping[str, Any]] = None,
        *,
        expected_response_type: ResponseType = ResponseType.JSON,
        raise_if_request_fails: bool = True,
    ) -> Dict[str, Any]:
        while True:
            # Lazily fetch access token
            if not self._access_token:
                logger.info("Getting new access token")
                self._access_token = await self.get_user_access_token(
                    client_id=self._client_id,
                    client_secret=self._client_secret,
                    refresh_token=self._refresh_token,
                )
                logger.info("Got new access token")

            # Choose the correct method
            func = {
                HttpMethod.GET: self._session.get,
                HttpMethod.PUT: self._session.put,
                HttpMethod.POST: self._session.post,
                HttpMethod.DELETE: self._session.delete,
            }[method]

            # Prepare the request
            aenter_to_send_request = func(
                url=url,
                json=json,
                headers={"Authorization": f"Bearer {self._access_token}"},
            )

            # Make a retryable attempt
            try:
                return await self._send_request_and_coerce_errors(
                    aenter_to_send_request,
                    expected_response_type,
                    raise_if_request_fails,
                )
            except RetryableError as e:
                if e.refresh_access_token:
                    self._access_token = None
                self._retry_budget_seconds -= e.sleep_seconds
                if self._retry_budget_seconds <= 0:
                    raise RetryBudgetExceededError("Overall retry budget exceeded")
                logger.warning(f"{e.message}, will retry after {e.sleep_seconds}s")
                await self._sleep(e.sleep_seconds)

    @classmethod
    async def _send_request_and_coerce_errors(
        cls,
        aenter_to_send_request: contextlib.AbstractAsyncContextManager,
        expected_response_type: ResponseType,
        raise_if_request_fails: bool,
    ) -> Dict[str, Any]:
        """Catch retryable errors and coerce them into uniform type"""
        try:
            return await cls._send_request(
                aenter_to_send_request,
                expected_response_type,
                raise_if_request_fails,
            )
        except InvalidAccessTokenError:
            raise RetryableError(
                message="Invalid access token",
                refresh_access_token=True,
            )
        except RateLimitedError as e:
            raise RetryableError(
                message="Rate limited",
                # Add an extra second, just to be safe
                # https://stackoverflow.com/a/30557896/3176152
                sleep_seconds=e.retry_after + 1,
            )
        except ServerError as e:
            raise RetryableError(f"Server error ({e.status})")
        except aiohttp.ContentTypeError:
            raise RetryableError("Invalid response (invalid JSON)")
        except UnexpectedEmptyResponseError:
            raise RetryableError("Invalid response (empty JSON)")
        except aiohttp.client_exceptions.ClientConnectionError:
            raise RetryableError("Connection problem")
        except asyncio.exceptions.TimeoutError:
            raise RetryableError("Asyncio timeout")

    @classmethod
    async def _send_request(
        cls,
        aenter_to_send_request: contextlib.AbstractAsyncContextManager,
        expected_response_type: ResponseType,
        raise_if_request_fails: bool,
    ) -> Dict[str, Any]:
        async with aenter_to_send_request as response:
            status = response.status

            # Straightforward retryable errors
            if status == 401:
                raise InvalidAccessTokenError()
            if status == 429:
                retry_after = int(response.headers["Retry-After"])
                raise RateLimitedError(retry_after=retry_after)
            if status >= 500:
                raise ServerError(status=status)

            # Sometimes Spotify just returns empty data
            data = None
            if expected_response_type == ResponseType.JSON:
                data = await response.json()
                if not data:
                    raise UnexpectedEmptyResponseError()

            # Handle unretryable error
            if status >= 400 and raise_if_request_fails:
                error = (data or {}).get("error") or {}
                error_message = error.get("message")
                raise RequestFailedError(f"{error_message} ({status})")

            # Return data from "successful" request
            if expected_response_type == ResponseType.JSON:
                return data
            assert expected_response_type == ResponseType.EMPTY
            return {}

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
        logger.info(f"Getting {len(sorted_ids)} playlists")
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
        logger.info("Getting playlist ID(s)")
        while len(playlist_ids) < (limit or total):
            offset = len(playlist_ids)
            url = (
                self.BASE_URL
                + f"/users/{self.USER_ID}/playlists?limit={fetch_limit}&offset={offset}"
            )
            data = await self._make_retryable_request(method=HttpMethod.GET, url=url)
            playlist_ids |= {PublishedPlaylistID(item["id"]) for item in data["items"]}
            total = data["total"]  # total number of public playlists
            logger.info(f"Got {len(playlist_ids)} / {limit or total} playlist IDs")
        return playlist_ids

    async def _get_playlist(
        self, playlist_id: PublishedPlaylistID
    ) -> PublishedPlaylist:
        url = self.BASE_URL + f"/playlists/{playlist_id}?fields=name,description"
        data = await self._make_retryable_request(method=HttpMethod.GET, url=url)
        name = data["name"]
        description = data["description"].replace("&#x2F;", "/")
        logger.info(f"Getting playlist content: {name}")
        track_ids = await self._get_track_ids(playlist_id)
        logger.info(f"Got playlist content: {name}")
        return PublishedPlaylist(
            playlist_id=playlist_id,
            name=name,
            description=description,
            track_ids=track_ids,
        )

    async def _get_track_ids(self, playlist_id: PublishedPlaylistID) -> Set[str]:
        track_ids = set()
        url = (
            self.BASE_URL
            + f"/playlists/{playlist_id}/tracks?fields=next,items.track(id)"
        )
        while url:
            data = await self._make_retryable_request(method=HttpMethod.GET, url=url)
            for item in data["items"]:
                track = item["track"]
                if not track:
                    continue
                track_ids.add(track["id"])
            url = data["next"]
        return track_ids

    async def create_playlist(self, name: str) -> PublishedPlaylistID:
        url = self.BASE_URL + f"/users/{self.USER_ID}/playlists"
        data = await self._make_retryable_request(
            method=HttpMethod.POST,
            url=url,
            json={
                "name": name,
                "public": True,
                "collaborative": False,
            },
        )
        return PublishedPlaylistID(data["id"])

    async def unsubscribe_from_playlist(self, playlist_id: PublishedPlaylistID) -> None:
        url = self.BASE_URL + f"/playlists/{playlist_id}/followers"
        await self._make_retryable_request(
            method=HttpMethod.DELETE,
            url=url,
            expected_response_type=ResponseType.EMPTY,
        )

    async def change_playlist_details(
        self, playlist_id: PublishedPlaylistID, details_to_change: Mapping[str, str]
    ) -> None:
        description = details_to_change.get("description")
        if description and "\n" in description:
            raise Exception(f"Newlines in description are not allowed: {description}")
        await self._make_retryable_request(
            method=HttpMethod.PUT,
            url=self.BASE_URL + f"/playlists/{playlist_id}",
            json=details_to_change,
            expected_response_type=ResponseType.EMPTY,
        )

    async def add_items(
        self, playlist_id: PublishedPlaylistID, track_ids: Sequence[str]
    ) -> None:
        # Group the tracks in batches of 100, since that's the limit.
        for i in range(0, len(track_ids), 100):
            track_uris = [
                "spotify:track:{}".format(track_id)
                for track_id in track_ids[i : i + 100]  # noqa
            ]
            data = await self._make_retryable_request(
                method=HttpMethod.POST,
                url=self.BASE_URL + f"/playlists/{playlist_id}/tracks",
                json={"uris": track_uris},
                raise_if_request_fails=False,
            )
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
            await self._make_retryable_request(
                method=HttpMethod.DELETE,
                url=self.BASE_URL + f"/playlists/{playlist_id}/tracks",
                json={"tracks": track_uris},
            )

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
        cls,
        client_id: str,
        client_secret: str,
        refresh_token: str,
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
    def _get_session(cls) -> aiohttp.ClientSession:
        return aiohttp.ClientSession()

    @classmethod
    @external
    async def _sleep(cls, seconds: float) -> None:
        await asyncio.sleep(seconds)
