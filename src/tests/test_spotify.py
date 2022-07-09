#!/usr/bin/env python3

from __future__ import annotations

from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, Mock, patch

import aiohttp

from plants.unittest_utils import UnittestUtils
from playlist_types import PublishedPlaylist, PublishedPlaylistID
from spotify import (
    RequestFailedError,
    ResponseType,
    RetryableError,
    RetryBudgetExceededError,
    Spotify,
)


class TestSendRequestAndCoerceErrors(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.aenter_to_send_request = AsyncMock()

    async def test_invalid_access_token_error(self) -> None:
        async with self.aenter_to_send_request as response:
            response.status = 401
        with self.assertRaises(RetryableError) as e:
            await Spotify._send_request_and_coerce_errors(
                aenter_to_send_request=self.aenter_to_send_request,
                expected_response_type=ResponseType.JSON,
                raise_if_request_fails=True,
            )
        self.assertEqual(e.exception.sleep_seconds, 1)
        self.assertTrue(e.exception.refresh_access_token)

    async def test_rate_limited_error(self) -> None:
        async with self.aenter_to_send_request as response:
            response.status = 429
            response.headers = {"Retry-After": "2"}
        with self.assertRaises(RetryableError) as e:
            await Spotify._send_request_and_coerce_errors(
                aenter_to_send_request=self.aenter_to_send_request,
                expected_response_type=ResponseType.JSON,
                raise_if_request_fails=True,
            )
        self.assertEqual(e.exception.sleep_seconds, 3)
        self.assertFalse(e.exception.refresh_access_token)

    async def test_unexpected_empty_response_error(self) -> None:
        async with self.aenter_to_send_request as response:
            response.status = 200
            response.json.return_value = {}
        # Problematic for expected_response_type=ResponseType.JSON
        with self.assertRaises(RetryableError) as e:
            await Spotify._send_request_and_coerce_errors(
                aenter_to_send_request=self.aenter_to_send_request,
                expected_response_type=ResponseType.JSON,
                raise_if_request_fails=True,
            )
        self.assertEqual(e.exception.sleep_seconds, 1)
        self.assertFalse(e.exception.refresh_access_token)
        # No problems for expected_response_type=ResponseType.EMPTY
        await Spotify._send_request_and_coerce_errors(
            aenter_to_send_request=self.aenter_to_send_request,
            expected_response_type=ResponseType.EMPTY,
            raise_if_request_fails=True,
        )

    async def test_request_failed_error(self) -> None:
        async with self.aenter_to_send_request as response:
            response.status = 400
            response.json.return_value = {"error": {"message": "foo"}}
        # Problematic for raise_if_request_fails=True
        with self.assertRaises(RequestFailedError) as e:
            await Spotify._send_request_and_coerce_errors(
                aenter_to_send_request=self.aenter_to_send_request,
                expected_response_type=ResponseType.JSON,
                raise_if_request_fails=True,
            )
        self.assertEqual(str(e.exception), "foo (400)")
        # No problems for raise_if_request_fails=False
        await Spotify._send_request_and_coerce_errors(
            aenter_to_send_request=self.aenter_to_send_request,
            expected_response_type=ResponseType.JSON,
            raise_if_request_fails=False,
        )

    async def test_success(self) -> None:
        async with self.aenter_to_send_request as response:
            response.status = 201
            response.json.return_value = {"foo": "bar"}
        data = await Spotify._send_request_and_coerce_errors(
            aenter_to_send_request=self.aenter_to_send_request,
            expected_response_type=ResponseType.JSON,
            raise_if_request_fails=True,
        )
        self.assertEqual(data, {"foo": "bar"})


class MockSession(AsyncMock):
    @classmethod
    async def create(cls) -> MockSession:
        mock_session = MockSession()
        await mock_session._init()
        return mock_session

    async def _init(self) -> None:
        # AsyncMock objects beget other AsyncMock objects, but these methods
        # are synchronous so we need initialize them explicitly
        self.get = Mock(return_value=AsyncMock())
        self.put = Mock(return_value=AsyncMock())
        self.post = Mock(return_value=AsyncMock())
        self.delete = Mock(return_value=AsyncMock())
        # Allow MockSession objects to be used as async context managers
        async with self as session:
            session.get = self.get
            session.put = self.put
            session.post = self.post
            session.delete = self.delete


class SpotifyTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.mock_session = await MockSession.create()
        self.mock_get_session = UnittestUtils.patch(
            self,
            "spotify.Spotify._get_session",
            # new_callable returns the replacement for get_session
            new_callable=lambda: Mock(return_value=self.mock_session),
        )
        self.mock_get_user_access_token = UnittestUtils.patch(
            self,
            "spotify.Spotify.get_user_access_token",
            new_callable=AsyncMock,
        )
        self.mock_sleep = UnittestUtils.patch(
            self,
            "spotify.Spotify._sleep",
            new_callable=AsyncMock,
        )
        self.spotify = Spotify(
            client_id="client_id",
            client_secret="client_secret",
            refresh_token="refresh_token",
            retry_budget_seconds=10,
        )


class TestGetPlaylist(SpotifyTestCase):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.mock_get_track_ids = UnittestUtils.patch(
            self,
            "spotify.Spotify._get_track_ids",
            new_callable=AsyncMock,
        )
        self.mock_get_track_ids.return_value = {"track"}

    # Patch the logger to suppress log spew
    @patch("spotify.logger")
    async def test_exception(self, mock_logger: Mock) -> None:
        self.mock_session.get.return_value.__aenter__.side_effect = (
            aiohttp.client_exceptions.ClientOSError
        )
        with self.assertRaises(RetryBudgetExceededError):
            await self.spotify._get_playlist(PublishedPlaylistID("abc123"))

    async def test_failed_request(self) -> None:
        async with self.mock_session.get() as response:
            response.json = {"error": "error"}
        with self.assertRaises(Exception):
            await self.spotify._get_playlist(PublishedPlaylistID("abc123"))

    # Patch the logger to suppress log spew
    @patch("spotify.logger")
    async def test_server_unavailable(self, mock_logger: Mock) -> None:
        async with self.mock_session.get() as response:
            response.status = 500
        with self.assertRaises(RetryBudgetExceededError):
            await self.spotify._get_playlist(PublishedPlaylistID("abc123"))

    # Patch the logger to suppress log spew
    @patch("spotify.logger")
    async def test_transient_server_error(self, mock_logger: Mock) -> None:
        mock_responses = [
            AsyncMock(status=500),
            AsyncMock(
                status=200,
                json=AsyncMock(
                    return_value={
                        "name": "playlist_name",
                        "description": "playlist_description",
                    }
                ),
            ),
        ]
        self.mock_session.get.return_value.__aenter__.side_effect = mock_responses
        playlist_id = PublishedPlaylistID("playlist_id")
        playlist = await self.spotify._get_playlist(playlist_id)
        self.assertEqual(
            playlist,
            PublishedPlaylist(
                playlist_id=playlist_id,
                name="playlist_name",
                description="playlist_description",
                track_ids={"track"},
            ),
        )
        self.assertEqual(self.mock_session.get.call_count, 2)
        self.mock_sleep.assert_called_once_with(1)

    # Patch the logger to suppress log spew
    @patch("spotify.logger")
    async def test_rate_limited(self, mock_logger: Mock) -> None:
        mock_responses = [
            AsyncMock(
                status=429,
                headers={"Retry-After": 4.2},
            ),
            AsyncMock(
                status=200,
                json=AsyncMock(
                    return_value={
                        "name": "playlist_name",
                        # Test URL escaping in playlist description
                        "description": "https:&#x2F;&#x2F;foo",
                    }
                ),
            ),
        ]
        self.mock_session.get.return_value.__aenter__.side_effect = mock_responses
        playlist_id = PublishedPlaylistID("playlist_id")
        playlist = await self.spotify._get_playlist(playlist_id)
        self.assertEqual(
            playlist,
            PublishedPlaylist(
                playlist_id=playlist_id,
                name="playlist_name",
                description="https://foo",
                track_ids={"track"},
            ),
        )
        self.assertEqual(self.mock_session.get.call_count, 2)
        self.mock_sleep.assert_called_once_with(5)


class TestShutdown(SpotifyTestCase):
    async def test_success(self) -> None:
        await self.spotify.shutdown()
        self.mock_session.close.assert_called_once()
        self.mock_sleep.assert_called_once_with(0)
