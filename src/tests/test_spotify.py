#!/usr/bin/env python3

from __future__ import annotations

from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, Mock, patch

import aiohttp

from plants.unittest_utils import UnittestUtils
from playlist_types import PublishedPlaylist, PublishedPlaylistID
from spotify import RetryBudgetExceededError, Spotify


class SpotifyTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.mock_session = AsyncMock()

        # Retain references to the decorated functions
        self.mock_get = self.mock_session.get
        self.mock_put = self.mock_session.put
        self.mock_post = self.mock_session.post
        self.mock_delete = self.mock_session.delete

        self.mock_get_session = UnittestUtils.patch(
            self,
            "spotify.Spotify._get_session",
            # new_callable returns the replacement for get_session
            new_callable=lambda: Mock(return_value=self.mock_session),
        )
        self.mock_sleep = UnittestUtils.patch(
            self,
            "spotify.Spotify._sleep",
            new_callable=AsyncMock,
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
        self.mock_session.get.side_effect = aiohttp.client_exceptions.ClientOSError
        spotify = Spotify("token")
        with self.assertRaises(RetryBudgetExceededError):
            await spotify._get_playlist(PublishedPlaylistID("abc123"))

    async def test_failed_request(self) -> None:
        self.mock_session.get.return_value.json.return_value = {"error": "error"}
        spotify = Spotify("token")
        with self.assertRaises(Exception):
            await spotify._get_playlist(PublishedPlaylistID("abc123"))

    # Patch the logger to suppress log spew
    @patch("spotify.logger")
    async def test_server_unavailable(self, mock_logger: Mock) -> None:
        self.mock_session.get.return_value.status = 500
        spotify = Spotify("token")
        with self.assertRaises(RetryBudgetExceededError):
            await spotify._get_playlist(PublishedPlaylistID("abc123"))

    # Patch the logger to suppress log spew
    @patch("spotify.logger")
    async def test_transient_server_error(self, mock_logger: Mock) -> None:
        mock_responses = [
            AsyncMock(status=500),
            AsyncMock(
                json=AsyncMock(
                    return_value={
                        "name": "playlist_name",
                        "description": "playlist_description",
                    }
                )
            ),
        ]
        self.mock_session.get.side_effect = mock_responses
        spotify = Spotify("token")
        playlist_id = PublishedPlaylistID("playlist_id")
        playlist = await spotify._get_playlist(playlist_id)
        self.assertEqual(
            playlist,
            PublishedPlaylist(
                playlist_id=playlist_id,
                name="playlist_name",
                description="playlist_description",
                track_ids={"track"},
            ),
        )
        self.assertEqual(self.mock_get.call_count, 2)
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
                json=AsyncMock(
                    return_value={
                        "name": "playlist_name",
                        "description": "playlist_description",
                    }
                )
            ),
        ]
        self.mock_session.get.side_effect = mock_responses
        spotify = Spotify("token")
        playlist_id = PublishedPlaylistID("playlist_id")
        playlist = await spotify._get_playlist(playlist_id)
        self.assertEqual(
            playlist,
            PublishedPlaylist(
                playlist_id=playlist_id,
                name="playlist_name",
                description="playlist_description",
                track_ids={"track"},
            ),
        )
        self.assertEqual(self.mock_get.call_count, 2)
        self.mock_sleep.assert_called_once_with(5)


class TestShutdown(SpotifyTestCase):
    async def test_success(self) -> None:
        spotify = Spotify("token")
        await spotify.shutdown()
        self.mock_session.close.assert_called_once()
        self.mock_sleep.assert_called_once_with(0)
