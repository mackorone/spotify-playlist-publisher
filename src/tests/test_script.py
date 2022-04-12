#!/usr/bin/env python3

import os
import pathlib
import tempfile
import textwrap
from typing import AsyncIterator, Dict, TypeVar
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, Mock, call, patch, sentinel

from playlist_types import (
    PublishedPlaylist,
    PublishedPlaylistID,
    ScrapedPlaylist,
    ScrapedPlaylistID,
)
from script import (
    PlaylistMapping,
    Playlists,
    get_scraped_playlists,
    publish_impl,
)

T = TypeVar("T")


class TestPlaylistsToAndFromJSON(TestCase):
    def test_overlaps(self) -> None:
        cases = [
            """
            {
              "mappings": [
                {
                  "scraped_playlist_id": "foo"
                  "published_playlist_ids": ["abc", "abc"],
                }
              ]
            }
            """,
            """
            {
              "mappings": [
                {
                  "scraped_playlist_id": "foo"
                  "published_playlist_ids": ["abc"],
                },
                {
                  "scraped_playlist_id": "bar"
                  "published_playlist_ids": ["abc"],
                }
              ]
            }
            """,
        ]
        for playlists_json in cases:
            with self.assertRaises(Exception):
                Playlists.from_json(playlists_json)

    def test_success(self) -> None:
        playlists = Playlists(
            mappings=[
                PlaylistMapping(
                    scraped_playlist_id=ScrapedPlaylistID("scraped_2"),
                    published_playlist_ids=[
                        PublishedPlaylistID("published_2a"),
                        PublishedPlaylistID("published_2b"),
                    ],
                ),
                PlaylistMapping(
                    scraped_playlist_id=ScrapedPlaylistID("scraped_1"),
                    published_playlist_ids=[
                        PublishedPlaylistID("published_1b"),
                        PublishedPlaylistID("published_1a"),
                    ],
                ),
            ],
        )
        playlists_json = playlists.to_json()
        self.assertEqual(
            playlists_json,
            textwrap.dedent(
                """\
                {
                  "mappings": [
                    {
                      "published_playlist_ids": [
                        "published_2a",
                        "published_2b"
                      ],
                      "scraped_playlist_id": "scraped_2"
                    },
                    {
                      "published_playlist_ids": [
                        "published_1b",
                        "published_1a"
                      ],
                      "scraped_playlist_id": "scraped_1"
                    }
                  ]
                }"""
            ),
        )
        self.assertEqual(playlists, Playlists.from_json(playlists_json))


class TestGetScrapedPlaylists(TestCase):
    # Patch the logger to suppress log spew
    @patch("script.logger")
    def test_success(self, mock_logger: Mock) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            playlists_dir = pathlib.Path(temp_dir)
            cumulative_dir = playlists_dir / "cumulative"
            os.mkdir(cumulative_dir)
            with open(cumulative_dir / "foo.json", "w") as f:
                f.write(
                    textwrap.dedent(
                        """\
                        {
                          "description": "The foo playlist",
                          "name": "Foo",
                          "tracks": [
                            {"url": "https://open.spotify.com/track/1"},
                            {"url": "https://open.spotify.com/track/2"},
                            {"url": "https://open.spotify.com/track/3"}
                          ]
                        }
                        """
                    )
                )
            with open(cumulative_dir / "bar.json", "w") as f:
                f.write(
                    textwrap.dedent(
                        """\
                        {
                          "description": "The bar playlist",
                          "name": "Bar",
                          "tracks": [
                            {"url": "https://open.spotify.com/track/3"},
                            {"url": "https://open.spotify.com/track/4"},
                            {"url": "https://open.spotify.com/track/5"}
                          ]
                        }
                        """
                    )
                )

            scraped_playlists = get_scraped_playlists(playlists_dir)
            self.assertEqual(
                scraped_playlists,
                {
                    ScrapedPlaylistID("foo"): ScrapedPlaylist(
                        playlist_id=ScrapedPlaylistID("foo"),
                        name="Foo (Cumulative)",
                        description="The foo playlist",
                        track_ids={"1", "2", "3"},
                    ),
                    ScrapedPlaylistID("bar"): ScrapedPlaylist(
                        playlist_id=ScrapedPlaylistID("bar"),
                        name="Bar (Cumulative)",
                        description="The bar playlist",
                        track_ids={"3", "4", "5"},
                    ),
                },
            )


class TestPublishImpl(IsolatedAsyncioTestCase):
    def _patch(
        self,
        target: str,
        side_effect: T,
    ) -> T:
        patcher = patch(target, side_effect=side_effect)
        mock_object = patcher.start()
        self.addCleanup(patcher.stop)
        return mock_object

    async def asyncSetUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.repo_dir = pathlib.Path(self.temp_dir.name)

        self.mock_get_repo_root = self._patch(
            "plants.environment.Environment.get_repo_root",
            side_effect=[self.repo_dir],
        )

        self.mock_get_scraped_playlists = self._patch(
            "script.get_scraped_playlists",
            side_effect=self._mock_get_scraped_playlists,
        )

        self.mock_playlists_dir = sentinel.playlists_dir

        self.mock_spotify = Mock()
        self.mock_spotify.get_published_playlists.side_effect = (
            self._mock_get_published_playlists
        )
        self.mock_spotify.create_playlist = AsyncMock()
        self.mock_spotify.create_playlist.side_effect = lambda name: {
            "scraped_2_name": "published_4_id",
            "scraped_3_name": "published_5_id",
        }[name]
        self.mock_spotify.unsubscribe_from_playlist = AsyncMock()
        self.mock_spotify.add_items = AsyncMock()
        self.mock_spotify.remove_items = AsyncMock()

    async def asyncTearDown(self) -> None:
        self.temp_dir.cleanup()

    def _mock_get_scraped_playlists(
        self, playlists_dir: pathlib.Path
    ) -> Dict[ScrapedPlaylistID, ScrapedPlaylist]:
        return {
            # Has a valid mapping
            ScrapedPlaylistID("scraped_1_id"): ScrapedPlaylist(
                playlist_id=ScrapedPlaylistID("scraped_1_id"),
                name="scraped_1_name",
                description="Scraped_1_desc",
                track_ids={"1", "2"},
            ),
            # Has an invalid mapping
            ScrapedPlaylistID("scraped_2_id"): ScrapedPlaylist(
                playlist_id=ScrapedPlaylistID("scraped_2_id"),
                name="scraped_2_name",
                description="scraped_2_desc",
                track_ids={"123"},
            ),
            # Has no mapping
            ScrapedPlaylistID("scraped_3_id"): ScrapedPlaylist(
                playlist_id=ScrapedPlaylistID("scraped_3_id"),
                name="scraped_3_name",
                description="scraped_3_desc",
                track_ids={"123"},
            ),
        }

    async def _mock_get_published_playlists(self) -> AsyncIterator[PublishedPlaylist]:
        # Has a valid mapping
        yield PublishedPlaylist(
            playlist_id=PublishedPlaylistID("published_1_id"),
            name="\t  published_1_name   ",  # ensure whitespace is stripped
            description="published_1_desc",
            track_ids={"2", "3"},
        )
        # Has an invalid mapping
        yield PublishedPlaylist(
            playlist_id=PublishedPlaylistID("published_2_id"),
            name="published_2_name",
            description="published_2_desc",
            track_ids={"456"},
        )
        # Has no mapping (unreferenced)
        yield PublishedPlaylist(
            playlist_id=PublishedPlaylistID("published_3_id"),
            name="published_3_name",
            description="published_3_desc",
            track_ids={"456"},
        )

    # Patch the logger to suppress log spew
    @patch("script.logger")
    async def test_success(self, mock_logger: Mock) -> None:
        with open(self.repo_dir / "README.md", "w") as f:
            f.write(
                textwrap.dedent(
                    """\
                    Arbitrary text

                    ## Playlists
                    """
                )
            )
        with open(self.repo_dir / "playlists.json", "w") as f:
            f.write(
                textwrap.dedent(
                    """\
                    {
                      "mappings": [
                        {
                          "scraped_playlist_id": "scraped_1_id",
                          "published_playlist_ids": [
                            "published_1_id"
                          ]
                        },
                        {
                          "scraped_playlist_id": "scraped_2_id",
                          "published_playlist_ids": [
                            "published_invalid_id"
                          ]
                        },
                        {
                          "scraped_playlist_id": "scraped_invalid_id",
                          "published_playlist_ids": [
                            "published_2_id"
                          ]
                        }
                      ]
                    }
                    """
                )
            )
        await publish_impl(
            spotify=self.mock_spotify,
            playlists_dir=self.mock_playlists_dir,
            prod=True,
        )
        self.mock_get_scraped_playlists.assert_called_once_with(self.mock_playlists_dir)
        self.mock_spotify.get_published_playlists.assert_called_once_with()
        with open(self.repo_dir / "README.md", "r") as f:
            link_prefix = "https://open.spotify.com/playlist"
            self.assertEqual(
                f.read(),
                textwrap.dedent(
                    f"""\
                    Arbitrary text

                    ## Playlists

                    - [published\\_1\\_name]({link_prefix}/published_1_id)
                    - [scraped\\_2\\_name]({link_prefix}/published_4_id)
                    - [scraped\\_3\\_name]({link_prefix}/published_5_id)
                    """,
                ),
            )
        with open(self.repo_dir / "playlists.json", "r") as f:
            self.assertEqual(
                f.read(),
                textwrap.dedent(
                    """\
                    {
                      "mappings": [
                        {
                          "published_playlist_ids": [
                            "published_1_id"
                          ],
                          "scraped_playlist_id": "scraped_1_id"
                        },
                        {
                          "published_playlist_ids": [
                            "published_4_id"
                          ],
                          "scraped_playlist_id": "scraped_2_id"
                        },
                        {
                          "published_playlist_ids": [
                            "published_5_id"
                          ],
                          "scraped_playlist_id": "scraped_3_id"
                        }
                      ]
                    }
                    """,
                ),
            )
        self.mock_spotify.create_playlist.assert_has_calls(
            [
                call("scraped_2_name"),
                call("scraped_3_name"),
            ]
        )
        self.mock_spotify.unsubscribe_from_playlist.assert_has_calls(
            [
                call("published_2_id"),
                call("published_3_id"),
            ]
        )
        self.mock_spotify.add_items.assert_has_calls(
            [
                call("published_1_id", ["1"]),
                call("published_4_id", ["123"]),
                call("published_5_id", ["123"]),
            ]
        )
        self.mock_spotify.remove_items.assert_has_calls(
            [
                call("published_1_id", ["3"]),
            ]
        )
