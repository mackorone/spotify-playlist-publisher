#!/usr/bin/env python3

import dataclasses
from typing import AbstractSet


@dataclasses.dataclass(frozen=True)
class Playlist:
    playlist_id: str
    name: str
    description: str
    track_ids: AbstractSet[str]


class ScrapedPlaylist(Playlist):
    pass


class PublishedPlaylist(Playlist):
    pass
