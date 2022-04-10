#!/usr/bin/env python3

import dataclasses
from typing import AbstractSet, Generic, NewType, TypeVar

TPlaylistID = TypeVar("TPlaylistID", bound=str)
ScrapedPlaylistID = NewType("ScrapedPlaylistID", str)
PublishedPlaylistID = NewType("PublishedPlaylistID", str)


@dataclasses.dataclass(frozen=True)
class Playlist(Generic[TPlaylistID]):
    playlist_id: TPlaylistID
    name: str
    description: str
    track_ids: AbstractSet[str]


class ScrapedPlaylist(Playlist[ScrapedPlaylistID]):
    pass


class PublishedPlaylist(Playlist[PublishedPlaylistID]):
    pass
