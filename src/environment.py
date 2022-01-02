#!/usr/bin/env python3

import os
import pathlib
from typing import Optional


class Environment:
    @classmethod
    def get_env(cls, name: str) -> Optional[str]:
        return os.getenv(name)

    @classmethod
    def get_repo_dir(cls) -> pathlib.Path:
        repo_dir = pathlib.Path(__file__).resolve().parent.parent
        assert repo_dir.name == "spotify-playlist-publisher"
        return pathlib.Path(os.path.relpath(repo_dir))
