# spotify-playlist-publisher [![Build Status](https://travis-ci.com/mackorone/spotify-playlist-publisher.svg?branch=master)](https://travis-ci.com/mackorone/spotify-playlist-publisher)

> Publishing cumulative playlists back to Spotify

- Published playlists: [open.spotify.com/user/w6hfc0hfa4s53j4l44mqn2ppe/playlists](https://open.spotify.com/user/w6hfc0hfa4s53j4l44mqn2ppe/playlists)
- GitHub archive: [github.com/mackorone/spotify-playlist-archive](https://github.com/mackorone/spotify-playlist-archive)

## Development

This project uses [`pip-tools`](https://github.com/jazzband/pip-tools) to manage
dependencies.

To get started, first create and activate a new virtual environment:
```
$ python3.8 -m venv venv
$ source venv/bin/activate
```

Then install `pip-tools`:
```
$ pip install pip-tools
```

Lastly, use `pip-sync` to install the dev requirements:
```
$ pip-sync requirements/requirements-dev.txt
```
