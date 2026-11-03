from pathlib import Path
from unittest.mock import patch

import pytest

from modules import youtube_handler


def test_cookie_directory_can_be_on_persistent_storage(tmp_path):
    root = tmp_path / 'source'
    storage = tmp_path / 'data' / 'cookies'
    root.mkdir()
    storage.mkdir(parents=True)
    cookie = storage / 'yt_cookies.txt'
    cookie.write_text('# Netscape HTTP Cookie File\n', encoding='utf-8')
    link = root / 'cookies'
    try:
        link.symlink_to(storage, target_is_directory=True)
    except OSError:
        pytest.skip('Creating directory symlinks requires platform privileges')
    with patch.object(youtube_handler, '_BASE_DIR', str(root)):
        assert youtube_handler._resolve_safe_cookies_path('cookies/yt_cookies.txt') == str(cookie)
        assert youtube_handler._resolve_safe_cookies_path(str(cookie)) == str(cookie)


def test_symlinked_cookie_file_cannot_escape_persistent_directory(tmp_path):
    root = tmp_path / 'source'
    storage = tmp_path / 'data' / 'cookies'
    root.mkdir()
    storage.mkdir(parents=True)
    outside = tmp_path / 'private.txt'
    outside.write_text('not a cookie', encoding='utf-8')
    try:
        (root / 'cookies').symlink_to(storage, target_is_directory=True)
        (storage / 'escape.txt').symlink_to(outside)
    except OSError:
        pytest.skip('Creating symlinks requires platform privileges')
    with patch.object(youtube_handler, '_BASE_DIR', str(root)):
        assert youtube_handler._resolve_safe_cookies_path('cookies/escape.txt') is None
        assert youtube_handler._resolve_safe_cookies_path('../private.txt') is None


def test_regular_project_cookie_still_works(tmp_path):
    cookie = tmp_path / 'cookies' / 'yt_cookies.txt'
    cookie.parent.mkdir()
    cookie.write_text('# Netscape HTTP Cookie File\n', encoding='utf-8')
    with patch.object(youtube_handler, '_BASE_DIR', str(tmp_path)):
        assert youtube_handler._resolve_safe_cookies_path('cookies/yt_cookies.txt') == str(cookie)
