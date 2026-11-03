import uuid

import pytest

import app as web


@pytest.fixture
def media(tmp_path, monkeypatch):
    downloads = tmp_path / 'downloads'
    tid = str(uuid.uuid4())
    directory = downloads / tid
    directory.mkdir(parents=True)
    source = directory / 'video.mp4'
    source.write_bytes(b'original-video')
    finished = directory / 'video_subtitled.mp4'
    finished.write_bytes(b'finished-video-with-subtitles')
    task = {'id': tid, 'status': 'ready_for_upload', 'video_path_local': str(finished),
            'video_title_translated': '中文标题/测试', 'upload_target': 'bilibili'}
    monkeypatch.setattr(web, 'get_app_subdir', lambda name: str(tmp_path / name))
    monkeypatch.setattr(web, 'get_task', lambda task_id: task if task_id == tid else None)
    monkeypatch.setattr(web, 'load_config', lambda: {})
    web.app.config['TESTING'] = True
    return web.app.test_client(), task, finished, tmp_path


def test_download_current_processed_file_and_range(media):
    client, task, finished, _ = media
    url = f"/tasks/{task['id']}/video/download"
    response = client.get(url)
    assert response.status_code == 200
    assert response.data == finished.read_bytes()
    assert 'attachment;' in response.headers['Content-Disposition']
    assert "filename*=UTF-8''" in response.headers['Content-Disposition']
    assert response.headers['Cache-Control'] == 'private, no-store'
    response.close()
    partial = client.get(url, headers={'Range': 'bytes=0-7'})
    assert partial.status_code == 206 and partial.data == b'finished'
    partial.close()


def test_login_required(media, monkeypatch):
    client, task, _, _ = media
    monkeypatch.setattr(web, 'load_config', lambda: {'password_protection_enabled': True})
    assert client.get(f"/tasks/{task['id']}/video/download").status_code == 302


def test_missing_processing_and_cross_task_paths_rejected(media):
    client, task, finished, root = media
    url = f"/tasks/{task['id']}/video/download"
    task['status'] = 'encoding_video'
    assert client.get(url).status_code == 404
    task['status'] = 'ready_for_upload'
    outside = root / 'private.mp4'
    outside.write_bytes(b'private')
    task['video_path_local'] = str(outside)
    assert client.get(url).status_code == 404
    task['video_path_local'] = str(finished)
    finished.unlink()
    assert client.get(url).status_code == 404
    assert client.get('/tasks/nonexistent/video/download').status_code == 404


def test_file_symlink_escape_rejected(media):
    client, task, finished, root = media
    outside = root / 'private.mp4'
    outside.write_bytes(b'private')
    link = finished.parent / 'escape.mp4'
    try:
        link.symlink_to(outside)
    except OSError:
        pytest.skip('Symlink privileges required')
    task['video_path_local'] = str(link)
    assert client.get(f"/tasks/{task['id']}/video/download").status_code == 404
