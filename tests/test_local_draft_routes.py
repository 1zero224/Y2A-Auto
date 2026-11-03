import json
from unittest.mock import Mock

import pytest

import app as web
from modules import task_manager as tm
from modules.config_manager import DEFAULT_CONFIG


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(tm, 'DB_PATH', str(tmp_path / 'tasks.db'))
    monkeypatch.setattr(tm, 'get_global_task_processor', Mock())
    monkeypatch.setattr(tm, 'emit_notification_event', Mock())
    monkeypatch.setattr(web, 'load_config', lambda: dict(DEFAULT_CONFIG))
    tm.init_db()
    web.app.config['TESTING'] = True
    return web.app.test_client()


def test_save_edit_reopen_and_explicit_publish(client, monkeypatch):
    tid = tm.add_task('https://www.youtube.com/watch?v=aaaaaaaaaaa', 'bilibili')
    tm.update_task(tid, status='ready_for_upload', video_title_original='Original',
                   description_original='Original description')
    publish = Mock()
    monkeypatch.setattr(web, '_start_background_force_upload', publish)
    form = {'action': 'save_metadata', 'video_title_translated': '人工确认的标题',
            'description_translated': '人工确认的简介', 'selected_partition_id_bilibili': '171',
            'tags_json': '["标签"]'}
    assert client.post(f'/tasks/{tid}/edit', data=form).status_code == 302
    publish.assert_not_called()
    # AI can regenerate its own output, but persisted human edits still win.
    tm.update_task(tid, video_title_translated='AI changed', description_translated='AI changed')
    response = client.get(f'/tasks/{tid}/edit')
    assert response.status_code == 200
    assert '人工确认的标题' in response.get_data(as_text=True)
    assert '人工确认的简介' in response.get_data(as_text=True)
    assert response.get_data(as_text=True).count('class="form-control tag-input"') == 12
    assert '人工确认的标题' in client.get('/drafts').get_data(as_text=True)
    form['action'] = 'force_upload'
    response = client.post(f'/tasks/{tid}/edit', data=form)
    assert response.status_code == 302
    publish.assert_called_once()
    assert json.loads(tm.get_task(tid)['draft_metadata_json'])['video_title_translated'] == '人工确认的标题'


def test_failed_save_never_starts_publish(client, monkeypatch):
    tid = tm.add_task('https://www.youtube.com/watch?v=aaaaaaaaaaa', 'bilibili')
    publish = Mock()
    monkeypatch.setattr(web, '_start_background_force_upload', publish)
    monkeypatch.setattr(web, 'update_task', lambda *a, **k: False)
    response = client.post(f'/tasks/{tid}/edit', data={'action': 'force_upload'})
    assert response.status_code == 302
    publish.assert_not_called()
