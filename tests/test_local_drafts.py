import json
import threading
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

import pytest

from modules import task_manager as tm
from modules.local_drafts import serialize_edits


@pytest.fixture
def database(tmp_path, monkeypatch):
    monkeypatch.setattr(tm, 'DB_PATH', str(tmp_path / 'tasks.db'))
    monkeypatch.setattr(tm, 'get_global_task_processor', Mock())
    monkeypatch.setattr(tm, 'emit_notification_event', Mock())
    tm.init_db()
    return tmp_path


def test_human_edits_survive_ai_regeneration_and_database_reopen(database):
    task = tm.add_task('https://www.youtube.com/watch?v=aaaaaaaaaaa', 'bilibili')
    assert tm.update_task(task, draft_metadata_json=serialize_edits({
        'video_title_translated': '人工标题', 'description_translated': '人工简介',
        'tags_generated': '["人工标签"]'}))
    assert tm.update_task(task, video_title_translated='重新生成的标题',
                          description_translated='重新生成的简介', tags_generated='["AI标签"]')
    current = tm.get_task(task)
    assert current['video_title_translated'] == '人工标题'
    assert current['description_translated'] == '人工简介'
    assert tm.get_tasks_by_status('pending')[0]['tags_generated'] == '["人工标签"]'
    with tm.get_db_connection() as db:
        assert db.execute('SELECT video_title_translated FROM tasks').fetchone()[0] == '重新生成的标题'
    tm.init_db()  # existing database migration is idempotent
    assert tm.get_task(task)['video_title_translated'] == '人工标题'






def test_batch_retry_does_not_publish_local_drafts(database, monkeypatch):
    task = tm.add_task('https://www.youtube.com/watch?v=aaaaaaaaaaa', 'bilibili')
    video = database / 'download.mp4'
    video.write_bytes(b'local fixture')
    tm.update_task(task, status='failed', error_message='bilibili upload failed', video_path_local=str(video))
    publish = Mock()
    monkeypatch.setattr(tm, '_start_background_upload_retry', publish)
    tm.retry_failed_tasks({'BILIBILI_LOCAL_DRAFT_ENABLED': True})
    publish.assert_not_called()
    assert tm.get_task(task)['status'] == 'ready_for_upload'


def test_automatic_pipeline_stops_at_draft_even_in_auto_mode(database, monkeypatch):
    task_id = tm.add_task('https://www.youtube.com/watch?v=aaaaaaaaaaa', 'bilibili')
    processor = tm.TaskProcessor.__new__(tm.TaskProcessor)
    processor.config = {'AUTO_MODE_ENABLED': True, 'BILIBILI_LOCAL_DRAFT_ENABLED': True,
                        'TRANSLATE_TITLE': True, 'TRANSLATE_DESCRIPTION': True,
                        'GENERATE_TAGS': False, 'SUBTITLE_EMBED_IN_VIDEO': False}
    processor._current_max_concurrent_tasks = 1
    processor._fetch_video_info = Mock()
    processor._translate_content = Mock(return_value=True)
    processor._download_video_file = Mock()
    processor._upload_to_target = Mock()
    processor._check_and_start_next_pending_task = Mock()
    monkeypatch.setattr(tm, '_get_completed_stages', lambda *args: set())
    processor.process_task(task_id, slot_already_acquired=True, acquired_task_semaphore=threading.Semaphore(0))
    processor._translate_content.assert_called_once()
    processor._download_video_file.assert_called_once()
    processor._upload_to_target.assert_not_called()
    assert tm.get_task(task_id)['status'] == tm.TASK_STATES['READY_FOR_UPLOAD']


def test_manual_publish_uses_saved_draft_and_rejects_concurrent_click(database, monkeypatch):
    task_id = tm.add_task('https://www.youtube.com/watch?v=aaaaaaaaaaa', 'bilibili')
    tm.update_task(task_id, status='ready_for_upload', draft_metadata_json=serialize_edits({
        'video_title_translated': '最终标题', 'description_translated': '最终简介'}))
    observed = []
    def upload(tid, *args, **kwargs):
        observed.append(tm.get_task(tid))
        assert tm.force_upload_task(tid, {'AUTO_MODE_ENABLED': True}) is False
        tm.update_task(tid, status='completed', bilibili_upload_response=json.dumps({'bvid': 'BV-test'}))
    processor = Mock()
    processor._upload_to_target.side_effect = upload
    monkeypatch.setattr(tm, 'get_global_task_processor', lambda config: processor)
    assert tm.force_upload_task(task_id, {'AUTO_MODE_ENABLED': True})
    assert observed[0]['video_title_translated'] == '最终标题'
    assert observed[0]['description_translated'] == '最终简介'
    assert tm.get_task(task_id)['status'] == 'completed'
