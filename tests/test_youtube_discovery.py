import hashlib
import hmac
import sqlite3
import threading
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from modules.youtube_discovery import YouTubeDiscovery, TOPIC

CHANNEL = 'UC' + 'a' * 22
VIDEO = 'b' * 11


@pytest.fixture
def service(tmp_path):
    config = {'id': 1, 'enabled': True, 'schedule_type': 'auto', 'channel_mode': 'latest',
              'channel_ids': CHANNEL, 'auto_add_to_tasks': True}
    monitor = SimpleNamespace(db_path=str(tmp_path / 'monitor.db'),
        get_monitor_configs=lambda: [config], youtube=Mock(),
        _discovery_api_lock=threading.RLock(), _filter_videos=lambda items, c: items,
        record_discovered_video=Mock())
    now = [1700000000.0]
    adder = Mock(return_value='task-1')
    s = YouTubeDiscovery(monitor, lambda: {'YOUTUBE_WEBSUB_ENABLED': True}, adder,
                        probe=Mock(return_value=True), clock=lambda: now[0])
    s.enroll(CHANNEL)
    s.now, s.config = now, config
    with s.connect() as db:
        token = db.execute('SELECT token FROM discovery_channels').fetchone()[0]
    args = {'hub.topic': TOPIC + CHANNEL, 'hub.mode': 'subscribe',
            'hub.verify_token': token, 'hub.lease_seconds': '1000', 'hub.challenge': 'challenge'}
    assert s.verify(args) == 'challenge'
    return s


def payload(service, published=None):
    published = published or datetime.fromtimestamp(service.now[0] + 1, timezone.utc).isoformat()
    body = (f'<feed xmlns="http://www.w3.org/2005/Atom" xmlns:yt="http://www.youtube.com/xml/schemas/2015">'
            f'<entry><yt:videoId>{VIDEO}</yt:videoId><yt:channelId>{CHANNEL}</yt:channelId>'
            f'<published>{published}</published></entry></feed>').encode()
    with service.connect() as db:
        secret = db.execute('SELECT secret FROM discovery_channels').fetchone()[0]
    return body, 'sha256=' + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def video_response():
    return {'items': [{'id': VIDEO, 'status': {'privacyStatus': 'public'},
                       'snippet': {'channelId': CHANNEL}}]}


def test_signed_push_durable_dedup_and_restart(service):
    body, signature = payload(service)
    service.receive(body, signature)
    service.receive(body, signature)
    restarted = YouTubeDiscovery(service.monitor, service.load_config, service.add_task,
                                probe=service.probe, clock=service.clock)
    assert len(restarted.status()['candidates']) == 1
    restarted.api = Mock(return_value=video_response())
    restarted.process_candidates({})
    assert restarted.status()['candidates'][0]['state'] == 'ready'
    service.add_task.assert_called_once_with('https://www.youtube.com/watch?v=' + VIDEO,
                                           source_key='youtube:' + VIDEO)
    restarted.process_candidates({})
    assert service.add_task.call_count == 1


def test_invalid_signature_old_updates_and_expired_lease(service):
    body, signature = payload(service)
    with pytest.raises(ValueError):
        service.receive(body, 'sha256=bad')
    old, old_signature = payload(service, '2020-01-01T00:00:00Z')
    assert service.receive(old, old_signature) == 0
    assert not service.status()['candidates']
    service.now[0] += 1001
    with pytest.raises(ValueError):
        service.receive(body, signature)


def test_unavailable_and_failed_enqueue_retry_without_losing_candidate(service):
    service.receive(*payload(service))
    service.api = Mock(return_value=video_response())
    service.probe.return_value = False
    service.process_candidates({})
    row = service.status()['candidates'][0]
    assert row['state'] == 'waiting' and row['attempts'] == 1
    service.add_task.assert_not_called()
    service.now[0] = row['next_check']
    service.probe.return_value = True
    service.add_task.return_value = None
    service.process_candidates({})
    row = service.status()['candidates'][0]
    assert row['state'] == 'waiting' and row['attempts'] == 2
    service.now[0] = row['next_check']
    service.add_task.return_value = 'task-1'
    service.process_candidates({})
    assert service.status()['candidates'][0]['task_id'] == 'task-1'


def test_premiere_waits_and_disabled_config_cannot_enqueue(service):
    service.receive(*payload(service))
    response = video_response()
    response['items'][0]['snippet']['liveBroadcastContent'] = 'upcoming'
    service.api = Mock(return_value=response)
    service.process_candidates({})
    service.probe.assert_not_called()
    service.add_task.assert_not_called()
    service.config['enabled'] = False
    service.now[0] += 100
    service.process_candidates({})
    service.add_task.assert_not_called()


def test_reconcile_paginates_and_preserves_cursor_on_error(service):
    at = datetime.fromtimestamp(service.now[0] + 1, timezone.utc).isoformat()
    with service.connect() as db:
        db.execute('UPDATE discovery_channels SET playlist_id=?', ('uploads',))
    service.api = Mock(side_effect=[{'items': [{'snippet': {'publishedAt': at,
                'resourceId': {'videoId': VIDEO}}}], 'nextPageToken': 'next'}, RuntimeError('offline')])
    with pytest.raises(RuntimeError):
        service.reconcile(CHANNEL, {})
    assert service.status()['channels'][0]['last_poll'] == 0
    assert len(service.status()['candidates']) == 1
    service.api = Mock(return_value={'items': []})
    service.reconcile(CHANNEL, {})
    assert service.status()['channels'][0]['last_poll'] == service.now[0]


def test_subscription_error_does_not_stop_reconciliation(service):
    service.reconcile = Mock()
    service.renew = Mock(side_effect=RuntimeError('hub offline'))
    service.process_candidates = Mock()
    service.tick()
    service.reconcile.assert_called_once()
    service.process_candidates.assert_called_once()


def test_untrusted_xml_rejected(service):
    with pytest.raises(ValueError):
        service.receive(b'<!DOCTYPE x><feed/>', '')


def test_inactive_candidates_do_not_starve_active_queue(service):
    for number in range(12):
        service.enqueue(99, str(number).zfill(11), CHANNEL)
    service.receive(*payload(service))
    service.api = Mock(return_value=video_response())
    service.process_candidates({})
    service.add_task.assert_called_once()


def test_manual_monitor_keeps_legacy_history_path(service):
    service.config['auto_add_to_tasks'] = False
    assert service.configs() == []
    assert service.active_channels() == set()


def test_persistent_dispatch_limit_and_next_window(service):
    service.config.update(max_results=1, rate_limit_requests=1)
    assert service.reserve_dispatch(service.config, {})
    restarted = YouTubeDiscovery(service.monitor, service.load_config, service.add_task,
                                probe=service.probe, clock=service.clock)
    assert not restarted.reserve_dispatch(service.config, {})
    service.now[0] += 120
    assert restarted.reserve_dispatch(service.config, {})


def test_static_filter_exclusion_does_not_poll_forever(service):
    service.receive(*payload(service))
    service.api = Mock(return_value=video_response())
    service.monitor._filter_videos = Mock(return_value=[])
    service.process_candidates({})
    assert service.status()['candidates'][0]['state'] == 'ignored'
    service.now[0] += 1000
    service.process_candidates({})
    assert service.api.call_count == 1
    service.add_task.assert_not_called()


def test_real_filter_and_history_integration(service):
    from modules.youtube_monitor import YouTubeMonitor, MONITOR_CONFIG_FIELD_DEFAULTS
    real = YouTubeMonitor.__new__(YouTubeMonitor)
    service.monitor._filter_videos = real._filter_videos
    service.config.update({key: value for key, value in MONITOR_CONFIG_FIELD_DEFAULTS.items()
                           if key not in service.config})
    response = video_response()
    response['items'][0].update(contentDetails={'duration': 'PT5M'}, statistics={'viewCount': '100'})
    response['items'][0]['snippet'].update(title='Official upload', channelTitle='Official',
        description='Description', publishedAt='2023-11-14T22:13:21Z')
    service.receive(*payload(service))
    service.api = Mock(return_value=response)
    service.process_candidates({})
    assert service.status()['candidates'][0]['state'] == 'ready'
    recorded = service.monitor.record_discovered_video.call_args.args[0]
    assert recorded['title'] == 'Official upload' and recorded['duration'] == 'PT5M'
