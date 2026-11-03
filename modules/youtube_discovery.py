"""Durable WebSub inbox, uploads-playlist reconciliation and availability retries."""
import hashlib
import hmac
import json
import logging
import re
import secrets
import sqlite3
import subprocess
import threading
import time
from datetime import datetime, timezone
from urllib.parse import urlsplit
from xml.etree import ElementTree as ET

import requests

LOG = logging.getLogger(__name__)
HUB = 'https://pubsubhubbub.appspot.com/subscribe'
TOPIC = 'https://www.youtube.com/xml/feeds/videos.xml?channel_id='
ID = re.compile(r'^[A-Za-z0-9_-]{11}$')
CHANNEL = re.compile(r'^UC[A-Za-z0-9_-]{22}$')


def timestamp(value):
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    return (parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)).timestamp()


def probe_video(video_id, config):
    """Short probe; scheduled/live videos must become complete VODs first."""
    from .youtube_handler import _find_yt_dlp_command, _append_yt_dlp_network_args, build_proxy_url
    from .task_manager import resolve_youtube_cookies_path
    cmd = _find_yt_dlp_command(LOG)
    if not cmd:
        raise RuntimeError('yt-dlp 不可用')
    cmd += ['--ignore-config', '--dump-single-json', '--skip-download', '--no-playlist',
            '--socket-timeout', '15', '--retries', '0', '--extractor-retries', '0']
    _append_yt_dlp_network_args(cmd, proxy_url=build_proxy_url(config),
                              cookies_path=resolve_youtube_cookies_path(config))
    cmd.append('https://www.youtube.com/watch?v=' + video_id)
    result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8',
                            errors='replace', timeout=45)
    if result.returncode:
        # Do not persist command output: it may contain signed URLs or cookie details.
        raise RuntimeError('yt-dlp 暂未取得可下载格式，请检查下载日志或登录状态')
    info = json.loads(result.stdout)
    if info.get('is_live') or info.get('live_status') in ('is_upcoming', 'is_live', 'post_live'):
        return False
    return any(f.get('url') and f.get('vcodec') not in (None, 'none')
               for f in info.get('formats', []))


class YouTubeDiscovery:
    def __init__(self, monitor, config_loader, task_adder, probe=probe_video, clock=time.time):
        self.monitor, self.load_config, self.add_task = monitor, config_loader, task_adder
        self.probe, self.clock = probe, clock
        self.lock = threading.Lock()
        with self.connect() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS discovery_channels (
                    channel_id TEXT PRIMARY KEY, baseline REAL NOT NULL,
                    secret TEXT NOT NULL, token TEXT NOT NULL, expires REAL DEFAULT 0,
                    renew_after REAL DEFAULT 0, playlist_id TEXT, last_poll REAL DEFAULT 0,
                    error TEXT, poll_retry REAL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS discovery_candidates (
                    config_id INTEGER NOT NULL, video_id TEXT NOT NULL, channel_id TEXT NOT NULL,
                    state TEXT NOT NULL DEFAULT 'waiting', attempts INTEGER DEFAULT 0,
                    next_check REAL NOT NULL, first_seen REAL NOT NULL, task_id TEXT,
                    error TEXT, PRIMARY KEY(config_id, video_id));
                CREATE TABLE IF NOT EXISTS discovery_dispatch_budget (
                    config_id INTEGER PRIMARY KEY, window INTEGER NOT NULL, used INTEGER NOT NULL);
            ''')
            if 'poll_retry' not in {r[1] for r in db.execute('PRAGMA table_info(discovery_channels)')}:
                db.execute('ALTER TABLE discovery_channels ADD COLUMN poll_retry REAL DEFAULT 0')

    def connect(self):
        db = sqlite3.connect(self.monitor.db_path, timeout=30)
        db.row_factory = sqlite3.Row
        return db

    def configs(self):
        return [c for c in self.monitor.get_monitor_configs()
                if c.get('enabled') and c.get('schedule_type') == 'auto'
                and c.get('channel_mode') == 'latest' and c.get('channel_ids')
                and c.get('auto_add_to_tasks')]

    @staticmethod
    def channels(config):
        return [s for s in re.split(r'[\s,;，；]+', config.get('channel_ids', '')) if CHANNEL.fullmatch(s)]

    def active_channels(self):
        return {s for c in self.configs() for s in self.channels(c)}

    def enroll(self, channel):
        with self.connect() as db:
            db.execute('INSERT OR IGNORE INTO discovery_channels(channel_id, baseline, secret, token) VALUES (?,?,?,?)',
                       (channel, self.clock(), secrets.token_hex(32), secrets.token_urlsafe(24)))

    def receive(self, body, signature):
        if len(body) > 1024 * 1024 or b'<!DOCTYPE' in body.upper() or b'<!ENTITY' in body.upper():
            raise ValueError('不支持的通知内容')
        try:
            root = ET.fromstring(body)
        except ET.ParseError as exc:
            raise ValueError('通知 XML 无效') from exc
        entries = root.findall('{http://www.w3.org/2005/Atom}entry')
        accepted = 0
        configs = self.configs()
        active = {ch for c in configs for ch in self.channels(c)}
        for entry in entries:
            channel = entry.findtext('{http://www.youtube.com/xml/schemas/2015}channelId', '')
            video = entry.findtext('{http://www.youtube.com/xml/schemas/2015}videoId', '')
            if channel not in active or not ID.fullmatch(video):
                continue
            with self.connect() as db:
                row = db.execute('SELECT * FROM discovery_channels WHERE channel_id=?', (channel,)).fetchone()
            if not row or row['expires'] <= self.clock():
                raise ValueError('订阅未确认或已过期')
            algorithm, _, digest = (signature or '').partition('=')
            if algorithm not in ('sha1', 'sha256') or not hmac.compare_digest(
                    hmac.new(row['secret'].encode(), body, getattr(hashlib, algorithm)).hexdigest(), digest):
                raise ValueError('通知签名无效')
            published = entry.findtext('{http://www.w3.org/2005/Atom}published', '')
            # Old title/description changes must not create new tasks.
            if not published or timestamp(published) < row['baseline']:
                continue
            for config in configs:
                if channel in self.channels(config):
                    self.enqueue(config['id'], video, channel)
                    accepted += 1
        return accepted

    def verify(self, args):
        topic = args.get('hub.topic', '')
        channel = topic[len(TOPIC):] if topic.startswith(TOPIC) else ''
        if channel not in self.active_channels() or args.get('hub.mode') != 'subscribe':
            raise ValueError('未知订阅')
        lease = int(args.get('hub.lease_seconds', '0'))
        challenge = args.get('hub.challenge', '')
        with self.connect() as db:
            row = db.execute('SELECT token FROM discovery_channels WHERE channel_id=?', (channel,)).fetchone()
            if not row or not hmac.compare_digest(row['token'], args.get('hub.verify_token', '')) or not challenge or not 0 < lease <= 31536000:
                raise ValueError('订阅验证失败')
            db.execute('UPDATE discovery_channels SET expires=?, renew_after=?, error=NULL WHERE channel_id=?',
                       (self.clock() + lease, self.clock() + lease * .7, channel))
        return challenge

    def enqueue(self, config_id, video, channel):
        now = self.clock()
        with self.connect() as db:
            db.execute('INSERT OR IGNORE INTO discovery_candidates(config_id,video_id,channel_id,next_check,first_seen) VALUES (?,?,?,?,?)',
                       (config_id, video, channel, now, now))

    def renew(self, channel, config):
        callback = config.get('YOUTUBE_WEBSUB_CALLBACK_URL', '').strip()
        parsed = urlsplit(callback)
        if parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.query or parsed.fragment:
            raise ValueError('WebSub 回调必须是无查询参数的公网 HTTPS 地址')
        with self.connect() as db:
            row = db.execute('SELECT * FROM discovery_channels WHERE channel_id=?', (channel,)).fetchone()
            if row['renew_after'] > self.clock():
                return
            # Prevent hot retries while allowing asynchronous hub confirmation.
            db.execute('UPDATE discovery_channels SET renew_after=? WHERE channel_id=?', (self.clock() + 300, channel))
        response = requests.post(HUB, data={'hub.mode': 'subscribe', 'hub.topic': TOPIC + channel,
            'hub.callback': callback, 'hub.verify': 'async', 'hub.verify_token': row['token'],
            'hub.secret': row['secret'], 'hub.lease_seconds': '432000'}, timeout=15,
            allow_redirects=False)
        if response.status_code not in (202, 204):
            raise RuntimeError('WebSub hub 拒绝订阅')

    def api(self, request):
        # Existing polling and this worker share httplib2: serialize API access.
        with self.monitor._discovery_api_lock:
            return self.monitor._execute_with_retry(request, 'WebSub/Data API 补查')

    def reconcile(self, channel, config):
        with self.connect() as db:
            row = db.execute('SELECT * FROM discovery_channels WHERE channel_id=?', (channel,)).fetchone()
        interval = max(30, int(config.get('YOUTUBE_RECONCILE_SECONDS', 120)))
        if row['last_poll'] + interval > self.clock() or row['poll_retry'] > self.clock():
            return
        playlist = row['playlist_id']
        if not playlist:
            response = self.api(self.monitor.youtube.channels().list(part='contentDetails', id=channel))
            playlist = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        # Overlap since last successful poll; on restart include downtime. Walk pages
        # to the boundary, never advance last_poll after a partial/failed scan.
        boundary = max(row['baseline'], row['last_poll'] - 86400)
        configs = [c for c in self.configs() if channel in self.channels(c)]
        token = None
        poll_started = self.clock()
        for _ in range(100):
            response = self.api(self.monitor.youtube.playlistItems().list(
                part='snippet,contentDetails', playlistId=playlist, maxResults=50, pageToken=token))
            times = []
            for item in response.get('items', []):
                snippet = item['snippet']
                published = item.get('contentDetails', {}).get('videoPublishedAt') or snippet.get('publishedAt')
                if not published:
                    continue
                at = timestamp(published)
                times.append(at)
                video = snippet.get('resourceId', {}).get('videoId', '')
                if at >= boundary and ID.fullmatch(video):
                    for c in configs:
                        self.enqueue(c['id'], video, channel)
            token = response.get('nextPageToken')
            if not token or (times and max(times) < boundary):
                break
        else:
            raise RuntimeError('补查超过分页上限，未推进游标')
        with self.connect() as db:
            db.execute('UPDATE discovery_channels SET playlist_id=?,last_poll=?,error=NULL,poll_retry=0 WHERE channel_id=?',
                       (playlist, poll_started, channel))

    def process_candidates(self, runtime_config):
        now = self.clock()
        configs = {c['id']: c for c in self.configs()}
        allowed = [(c['id'], ch) for c in configs.values() for ch in self.channels(c)]
        if not allowed:
            return
        scope = ' OR '.join('(config_id=? AND channel_id=?)' for _ in allowed)
        params = [now] + [value for pair in allowed for value in pair]
        with self.connect() as db:
            rows = db.execute("SELECT * FROM discovery_candidates WHERE state='waiting' AND next_check<=? AND (" + scope + ') ORDER BY next_check LIMIT 10', params).fetchall()
        for row in rows:
            c = configs.get(row['config_id'])
            if not c or row['channel_id'] not in self.channels(c):
                continue
            try:
                response = self.api(self.monitor.youtube.videos().list(
                    part='snippet,statistics,contentDetails,liveStreamingDetails,status', id=row['video_id']))
                items = response.get('items', [])
                if not items or items[0].get('status', {}).get('privacyStatus') != 'public':
                    raise RuntimeError('等待视频公开或 API 可见')
                video = items[0]
                if video['snippet']['channelId'] != row['channel_id']:
                    raise RuntimeError('频道信息不一致')
                static_config = dict(c, min_view_count=0, min_like_count=0, min_comment_count=0)
                if not self.monitor._filter_videos(items, static_config):
                    with self.connect() as db:
                        db.execute("UPDATE discovery_candidates SET state='ignored',error=? WHERE config_id=? AND video_id=?",
                                   ('不符合标题、类型、频道或时长筛选；调整配置后可手动重新检查', c['id'], row['video_id']))
                    continue
                filtered = self.monitor._filter_videos(items, c)
                if not filtered:
                    raise RuntimeError('等待满足监控筛选条件')
                if video['snippet'].get('liveBroadcastContent') in ('upcoming', 'live') or not self.probe(row['video_id'], runtime_config):
                    raise RuntimeError('等待完整视频可下载')
                task_id = None
                if self.reserve_dispatch(c, runtime_config):
                    task_id = self.add_task('https://www.youtube.com/watch?v=' + row['video_id'],
                                            source_key='youtube:' + row['video_id'])
                    if not task_id:
                        with self.connect() as db:
                            db.execute('UPDATE discovery_dispatch_budget SET used=MAX(0,used-1) WHERE config_id=?', (c['id'],))
                        raise RuntimeError('任务入队失败，将自动重试')
                else:
                    raise RuntimeError('等待本轮入队名额')
                self.monitor.record_discovered_video(filtered[0], c['id'])
                with self.connect() as db:
                    db.execute("UPDATE discovery_candidates SET state='ready',task_id=?,error=NULL WHERE config_id=? AND video_id=?",
                               (task_id, c['id'], row['video_id']))
            except Exception as exc:
                attempts = row['attempts'] + 1
                delay = min(300, 5 * 2 ** min(attempts - 1, 6))
                state = 'attention' if now - row['first_seen'] > 7 * 86400 else 'waiting'
                with self.connect() as db:
                    db.execute('UPDATE discovery_candidates SET attempts=?,next_check=?,state=?,error=? WHERE config_id=? AND video_id=?',
                               (attempts, self.clock() + delay, state,
                                str(exc)[:200] if type(exc) is RuntimeError else '服务请求失败，稍后重试',
                                c['id'], row['video_id']))

    def reserve_dispatch(self, config, runtime_config):
        period = max(30, int(runtime_config.get('YOUTUBE_RECONCILE_SECONDS', 120)))
        window = int(self.clock() // period)
        limit = max(0, min(int(config.get('max_results', 10)), int(config.get('rate_limit_requests', 100))))
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('INSERT OR IGNORE INTO discovery_dispatch_budget VALUES (?,?,0)', (config['id'], window))
            db.execute('UPDATE discovery_dispatch_budget SET window=?,used=0 WHERE config_id=? AND window!=?',
                       (window, config['id'], window))
            cursor = db.execute('UPDATE discovery_dispatch_budget SET used=used+1 WHERE config_id=? AND used<?',
                                (config['id'], limit))
            return cursor.rowcount == 1

    def tick(self):
        config = self.load_config()
        if not config.get('YOUTUBE_WEBSUB_ENABLED') or not self.monitor.youtube or not self.lock.acquire(False):
            return
        try:
            for channel in self.active_channels():
                self.enroll(channel)
                for name, operation in (('reconcile', self.reconcile), ('renew', self.renew)):
                    try:
                        operation(channel, config)
                    except Exception:
                        LOG.warning('YouTube %s failed for %s; retry scheduled', name, channel)
                        with self.connect() as db:
                            field = 'poll_retry' if name == 'reconcile' else 'renew_after'
                            db.execute(f'UPDATE discovery_channels SET error=?,{field}=? WHERE channel_id=?',
                                       (name + ' 失败，已安排重试；请检查网络和配置', self.clock() + 300, channel))
            self.process_candidates(config)
        finally:
            self.lock.release()

    def status(self):
        with self.connect() as db:
            channels = [dict(r) for r in db.execute('SELECT channel_id,expires,last_poll,error FROM discovery_channels')]
            candidates = [dict(r) for r in db.execute('SELECT config_id,video_id,state,attempts,next_check,task_id,error FROM discovery_candidates ORDER BY first_seen DESC LIMIT 100')]
        for channel in channels:
            for key in ('expires', 'last_poll'):
                channel[key + '_display'] = (datetime.fromtimestamp(channel[key], timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
                                            if channel[key] else '尚未确认' if key == 'expires' else '尚未补查')
        return {'channels': channels, 'candidates': candidates}

    def retry(self, config_id, video_id):
        if config_id not in {c['id'] for c in self.configs()}:
            raise ValueError('监控配置已停用')
        with self.connect() as db:
            db.execute("UPDATE discovery_candidates SET state='waiting',next_check=?,first_seen=?,attempts=0,error=NULL WHERE config_id=? AND video_id=? AND state!='ready'",
                       (self.clock(), self.clock(), config_id, video_id))
