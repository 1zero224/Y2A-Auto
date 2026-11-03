import yt_dlp

from modules.youtube_handler import (
    _QUALITY_FORMAT_SORT, _build_quality_retry_strategies, _set_yt_dlp_format_options,
)


def select(height=None):
    formats = []
    for fid, res, fps, codec, rate in [
        ('av1-small',1080,30,'av01',2000), ('h264-high',1080,30,'avc1',3600),
        ('720-large',720,30,'avc1',8000), ('4k',2160,30,'av01',2500),
    ]:
        formats.append(dict(format_id=fid,url='https://example.test/'+fid,ext='mp4',
                            height=res,width=res*16//9,fps=fps,vcodec=codec,acodec='none',tbr=rate))
    for fid, codec, rate in [('opus','opus',128),('aac','mp4a',256)]:
        formats.append(dict(format_id=fid,url='https://example.test/'+fid,ext='m4a',
                            vcodec='none',acodec=codec,abr=rate,tbr=rate,asr=48000,audio_channels=2))
    selector='bestvideo+bestaudio' if height is None else f'bestvideo[height<={height}]+bestaudio'
    with yt_dlp.YoutubeDL({'quiet':True,'skip_download':True,'format':selector,
                          'format_sort':_QUALITY_FORMAT_SORT.split(','),
                          'format_sort_force':True}) as ydl:
        info=ydl.process_ie_result({'id':'quality-test','title':'Test','formats':formats},download=False)
    return [f['format_id'] for f in info['requested_formats']]


def test_resolution_before_bitrate_and_audio_bitrate_before_codec():
    assert select()==['4k','aac']


def test_same_resolution_prefers_high_bitrate_over_av1_and_preserves_cap():
    assert select(1080)==['h264-high','aac']


def test_highest_retry_never_switches_to_combined_low_quality_stream():
    plan=_build_quality_retry_strategies({'YOUTUBE_DOWNLOAD_QUALITY_MODE':'highest'},True)
    assert len(plan['strategies'])==1
    assert plan['strategies'][0]['selector']=='bestvideo+bestaudio'


def test_format_retry_keeps_quality_sort_without_duplicate_arguments():
    cmd=['yt-dlp']
    _set_yt_dlp_format_options(cmd,'bestvideo+bestaudio',merge_output_format='mp4')
    _set_yt_dlp_format_options(cmd,'best[height<=720]')
    assert cmd.count('--format-sort')==cmd.count('--format-sort-force')==1
    assert cmd[cmd.index('--format-sort')+1]==_QUALITY_FORMAT_SORT
