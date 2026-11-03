"""Local Bilibili drafts. Preparing a draft never calls a publishing API."""
import json


EDITABLE_FIELDS = (
    'video_title_translated', 'description_translated', 'tags_generated',
    'selected_partition_id_acfun', 'selected_partition_id_bilibili',
)


def requires_manual_publish(task, config):
    return (task or {}).get('upload_target') in ('bilibili', 'both') and bool(
        config.get('BILIBILI_LOCAL_DRAFT_ENABLED', True)
    )


def apply_draft(task):
    """Keep human edits separate from regeneratable AI outputs."""
    if not task:
        return task
    result = dict(task)
    try:
        edits = json.loads(result.get('draft_metadata_json') or '{}')
    except (TypeError, ValueError):
        edits = {}
    if isinstance(edits, dict):
        result.update({key: edits[key] for key in EDITABLE_FIELDS if key in edits})
    return result


def serialize_edits(values):
    return json.dumps({key: values[key] for key in EDITABLE_FIELDS if key in values},
                      ensure_ascii=False)
