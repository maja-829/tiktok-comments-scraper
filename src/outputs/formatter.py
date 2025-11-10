from __future__ import annotations

from typing import Any, Dict

from ..extractors.user_extractor import build_user_block

def format_comment_record(raw: Dict[str, Any], source_url: str) -> Dict[str, Any]:
    """
    Normalize a raw TikTok comment into the documented target schema.
    Missing fields are filled with sensible defaults.
    """
    # Various possible keys for each attribute
    author_pin = bool(raw.get("author_pin") or raw.get("isPinned") or raw.get("pinned"))
    aweme_id = (
        raw.get("aweme_id")
        or raw.get("awemeId")
        or raw.get("video_id")
        or raw.get("videoId")
        or ""
    )
    cid = raw.get("cid") or raw.get("comment_id") or raw.get("id") or ""
    lang = (
        raw.get("comment_language")
        or raw.get("lang")
        or raw.get("language")
        or ""
    )
    create_time = (
        raw.get("create_time")
        or raw.get("createTime")
        or raw.get("timestamp")
        or 0
    )
    digg = (
        raw.get("digg_count")
        or raw.get("like_count")
        or raw.get("likes")
        or 0
    )
    replies = (
        raw.get("reply_comment_total")
        or raw.get("reply_count")
        or raw.get("replies")
        or 0
    )
    text = raw.get("text") or raw.get("comment") or raw.get("content") or ""

    user_block = build_user_block(raw.get("user") or raw.get("author") or {})

    share_url = ""
    share_info = raw.get("share_info") or {}
    if isinstance(share_info, dict):
        share_url = share_info.get("url") or ""

    record = {
        "author_pin": bool(author_pin),
        "aweme_id": str(aweme_id) if aweme_id else "",
        "cid": str(cid) if cid else "",
        "comment_language": str(lang) if lang else "",
        "create_time": int(create_time) if isinstance(create_time, (int, float, str)) and str(create_time).isdigit() else 0,
        "digg_count": int(digg) if isinstance(digg, (int, float, str)) and str(digg).isdigit() else 0,
        "reply_comment_total": int(replies) if isinstance(replies, (int, float, str)) and str(replies).isdigit() else 0,
        "text": text,
        "user": user_block,
        "share_info": {"url": share_url or build_share_url_fallback(aweme_id, cid, source_url)},
    }
    return record

def build_share_url_fallback(aweme_id: str, cid: str, source_url: str) -> str:
    if aweme_id and cid:
        return f"https://m.tiktok.com/v/{aweme_id}.html?share_comment_id={cid}"
    return source_url or ""