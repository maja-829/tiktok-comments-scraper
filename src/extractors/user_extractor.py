from typing import Any, Dict, List

def normalize_avatar_urls(user: Dict[str, Any]) -> List[str]:
    """
    Extract a list of avatar URLs from common TikTok user shapes.
    """
    if not isinstance(user, dict):
        return []
    # Typical shapes:
    # user.avatar_thumb.url_list -> [ ... ]
    # user.avatarThumb -> { urlList: [...] } (camelCase)
    # user.avatar -> string
    urls = []

    # snake case path
    try:
        urls = user.get("avatar_thumb", {}).get("url_list") or []
    except Exception:
        pass

    # camel case path
    if not urls:
        try:
            urls = user.get("avatarThumb", {}).get("urlList") or []
        except Exception:
            pass

    # direct string
    if not urls:
        single = user.get("avatar") or user.get("avatarUrl")
        if isinstance(single, str) and single.strip():
            urls = [single.strip()]

    # ensure strings only
    return [u for u in urls if isinstance(u, str) and u.strip()]

def build_user_block(user: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map various user shapes into the target structure:
    {
      "nickname": str,
      "unique_id": str,
      "avatar_thumb": { "url_list": [str, ...] }
    }
    """
    nickname = (
        user.get("nickname")
        or user.get("nicknameName")
        or user.get("display_name")
        or user.get("displayName")
        or ""
    )
    unique_id = (
        user.get("unique_id")
        or user.get("uniqueId")
        or user.get("username")
        or user.get("uid")
        or ""
    )
    urls = normalize_avatar_urls(user)

    return {
        "nickname": nickname,
        "unique_id": unique_id,
        "avatar_thumb": {"url_list": urls},
    }