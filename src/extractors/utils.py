from __future__ import annotations

import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, Iterable

def setup_logger() -> logging.Logger:
    logger = logging.getLogger("tiktok-comments-scraper")
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        ch.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
        logger.addHandler(ch)
    return logger

def load_settings(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        # Provide sensible defaults if settings.json is missing
        return {
            "user_agent": default_user_agent(),
            "timeout_seconds": 15,
            "proxy": os.environ.get("HTTP_PROXY") or os.environ.get("HTTPS_PROXY"),
            "max_items": 100,
        }
    with p.open("r", encoding="utf-8") as f:
        cfg = json.load(f)
    # fill defaults
    cfg.setdefault("user_agent", default_user_agent())
    cfg.setdefault("timeout_seconds", 15)
    cfg.setdefault("max_items", 100)
    return cfg

def default_user_agent() -> str:
    return (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )

def build_headers() -> Dict[str, str]:
    return {
        "User-Agent": default_user_agent(),
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }

def parse_aweme_id_from_url(url: str) -> str:
    """
    Extract aweme_id from common TikTok URL patterns.
    Examples:
      https://www.tiktok.com/@user/video/7171782248281165058