from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional

import httpx
from tenacity import retry, stop_after_attempt, wait_fixed

from .utils import setup_logger, parse_aweme_id_from_url, build_headers

LOGGER = setup_logger()

class CommentsClient:
    """
    Retrieves TikTok comments. Uses best-effort unauthenticated requests and
    gracefully degrades to empty results if remote access is restricted.

    For deterministic local runs, provide 'sampleComments' in input JSON.
    """

    def __init__(self, settings: Dict[str, Any]) -> None:
        self.settings = settings or {}
        self.timeout = int(self.settings.get("timeout_seconds", 15))
        self.proxy = self.settings.get("proxy")
        self.ua = self.settings.get("user_agent") or build_headers()["User-Agent"]

    def fetch_comments(self, url: str, max_items: int) -> List[Dict[str, Any]]:
        """
        Try multiple lightweight strategies to get comments.
        If network or endpoint constraints prevent access, returns [].
        """
        aweme_id = parse_aweme_id_from_url(url)
        if not aweme_id:
            LOGGER.warning("Could not parse aweme_id from URL; live comment retrieval may fail.")
        # Strategy 1: Try the mobile share page (may embed initial comments in JSON)
        try:
            records = self._try_mobile_page(url, max_items=max_items)
            if records:
                return records[:max_items]
        except Exception as e:
            LOGGER.debug(f"Mobile page strategy failed: {e}")

        # Strategy 2: Try oEmbed for basic info (no comments, but confirm URL is resolvable)
        try:
            if self._probe_oembed(url):
                LOGGER.info("oEmbed probe succeeded, but no comments available from this endpoint.")
        except Exception as e:
            LOGGER.debug(f"oEmbed probe failed: {e}")

        # Strategy 3: Attempt lightweight API (often requires cookies/params; usually blocked)
        try:
            if aweme_id:
                records = self._try_public_api_like(aweme_id, max_items=max_items)
                if records:
                    return records[:max_items]
        except Exception as e:
            LOGGER.debug(f"Public-like API strategy failed: {e}")

        # Degrade gracefully
        LOGGER.info("No live comments could be retrieved without credentials. Returning empty list.")
        return []

    @retry(stop=stop_after_attempt(2), wait=wait_fixed(1))
    def _try_mobile_page(self, url: str, max_items: int) -> List[Dict[str, Any]]:
        """
        Fetch the mobile share page; in some locales TikTok ships a JSON blob ('SIGI_STATE')
        containing partial comments or related data. We parse what's safely available.
        """
        headers = {"User-Agent": self.ua, "Accept": "text/html,application/xhtml+xml,application/xml"}
        proxies = {"http://": self.proxy, "https://": self.proxy} if self.proxy else None

        # Normalize to "m.tiktok.com" variant for better chance of a lighter payload
        m_url = re.sub(r"^(https?://)(www\.)?tiktok\.com/", r"\1m.tiktok.com/", url, flags=re.IGNORECASE)
        with httpx.Client(follow_redirects=True, timeout=self.timeout, proxies=proxies, headers=headers) as client:
            resp = client.get(m_url)
            resp.raise_for_status()
            html = resp.text

        # Extract JSON embedded in <script id="SIGI_STATE">...</script> if present
        m = re.search(r'<script[^>]+id="SIGI_STATE"[^>]*>(.*?)</script>', html, flags=re.DOTALL | re.IGNORECASE)
        if not m:
            return []

        try:
            data = json.loads(m.group(1))
        except Exception:
            return []

        comments = self._extract_comments_from_sigi(data)
        return comments[:max_items]

    @staticmethod
    def _extract_comments_from_sigi(data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Best-effort extraction of comments from SIGI_STATE structure.
        Real-world TikTok pages vary; we look for a plausible path.
        """
        comments: List[Dict[str, Any]] = []

        # TikTok often nests entities under 'ItemModule' or similar; comments may appear differently per locale.
        # We try a couple of common shapes safely (no KeyError).
        comment_dicts = []
        try:
            # Hypothetical path if comments are included (rare, but possible in some snapshots)
            comment_dicts = (
                data.get("CommentItem", {}).get("comments", [])
                or data.get("Comments", {}).get("comments", [])
                or []
            )
        except Exception:
            comment_dicts = []

        if isinstance(comment_dicts, dict):
            comment_dicts = list(comment_dicts.values())

        for c in comment_dicts or []:
            if not isinstance(c, dict):
                continue
            comments.append(c)

        return comments

    @retry(stop=stop_after_attempt(2), wait=wait_fixed(1))
    def _probe_oembed(self, url: str) -> bool:
        """
        Use the public oEmbed endpoint to check URL validity (no comments returned).
        """
        headers = {"User-Agent": self.ua}
        proxies = {"http://": self.proxy, "https://": self.proxy} if self.proxy else None
        oembed = "https://www.tiktok.com/oembed?url=" + httpx.utils.quote(url, safe="")
        with httpx.Client(follow_redirects=True, timeout=self.timeout, proxies=proxies, headers=headers) as client:
            r = client.get(oembed)
            if r.status_code == 404:
                return False
            r.raise_for_status()
            return True

    @retry(stop=stop_after_attempt(2), wait=wait_fixed(1))
    def _try_public_api_like(self, aweme_id: str, max_items: int) -> List[Dict[str, Any]]:
        """
        Placeholder for a lightweight, sometimes-working public-ish endpoint.
        Most production scrapers use authenticated flows / cookies and signed params,
        which we intentionally avoid here. We keep this to a safe 'attempt'.
        """
        # This endpoint is illustrative; typically blocked without proper params.
        # We return [] gracefully if access is denied.
        headers = {"User-Agent": self.ua, "Accept": "application/json"}
        proxies = {"http://": self.proxy, "https://": self.proxy} if self.proxy else None
        api_url = f"https://www.tiktok.com/api/comment/list/?aweme_id={aweme_id}&count={max_items}"
        try:
            with httpx.Client(follow_redirects=True, timeout=self.timeout, proxies=proxies, headers=headers) as client:
                r = client.get(api_url)
                if r.status_code != 200:
                    return []
                data = r.json()
        except Exception:
            return []

        # Normalize common shapes: { comments: [ ... ] } or { data: { comments: [...] } }
        comments = []
        if isinstance(data, dict):
            if isinstance(data.get("comments"), list):
                comments = data["comments"]
            elif isinstance(data.get("data", {}).get("comments"), list):
                comments = data["data"]["comments"]
        return comments or []