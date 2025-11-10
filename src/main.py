import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from extractors.comments_parser import CommentsClient
from outputs.exporter import Exporter
from outputs.formatter import format_comment_record
from extractors.utils import (
    load_settings,
    setup_logger,
    iter_unique,
    is_comment_like,
)

LOGGER = setup_logger()

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Tiktok Comments Scraper — extract structured comments from TikTok video URLs or a local sample file."
    )
    p.add_argument(
        "--url",
        "-u",
        action="append",
        help="TikTok video URL (can be provided multiple times).",
    )
    p.add_argument(
        "--input-file",
        "-i",
        help="Path to JSON file containing { startUrls: [...], sampleComments?: {...} }",
        default=str(Path(__file__).resolve().parents[1] / "data" / "sample_input.json"),
    )
    p.add_argument(
        "--settings",
        "-s",
        help="Path to settings.json",
        default=str(Path(__file__).resolve().parents[0] / "config" / "settings.json"),
    )
    p.add_argument(
        "--out",
        "-o",
        help="Output file path (json, jsonl, or csv). Defaults to data/output_example.json",
        default=str(Path(__file__).resolve().parents[1] / "data" / "output_example.json"),
    )
    p.add_argument(
        "--max-items",
        type=int,
        help="Maximum number of comments per video (overrides settings.json).",
    )
    p.add_argument(
        "--format",
        choices=["json", "jsonl", "csv"],
        help="Force output format (otherwise inferred from file extension).",
    )
    return p.parse_args()

def load_input(input_path: str) -> Dict[str, Any]:
    p = Path(input_path)
    if not p.exists():
        raise FileNotFoundError(f"Input file not found: {p}")
    with p.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("Input JSON must be an object with keys like 'startUrls' and optional 'sampleComments'.")
    return data

def infer_format(out_path: str, forced: Optional[str]) -> str:
    if forced:
        return forced
    suffix = Path(out_path).suffix.lower()
    if suffix == ".jsonl":
        return "jsonl"
    if suffix == ".csv":
        return "csv"
    return "json"

def main() -> None:
    args = parse_args()
    settings = load_settings(args.settings)

    start_urls: List[str] = []
    sample_comments_by_url: Dict[str, List[Dict[str, Any]]] = {}

    if args.url:
        start_urls.extend(args.url)

    input_data = {}
    if args.input_file:
        try:
            input_data = load_input(args.input_file)
        except Exception as e:
            LOGGER.warning(f"Failed to load input file '{args.input_file}': {e}")

    if not start_urls:
        start_urls = input_data.get("startUrls", []) if isinstance(input_data, dict) else []

    # Optional: local sample comments (for deterministic, offline runs)
    sample_comments_by_url = input_data.get("sampleComments", {}) if isinstance(input_data, dict) else {}

    start_urls = list(iter_unique(url for url in (start_urls or []) if isinstance(url, str) and url.strip()))

    if not start_urls:
        LOGGER.error("No video URLs provided. Use --url or include 'startUrls' in the input JSON.")
        raise SystemExit(1)

    max_items = args.max_items if args.max_items is not None else int(settings.get("max_items", 100))
    exporter = Exporter(
        output_path=args.out,
        output_format=infer_format(args.out, args.format),
        field_order=[
            "author_pin",
            "aweme_id",
            "cid",
            "comment_language",
            "create_time",
            "digg_count",
            "reply_comment_total",
            "text",
            "user.nickname",
            "user.unique_id",
            "user.avatar_thumb.url_list",
            "share_info.url",
        ],
    )

    client = CommentsClient(settings=settings)

    total_written = 0
    for url in start_urls:
        LOGGER.info(f"Processing video: {url}")
        try:
            # Prefer live fetch; if it fails or not available, fall back to sample payload if provided.
            comments = client.fetch_comments(url=url, max_items=max_items)
            if not comments and url in sample_comments_by_url:
                LOGGER.info("Falling back to local sample comments for this URL.")
                comments = sample_comments_by_url[url]

            # Validate basic shape & format to avoid writing garbage
            valid_records = [format_comment_record(c, url) for c in comments if is_comment_like(c)]
            exporter.write_many(valid_records)
            total_written += len(valid_records)
            LOGGER.info(f"Written {len(valid_records)} comments for {url}.")
        except Exception as e:
            LOGGER.exception(f"Failed to process {url}: {e}")

    exporter.close()
    LOGGER.info(f"All done. Total comments written: {total_written} -> {args.out}")

if __name__ == "__main__":
    main()