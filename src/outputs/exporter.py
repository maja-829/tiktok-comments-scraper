from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Dict, Iterable, List, Optional

class Exporter:
    """
    Write comment records into JSON, JSONL, or CSV.
    For CSV, nested properties are flattened via dot-paths present in `field_order`.
    """

    def __init__(self, output_path: str, output_format: str, field_order: Optional[List[str]] = None) -> None:
        self.output_path = Path(output_path)
        self.format = output_format.lower()
        self.field_order = field_order or []
        self._fp = None
        self._writer = None

        self.output_path.parent.mkdir(parents=True, exist_ok=True)

        if self.format == "json":
            self._buffer: List[Dict] = []
        elif self.format == "jsonl":
            self._fp = self.output_path.open("w", encoding="utf-8")
        elif self.format == "csv":
            self._fp = self.output_path.open("w", encoding="utf-8", newline="")
            self._writer = csv.DictWriter(self._fp, fieldnames=self.field_order or [])
            self._writer.writeheader()
        else:
            raise ValueError(f"Unsupported export format: {self.format}")

    def write_many(self, rows: Iterable[Dict]) -> None:
        for row in rows:
            self.write_one(row)

    def write_one(self, row: Dict) -> None:
        if self.format == "json":
            self._buffer.append(row)
        elif self.format == "jsonl":
            assert self._fp is not None
            self._fp.write(json.dumps(row, ensure_ascii=False) + "\n")
        elif self.format == "csv":
            assert self._writer is not None
            flat = flatten_for_csv(row, self.field_order)
            self._writer.writerow(flat)

    def close(self) -> None:
        if self.format == "json":
            self.output_path.write_text(json.dumps(self._buffer, ensure_ascii=False, indent=2), encoding="utf-8")
        if self._fp:
            self._fp.close()

def flatten_for_csv(record: Dict, field_order: List[str]) -> Dict[str, str]:
    """
    Flatten nested dicts using a whitelist of dot-paths so CSV columns are consistent.
    """
    out: Dict[str, str] = {}
    for path in field_order:
        value = walk_path(record, path)
        if isinstance(value, (list, dict)):
            out[path] = json.dumps(value, ensure_ascii=False)
        elif value is None:
            out[path] = ""
        else:
            out[path] = str(value)
    return out

def walk_path(obj: Dict, path: str):
    cur = obj
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur