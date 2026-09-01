"""HTTP + JSON plumbing (port of .safe_pwhl_api / .write_json / .merge_with_existing)."""

from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any, Optional

import requests

_JSONP_HEAD = re.compile(r"^angular\.callbacks\._\w+\(")
_JSONP_TAIL = re.compile(r"\)\s*$")


def safe_pwhl_api(url: str, *, retries: int = 3) -> Optional[dict]:
    """GET + strip the angular JSONP wrapper; ``None`` on any failure (R parity)."""
    for attempt in range(retries):
        try:
            res = requests.get(url, timeout=30)
            res.raise_for_status()
            text = res.text
            text = _JSONP_HEAD.sub("", text)
            text = _JSONP_TAIL.sub("", text)
            return json.loads(text)
        except Exception:
            if attempt == retries - 1:
                return None
            time.sleep(1 + attempt)
    return None


def write_json(data: Any, path: str | Path) -> None:
    """jsonlite::write_json parity: records orientation, null for NA/None."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(data, ensure_ascii=False, allow_nan=False), encoding="utf-8")


def block_populated(x: Any) -> bool:
    if x is None:
        return False
    try:
        return len(x) > 0
    except TypeError:
        return True


def merge_with_existing(new_data: dict, path: str | Path, gid: int) -> dict:
    """Carry forward blocks the fresh build lost.

    The final JSON is a whole-file overwrite and every enrichment block
    degrades to a missing field on error — so one failing dependency would
    silently DELETE good data from a previously-captured game (the
    2026-07-18 incident: a stale fastRhockey wiped shifts from 133 games).
    A completed game's blocks are immutable history: keep old data the new
    build lacks.
    """
    p = Path(path)
    if not p.is_file():
        return new_data
    try:
        old = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return new_data
    preserved = []
    for key, val in old.items():
        if block_populated(val) and not block_populated(new_data.get(key)):
            new_data[key] = val
            preserved.append(key)
    if preserved:
        print(f"Game {gid}: rebuild lost {preserved} -- kept the previous capture.")
    return new_data
