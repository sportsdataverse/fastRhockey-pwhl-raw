"""build_raw_json port — the four HockeyTech payloads for a PWHL game.

Keys and URL shapes are verbatim from R/scrape_pwhl_raw.R (statviewfeed + gc
use their embedded public keys; gameshifts uses the modulekit view with the
sdv-py PWHL registry key, JSON format — no JSONP wrapper, which
``safe_pwhl_api`` tolerates).
"""

from __future__ import annotations

from typing import Optional

from pwhl_raw.client import safe_pwhl_api

_BASE = "https://lscluster.hockeytech.com/feed/index.php"
_SVF_KEY = "694cfeed58c932ee"
_GC_KEY = "446521baf8c38984"


def build_raw_json(gid: int) -> Optional[dict]:
    pbp_raw = safe_pwhl_api(
        f"{_BASE}?feed=statviewfeed&view=gameCenterPlayByPlay&game_id={gid}"
        f"&key={_SVF_KEY}&client_code=pwhl&lang=en&league_id="
        "&callback=angular.callbacks._0"
    )
    summary_raw = safe_pwhl_api(
        f"{_BASE}?feed=statviewfeed&view=gameSummary&game_id={gid}"
        f"&key={_SVF_KEY}&site_id=2&client_code=pwhl&lang=en&league_id="
        "&callback=angular.callbacks._0"
    )
    gc_raw = safe_pwhl_api(
        f"{_BASE}?feed=gc&tab=gamesummary&game_id={gid}"
        f"&key={_GC_KEY}&client_code=pwhl&site_id=0&lang=en"
        "&callback=angular.callbacks._0"
    )
    shifts_raw = safe_pwhl_api(
        f"{_BASE}?feed=modulekit&view=gameshifts&game_id={gid}&key={_GC_KEY}&fmt=json&client_code=pwhl&lang=en"
    )
    if pbp_raw is None and summary_raw is None:
        return None
    return {
        "pbp_raw": pbp_raw,
        "summary_raw": summary_raw,
        "gc_raw": gc_raw,
        "shifts_raw": shifts_raw,
    }
