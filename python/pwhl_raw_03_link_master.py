"""Stage 03 — refresh schedule final-JSON links + rebuild the master schedule.

Python port of the R scraper's step 3 + the cross-season master build. Run
after stage 02 so the ``game_json`` / ``game_json_url`` columns reflect the
post-scrape state of ``pwhl/json/final``.

Usage::

    python -m pwhl_raw_03_link_master -s 2024 -e 2026 [--out pwhl]
    scripts/pwhl_raw.sh 03
"""

from __future__ import annotations

import argparse
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    import polars as pl
    from pwhl_raw.schedules import build_master, link_schedule, write_season

    ap = argparse.ArgumentParser(prog="python -m pwhl_raw_03_link_master")
    ap.add_argument("-s", "--start", type=int, required=True, help="season END year")
    ap.add_argument("-e", "--end", type=int, help="end season END year (default: --start)")
    ap.add_argument("--out", default="pwhl")
    a = ap.parse_args(argv)

    for year in range(a.start, (a.end or a.start) + 1):
        sched_path = Path(a.out) / "schedules" / "parquet" / f"pwhl_schedule_{year}.parquet"
        if not sched_path.is_file():
            print(f"{year}: no schedule parquet; skipped")
            continue
        sched = pl.read_parquet(sched_path).drop(
            [c for c in ("game_json", "game_json_url") if c in ("game_json", "game_json_url")],
            strict=False,
        )
        sched = link_schedule(sched, f"{a.out}/json/final")
        write_season(sched, year, a.out)
        print(f"{year}: relinked ({int(sched['game_json'].sum())} with final JSON)")

    master = build_master(a.out)
    if master.height:
        linked = int(master["game_json"].sum()) if "game_json" in master.columns else 0
        print(f"master: {master.height} rows, {linked} with final JSON")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
