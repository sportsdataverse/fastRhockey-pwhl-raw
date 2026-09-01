"""Stage 01 — fetch + write per-season PWHL schedules (with final-JSON links).

Python port of the R scraper's step 1/3 schedule handling; the R scraper
(`R/scrape_pwhl_raw.R`) remains the scheduled production path.

Usage::

    python -m pwhl_raw_01_schedules -s 2026 [-e 2026] [--out pwhl]
    scripts/pwhl_raw.sh 01
"""

from __future__ import annotations

import argparse


def main(argv: list[str] | None = None) -> int:
    from pwhl_raw.schedules import fetch_schedule, link_schedule, write_season

    ap = argparse.ArgumentParser(prog="python -m pwhl_raw_01_schedules")
    ap.add_argument("-s", "--start", type=int, required=True, help="season END year (2026 = 2025-26)")
    ap.add_argument("-e", "--end", type=int, help="end season END year (default: --start)")
    ap.add_argument("--out", default="pwhl")
    a = ap.parse_args(argv)

    for year in range(a.start, (a.end or a.start) + 1):
        sched = fetch_schedule(year)
        if sched.height == 0:
            print(f"{year}: no schedule rows; skipped")
            continue
        sched = link_schedule(sched, f"{a.out}/json/final")
        write_season(sched, year, a.out)
        print(f"{year}: {sched.height} schedule rows ({int(sched['game_json'].sum())} linked to final JSON)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
