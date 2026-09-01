"""Stage 02 — scrape raw + processed final JSON per completed game.

Python port of the R scraper's step 2 (``download_game`` loop): fetches the
four HockeyTech payloads (raw), builds the processed final via the sdv-py
PWHL surface + the flat parsers, and routes every final write through the
``merge_with_existing`` guard (a rebuild must never delete good blocks from
a previously-captured game — the 2026-07-18 incident). Resumable: existing
finals are skipped unless ``--rescrape``.

Usage::

    python -m pwhl_raw_02_games -s 2026 [--rescrape] [--sleep 0.5]
    scripts/pwhl_raw.sh 02
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    import polars as pl
    from pwhl_raw.client import merge_with_existing, write_json
    from pwhl_raw.final import build_final_json
    from pwhl_raw.raw import build_raw_json

    ap = argparse.ArgumentParser(prog="python -m pwhl_raw_02_games")
    ap.add_argument("-s", "--start", type=int, required=True, help="season END year")
    ap.add_argument("-e", "--end", type=int, help="end season END year (default: --start)")
    ap.add_argument("--out", default="pwhl")
    ap.add_argument("--rescrape", action="store_true", help="re-fetch games already on disk")
    ap.add_argument("--sleep", type=float, default=0.5, help="HockeyTech rate-limit pause")
    ap.add_argument("--limit", type=int, default=0, help="cap games scraped (0 = all)")
    a = ap.parse_args(argv)

    raw_dir = Path(a.out) / "json" / "raw"
    final_dir = Path(a.out) / "json" / "final"
    raw_dir.mkdir(parents=True, exist_ok=True)
    final_dir.mkdir(parents=True, exist_ok=True)

    total = 0
    for year in range(a.start, (a.end or a.start) + 1):
        sched_path = Path(a.out) / "schedules" / "parquet" / f"pwhl_schedule_{year}.parquet"
        if not sched_path.is_file():
            raise SystemExit(f"missing {sched_path} — run pwhl_raw_01_schedules first")
        sched = pl.read_parquet(sched_path)
        games = sched.filter(pl.col("game_status").cast(pl.Utf8).str.contains("(?i)final"))
        gids = [int(g) for g in games["game_id"].cast(pl.Int64, strict=False).drop_nulls().to_list()]
        if not a.rescrape:
            gids = [g for g in gids if not (final_dir / f"{g}.json").is_file()]
        print(f"{year}: {len(gids)} game(s) to scrape")
        for i, gid in enumerate(gids, start=1):
            if a.limit and total >= a.limit:
                print(f"--limit {a.limit} reached")
                return 0
            try:
                raw = build_raw_json(gid)
                if raw is None:
                    print(f"  {gid}: no raw payloads; skipped")
                    continue
                write_json(raw, raw_dir / f"{gid}.json")
                final = build_final_json(gid, raw)
                if final is not None:
                    final = merge_with_existing(final, final_dir / f"{gid}.json", gid)
                    write_json(final, final_dir / f"{gid}.json")
                total += 1
            except Exception as exc:  # noqa: BLE001 — one bad game must not abort the season
                print(f"  Failed game {gid}: {exc}")
            if i % 25 == 0 or i == len(gids):
                print(f"  Progress: {i}/{len(gids)} games")
            time.sleep(a.sleep)
    print(f"scraped {total} game(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
