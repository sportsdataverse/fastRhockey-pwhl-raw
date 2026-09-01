"""Season schedules + master (port of the R main-loop steps 1 and 3).

Schedule frames come from sdv-py ``pwhl_schedule`` (fastRhockey column
parity); ``game_json`` / ``game_json_url`` link columns point at the
committed ``pwhl/json/final`` tree on raw.githubusercontent.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

RAW_REPO = "sportsdataverse/fastRhockey-pwhl-raw"
RAW_BRANCH = "main"
PATH_FINAL = "pwhl/json/final"


def fetch_schedule(season_year: int) -> pl.DataFrame:
    from sportsdataverse.pwhl import pwhl_schedule

    sched = pwhl_schedule(season=season_year)
    if sched is None or sched.height == 0:
        return pl.DataFrame()
    return sched.with_columns(pl.lit(season_year, dtype=pl.Int64).alias("season"))


def link_schedule(sched: pl.DataFrame, final_dir: str | Path = PATH_FINAL) -> pl.DataFrame:
    final_ids = {int(p.stem) for p in Path(final_dir).glob("*.json") if p.stem.isdigit()}
    return sched.with_columns(
        pl.col("game_id").cast(pl.Int64, strict=False).is_in(sorted(final_ids) or [-1]).alias("game_json")
    ).with_columns(
        pl.when(pl.col("game_json"))
        .then(
            pl.format(
                "https://raw.githubusercontent.com/{}/{}/{}/{}.json",
                pl.lit(RAW_REPO),
                pl.lit(RAW_BRANCH),
                pl.lit(PATH_FINAL),
                pl.col("game_id"),
            )
        )
        .otherwise(None)
        .alias("game_json_url")
    )


def write_season(sched: pl.DataFrame, season_year: int, out_root: str | Path = "pwhl") -> None:
    from sportsdataverse._rds import write_rds

    out = Path(out_root)
    (out / "schedules" / "parquet").mkdir(parents=True, exist_ok=True)
    (out / "schedules" / "rds").mkdir(parents=True, exist_ok=True)
    sched.write_parquet(
        out / "schedules" / "parquet" / f"pwhl_schedule_{season_year}.parquet",
        compression="gzip",
    )
    write_rds(
        sched,
        out / "schedules" / "rds" / f"pwhl_schedule_{season_year}.rds",
        cls=["tbl_df", "tbl", "data.frame"],
    )


def build_master(out_root: str | Path = "pwhl") -> pl.DataFrame:
    from sportsdataverse._rds import write_rds

    out = Path(out_root)
    files = sorted((out / "schedules" / "parquet").glob("pwhl_schedule_*.parquet"))
    if not files:
        return pl.DataFrame()
    master = pl.concat([pl.read_parquet(f) for f in files], how="diagonal_relaxed").sort("game_date", descending=True)
    master.write_parquet(out / "pwhl_schedule_master.parquet", compression="gzip")
    write_rds(master, out / "pwhl_schedule_master.rds", cls=["tbl_df", "tbl", "data.frame"])
    return master
