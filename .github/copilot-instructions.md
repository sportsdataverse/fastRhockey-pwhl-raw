# fastRhockey-pwhl-raw Copilot Instructions

## Project Context

This repo is the HockeyTech-scrape stage for the PWHL (R production scraper + a numbered python port: `python/pwhl_raw_01_schedules.py` / `02_games` / `03_link_master` over the `pwhl_raw` package, flat parsers oracle-parity-tested against committed finals). It writes
per-game JSON under `pwhl/json/raw/{game_id}.json` and
`pwhl/json/final/{game_id}.json`, per-season schedules under
`pwhl/schedules/{rds,parquet}/`, plus a combined
`pwhl/pwhl_schedule_master.{rds,parquet}`, and commits results to
`main`. Every push wakes the downstream R parser in
`fastRhockey-pwhl-data` via `repository_dispatch`
(`.github/workflows/fastRhockey_pwhl_data_trigger.yml`).

Pipeline: `HockeyTech -> fastRhockey-pwhl-raw [HERE] -> fastRhockey-pwhl-data -> sportsdataverse-data -> fastRhockey`.

Package name (`DESCRIPTION`): `fastRhockey.pwhl.raw` (v1.0.0, MIT).
The scraper calls into the `fastRhockey` package, declared via the
`Remotes:` field — bug fixes to HockeyTech parsing belong upstream
there, not in this repo.

## Repository Workflow

- Branch from `main`; `main` is the default and release branch.
- The CI entry point is
  `scripts/daily_pwhl_scraper.sh -s <START> -e <END> -r <TRUE|FALSE>`.
- The R entry point is `R/scrape_pwhl_raw.R` (scheduled production); the python port runs via `scripts/pwhl_raw.sh` or `python -m pwhl_raw_01_schedules` etc.
- Season args refer to **end year** (e.g. `2026` = 2025-26 season).
- Don't reorganize the `pwhl/` output tree without aligning the
  downstream parsers in `fastRhockey-pwhl-data`.

## Build & Development Commands

```sh
# Full daily flow for one or more seasons
bash scripts/daily_pwhl_scraper.sh -s 2026 -e 2026 -r TRUE

# Direct R invocation while iterating
Rscript R/scrape_pwhl_raw.R -s 2026
Rscript R/scrape_pwhl_raw.R -s 2024 -e 2026
Rscript R/scrape_pwhl_raw.R -s 2026 -r TRUE
```

`-r TRUE` forces re-scrape; `-r FALSE` skips JSON files already on
disk. Outputs:

- `pwhl/json/raw/{game_id}.json` — raw HockeyTech responses (forensics)
- `pwhl/json/final/{game_id}.json` — processed payload, consumed downstream
- `pwhl/schedules/rds/pwhl_schedule_{end_year}.rds`
- `pwhl/schedules/parquet/pwhl_schedule_{end_year}.parquet`
- `pwhl/pwhl_schedule_master.{rds,parquet}` — combined across seasons
- `logs/fastRhockey_pwhl_raw_logfile_{end_year}.log` — scrape log

## Code Style

- Follow the parent `fastRhockey` package's R conventions:
  `snake_case`, 2-space indent, tidyverse pipe.
- Internal helpers in `R/scrape_pwhl_raw.R` start with `.` (e.g.
  `.safe_pwhl_api()`, `.write_json()`).
- HTTP: `httr::RETRY("GET", url, times = 3, pause_min = 1)` and
  `httr::stop_for_status()`. Wrap in `tryCatch(..., error = function(e) NULL)`
  so a single bad game doesn't abort the season.
- HockeyTech feeds return JSONP wrapped in `angular.callbacks._X(...)`.
  Strip with the existing regex shell in `.safe_pwhl_api()` before
  `jsonlite::parse_json(..., simplifyVector = FALSE)`. Do **not**
  swap to `fromJSON()` without also stripping JSONP.
- Persistence: write `.rds` and `.parquet` for schedules; per-game
  JSON via `jsonlite::write_json(auto_unbox = TRUE, null = "null",
  na = "null")`.
- Don't add new HockeyTech parsing logic here — extend the
  `fastRhockey` package and call into it. This repo stays thin.

## Daily Cron Workflow

`scripts/daily_pwhl_scraper.sh` runs each in-range season
sequentially, commits the per-season output with
`PWHL Raw Updated (Start: $i End: $i)` (load-bearing message —
downstream parses the years out of it), pushes, then separately
commits the per-season log file under `logs/`. Each push to `main`
fires the downstream `fastRhockey_pwhl_data_trigger.yml`.

The CI workflow `.github/workflows/scrape_pwhl_raw.yml` is the cron
entry point used during the PWHL season (Nov-May). Manual runs use
`workflow_dispatch` inputs (`start_year`, `end_year`, `rescrape`).

## Cross-Repo References

- R package this repo's scraper calls into: <https://github.com/sportsdataverse/fastRhockey>
- Downstream parser: <https://github.com/sportsdataverse/fastRhockey-pwhl-data>
- Sister raw repos: <https://github.com/sportsdataverse/fastRhockey-nhl-raw>, <https://github.com/sportsdataverse/wehoop-wbb-raw>
- Release destination tags: `pwhl_pbp`, `pwhl_player_boxscores`,
  `pwhl_rosters`, `pwhl_schedules` on
  <https://github.com/sportsdataverse/sportsdataverse-data/releases>

## Conventional Commits

Use: `type(scope): description`. Common types: `feat`, `fix`, `chore`,
`ci`, `docs`, `refactor`. Use `type!:` or a `BREAKING CHANGE:` footer
for breaking changes. The daily umbrella commits
(`PWHL Raw Updated (Start: YYYY End: YYYY)`) are exempt — that format
is load-bearing.

**Important: Never include AI agents or assistants (e.g., Claude,
Copilot, Cursor, GPT, Gemini) as co-authors on commits.** Omit all
`Co-Authored-By` trailers referencing AI tools. This applies whether
the change was generated, refactored, or reviewed with AI assistance
— the human author is the sole attributable contributor.
