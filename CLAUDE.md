# CLAUDE.md — fastRhockey-pwhl-raw Development Guide

## Repo Overview

`fastRhockey-pwhl-raw` is the R-side scraper that pulls Professional
Women's Hockey League (PWHL) raw game payloads from the HockeyTech API
via the [fastRhockey](https://github.com/sportsdataverse/fastRhockey)
package. It writes per-game JSON under `pwhl/json/raw/{game_id}.json`
and `pwhl/json/final/{game_id}.json`, season schedules under
`pwhl/schedules/{rds,parquet}/`, and a combined-across-seasons
`pwhl/pwhl_schedule_master.{rds,parquet}`. Results are committed back
to `main`. Every push fires a `repository_dispatch` that wakes the
downstream R parser in
[fastRhockey-pwhl-data](https://github.com/sportsdataverse/fastRhockey-pwhl-data).
This repo is the authoritative cache of raw HockeyTech PWHL payloads.

Package name from `DESCRIPTION`: `fastRhockey.pwhl.raw` (v1.0.0,
License: MIT).

## Pipeline Position

```
HockeyTech APIs --[R scrape]--> fastRhockey-pwhl-raw [HERE]
                                    | push trigger
                                    v
                               fastRhockey-pwhl-data --[release upload]--> sportsdataverse-data
                                                                              | piggyback
                                                                              v
                                                                        fastRhockey R package
```

The push trigger lives in
`.github/workflows/fastRhockey_pwhl_data_trigger.yml` and fires a
`repository_dispatch` against `sportsdataverse/fastRhockey-pwhl-data`
when `pwhl/**` changes land on `main`.

## Build & Development Commands

The repo is driven by `scripts/daily_pwhl_scraper.sh`, which calls the
R scraper in a per-season loop and commits + pushes between seasons:

```sh
# Full daily flow for one or more seasons (CI / local entry point)
bash scripts/daily_pwhl_scraper.sh -s 2026 -e 2026 -r TRUE

# Or call the R scraper directly when iterating
Rscript R/scrape_pwhl_raw.R -s 2026                # single season (end year 2026 = 2025-26)
Rscript R/scrape_pwhl_raw.R -s 2024 -e 2026        # range: 2023-24 through 2025-26
Rscript R/scrape_pwhl_raw.R -s 2026 -r TRUE        # rescrape existing files
```

Season convention: `-s` / `-e` refer to the **end year** of the season
(2026 means 2025-26). This matches `fastRhockey::most_recent_pwhl_season()`
and `fastRhockey::pwhl_schedule()`, both of which take the end year
directly.

`-r TRUE` forces re-scrape of games already on disk; `-r FALSE` skips
existing JSON files. Output paths the scraper writes:

- `pwhl/json/raw/{game_id}.json`    — raw HockeyTech API responses per game
- `pwhl/json/final/{game_id}.json`  — processed via the fastRhockey pipeline (PBP, box scores, game info)
- `pwhl/schedules/rds/pwhl_schedule_{end_year}.rds`        — per-season schedule (with `game_json` / `game_json_url` pointing at `pwhl/json/final/`)
- `pwhl/schedules/parquet/pwhl_schedule_{end_year}.parquet` — same content, parquet format
- `pwhl/pwhl_schedule_master.rds` / `pwhl_schedule_master.parquet` — combined schedule across all seasons

## Project Structure

```
R/
  scrape_pwhl_raw.R           # The single R entry point — pulls schedule + per-game JSON
scripts/
  daily_pwhl_scraper.sh       # Bash wrapper for cron / CI; loops seasons, commits per season
pwhl/                         # Committed scraped output (consumed downstream)
  json/raw/                   #   raw HockeyTech responses per game
  json/final/                 #   processed JSON (PBP / box scores / game info)
  schedules/rds/              #   per-season schedules (.rds)
  schedules/parquet/          #   per-season schedules (.parquet)
  pwhl_schedule_master.rds    #   combined schedule (all seasons)
  pwhl_schedule_master.parquet
logs/
  fastRhockey_pwhl_raw_logfile_{year}.log   # per-season scrape logs
.github/workflows/
  scrape_pwhl_raw.yml                       # Daily scrape (cron + workflow_dispatch)
  fastRhockey_pwhl_data_trigger.yml         # Fires repository_dispatch on push
DESCRIPTION                   # Package metadata (declares Remotes: sportsdataverse/fastRhockey)
```

## sportsdataverse-data Release Tags

The downstream `fastRhockey-pwhl-data` repo publishes the parsed
datasets sourced from this raw cache to the following tags on
[`sportsdataverse-data`](https://github.com/sportsdataverse/sportsdataverse-data):

| Release tag | Content |
|-------------|---------|
| [`pwhl_pbp`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/pwhl_pbp) | PWHL play-by-play |
| [`pwhl_player_boxscores`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/pwhl_player_boxscores) | PWHL player box scores (skaters + goalies) |
| [`pwhl_rosters`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/pwhl_rosters) | PWHL rosters |
| [`pwhl_schedules`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/pwhl_schedules) | PWHL schedules |

## Conventions

- `R/scrape_pwhl_raw.R` is **the** entry point. It calls into the
  installed `fastRhockey` package (declared as a `Remotes:` in
  `DESCRIPTION`) plus `dplyr`, `glue`, `purrr`, `jsonlite`, `httr`,
  `arrow`, `optparse`, and `cli`.
- HockeyTech feeds return JSONP wrapped in `angular.callbacks._X(...)`.
  The scraper's `.safe_pwhl_api()` helper regex-strips that wrapper
  before `jsonlite::parse_json()`. Do **not** swap to `fromJSON()`
  unless you also strip the JSONP shell.
- HTTP requests use `httr::RETRY("GET", url, times = 3, pause_min = 1)`
  and `httr::stop_for_status()` so transient HockeyTech 5xx responses
  don't abort the season.
- Scrape logic that needs fixing in upstream parsing (boxscore shape,
  PBP field drift, etc.) belongs in the `fastRhockey` package — not in
  this repo. This repo should stay thin.
- Schedules are written to **both** `.rds` and `.parquet` for the same
  season so downstream consumers can pick their format.
- Don't reorganize the `pwhl/` output tree without aligning
  `fastRhockey-pwhl-data` (its parsers read from
  `https://raw.githubusercontent.com/sportsdataverse/fastRhockey-pwhl-raw/main/pwhl/...`).

## Daily Cron Workflow

`scripts/daily_pwhl_scraper.sh` is the cron entry point. For each
season in the `-s..-e` range it:

1. `git pull`, configures the local `action@github.com` author
2. `Rscript R/scrape_pwhl_raw.R -s $i -e $i -r $RESCRAPE`
3. Stages `pwhl/*` and `pwhl/pwhl_schedule_master.*`
4. Commits with the load-bearing message `PWHL Raw Updated (Start: $i End: $i)`
5. Pushes — which fires the downstream trigger workflow
6. Separately commits the log file at `logs/fastRhockey_pwhl_raw_logfile_$i.log`
   with `PWHL Raw log update (Start: $i End: $i)`

The umbrella commit message format `<Sport> Raw Update (Start: YYYY End: YYYY)`
is **load-bearing** — downstream workflows parse the years out of it.
Do not reformat without coordinating with `fastRhockey-pwhl-data`.

## Project-Specific Gotchas

- PWHL `season` always means **end year** (2026 ⇒ 2025-26). Do not
  follow NHL `"20242025"` concatenated-year conventions here.
- HockeyTech JSONP must be stripped before parsing; tests of new
  endpoints should round-trip through `.safe_pwhl_api()`.
- HockeyTech has multiple feeds (`statviewfeed`, `modulekit`, `gc`),
  each with its own API key. The current scraper relies on
  `fastRhockey::pwhl_*()` to thread the right keys — don't hard-code
  endpoints here.
- `pwhl_schedule_master.{rds,parquet}` is regenerated on every run by
  concatenating per-season schedules. Stale or malformed per-season
  files will corrupt the master; the daily run will pick them up.
- Force-pushes can land changes without firing the downstream
  dispatch. Always push normally.

## Cross-Repo References

- R package this repo's scraper calls into: <https://github.com/sportsdataverse/fastRhockey>
- Downstream parser: <https://github.com/sportsdataverse/fastRhockey-pwhl-data>
- Sister raw repos: <https://github.com/sportsdataverse/fastRhockey-nhl-raw>, <https://github.com/sportsdataverse/wehoop-wbb-raw>
- Release destination: <https://github.com/sportsdataverse/sportsdataverse-data>

## Commit Convention

Daily automated commits use the load-bearing umbrella format:

```
PWHL Raw Updated (Start: 2026 End: 2026)
PWHL Raw log update (Start: 2026 End: 2026)
```

All other commits use [Conventional Commits](https://www.conventionalcommits.org/):

```
feat(scrape): add three-stars to pwhl/json/final payload
fix(scrape): handle JSONP stripping for empty HockeyTech responses
chore(deps): bump fastRhockey pin in DESCRIPTION Remotes
ci: align scrape_pwhl_raw.yml cadence with downstream parser
```

Prefer scoped subjects (`feat(scrape): ...`, `ci(trigger): ...`). Use
`type!:` or a `BREAKING CHANGE:` footer for breaking changes. Split
unrelated work into separate commits for reviewability.

**Important: Never include AI agents or assistants (e.g., Claude,
Copilot, Cursor, GPT, Gemini) as co-authors on commits.** Omit all
`Co-Authored-By` trailers referencing AI tools. This applies whether
the change was generated, refactored, or reviewed with AI assistance
— the human author is the sole attributable contributor.
