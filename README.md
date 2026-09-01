# fastRhockey-pwhl-raw

Raw PWHL game JSON data scraped from the HockeyTech API via [fastRhockey](https://github.com/sportsdataverse/fastRhockey).

```mermaid
  graph LR;
    A[fastRhockey-pwhl-raw]-->B[fastRhockey-pwhl-data];
    B[fastRhockey-pwhl-data]-->C1[pwhl_pbp];
    B[fastRhockey-pwhl-data]-->C2[pwhl_player_boxscores];
    B[fastRhockey-pwhl-data]-->C3[pwhl_rosters];
    B[fastRhockey-pwhl-data]-->C4[pwhl_schedules];

```

## fastRhockey PWHL workflow diagram

```mermaid
flowchart TB;
    subgraph A[fastRhockey-pwhl-raw];
        direction TB;
        A1[scripts/daily_pwhl_scraper.sh]-->A2[R/scrape_pwhl_raw.R];
    end;

    subgraph B[fastRhockey-pwhl-data];
        direction TB;
        B1[scripts/daily_pwhl_R_processor.sh]-->B2[R/pwhl_data_creation.R];
    end;

    subgraph C[sportsdataverse Releases];
        direction TB;
        C1[pwhl_pbp];
        C2[pwhl_player_boxscores];
        C3[pwhl_rosters];
        C4[pwhl_schedules];
    end;

    A-->B;
    B-->C1;
    B-->C2;
    B-->C3;
    B-->C4;

    click C1 "https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/pwhl_pbp" _blank;
    click C2 "https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/pwhl_player_boxscores" _blank;
    click C3 "https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/pwhl_rosters" _blank;
    click C4 "https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/pwhl_schedules" _blank;

```

## sportsdataverse-data releases

| Release tag | Content |
|-----|---------|
| [`pwhl_pbp`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/pwhl_pbp) | PWHL play-by-play data |
| [`pwhl_player_boxscores`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/pwhl_player_boxscores) | PWHL player box scores (skaters + goalies) |
| [`pwhl_rosters`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/pwhl_rosters) | PWHL rosters |
| [`pwhl_schedules`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/pwhl_schedules) | PWHL schedules |

## Structure

```
pwhl/
├── json/
│   ├── raw/              # Raw HockeyTech API responses per game
│   └── final/            # Processed via fastRhockey pipeline (PBP, box scores, game info)
├── schedules/
│   ├── rds/              # Season schedules (pwhl_schedule_{year}.rds)
│   └── parquet/          # Season schedules in parquet format
├── pwhl_schedule_master.rds      # Combined schedule across all seasons
└── pwhl_schedule_master.parquet
```

## Data Sources

- **HockeyTech statviewfeed** — play-by-play, game summary, schedule
- **HockeyTech gc feed** — game center summary (scoring, penalties, shots, three stars)

## Reports & explainers

<!-- BEGIN GENERATED: reports -->

| Report | What it is | Last updated |
|---|---|---|
| _none yet_ | — | — |

<!-- END GENERATED: reports -->

## Automation & status

<!-- BEGIN GENERATED: status -->

| workflow | schedule | last run |
|---|---|---|
| [![fastRhockey_pwhl_data_trigger.yml](https://github.com/sportsdataverse/fastRhockey-pwhl-raw/actions/workflows/fastRhockey_pwhl_data_trigger.yml/badge.svg)](https://github.com/sportsdataverse/fastRhockey-pwhl-raw/actions/workflows/fastRhockey_pwhl_data_trigger.yml) | on push / dispatch | 2026-07-18 |
| [![scrape_pwhl_raw.yml](https://github.com/sportsdataverse/fastRhockey-pwhl-raw/actions/workflows/scrape_pwhl_raw.yml/badge.svg)](https://github.com/sportsdataverse/fastRhockey-pwhl-raw/actions/workflows/scrape_pwhl_raw.yml) | daily 08:00 UTC in Nov-Dec; daily 08:00 UTC in Jan-Mar; daily 08:00 UTC in Apr-May | 2026-05-31 |

<!-- END GENERATED: status -->
- **Scraping workflow** runs daily during the PWHL season (Nov-May)
- On push, triggers the [fastRhockey-pwhl-data](https://github.com/sportsdataverse/fastRhockey-pwhl-data) repo to compile datasets

## Related repositories

[fastRhockey-pwhl-raw data repository (source: HockeyTech API)](https://github.com/sportsdataverse/fastRhockey-pwhl-raw)

[fastRhockey-pwhl-data repository (source: HockeyTech API)](https://github.com/sportsdataverse/fastRhockey-pwhl-data)

[fastRhockey-nhl-raw data repository (source: NHL API)](https://github.com/sportsdataverse/fastRhockey-nhl-raw)

[fastRhockey-nhl-data repository (source: NHL API)](https://github.com/sportsdataverse/fastRhockey-nhl-data)

[fastRhockey-data legacy repository (archived; sources: NHL Stats API + PHF)](https://github.com/sportsdataverse/fastRhockey-data)

## Part of the [SportsDataverse](https://sportsdataverse.org/)

## Consumers

The packages that read what this repo produces:

- **R:** [fastRhockey](https://fastRhockey.sportsdataverse.org) — docs at <https://fastRhockey.sportsdataverse.org>
- **Python:** [`sportsdataverse.pwhl`](https://github.com/sportsdataverse/sportsdataverse-py) — docs at <https://py.sportsdataverse.org>

## Stage inventory

Every numbered pipeline stage in `python/` (auto-listed; run subsets with the `scripts/*.sh` drivers by number or name):

- `python/pwhl_raw_01_schedules.py`
- `python/pwhl_raw_02_games.py`
- `python/pwhl_raw_03_link_master.py`
