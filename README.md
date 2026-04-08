# fastRhockey-pwhl-raw

Raw PWHL game JSON data scraped from the HockeyTech API via [fastRhockey](https://github.com/sportsdataverse/fastRhockey).

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

## Automation

- **Scraping workflow** runs daily during the PWHL season (Nov-May)
- On push, triggers the [fastRhockey-pwhl-data](https://github.com/sportsdataverse/fastRhockey-pwhl-data) repo to compile datasets

## Part of the [SportsDataverse](https://sportsdataverse.org/)
