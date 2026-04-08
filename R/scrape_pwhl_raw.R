## Scrape raw PWHL game JSON and schedules into fastRhockey-pwhl-raw
## Usage:
##   Rscript R/scrape_pwhl_raw.R -s 2024           (single season)
##   Rscript R/scrape_pwhl_raw.R -s 2024 -e 2025   (range of seasons)
##   Rscript R/scrape_pwhl_raw.R -s 2025 -r TRUE   (rescrape existing)
##
## Outputs:
##   pwhl/json/raw/{game_id}.json    — raw API data from HockeyTech endpoints
##   pwhl/json/final/{game_id}.json  — processed via fastRhockey pipeline
##   pwhl/schedules/{rds,parquet}/pwhl_schedule_{year}.*
##     (includes game_json, game_json_url pointing to final/)
##   pwhl/pwhl_schedule_master.{rds,parquet}

suppressPackageStartupMessages(library(fastRhockey))
suppressPackageStartupMessages(library(dplyr))
suppressPackageStartupMessages(library(glue))
suppressPackageStartupMessages(library(purrr))
suppressPackageStartupMessages(library(jsonlite))
suppressPackageStartupMessages(library(httr))
suppressPackageStartupMessages(library(arrow))
suppressPackageStartupMessages(library(optparse))
suppressPackageStartupMessages(library(cli))


option_list <- list(
  optparse::make_option(
    c("-s", "--start_year"),
    action = "store",
    default = fastRhockey:::most_recent_pwhl_season(),
    type = "integer",
    help = "Start year of the seasons to process [default: current season]"
  ),
  optparse::make_option(
    c("-e", "--end_year"),
    action = "store",
    default = NA_integer_,
    type = "integer",
    help = "End year of the seasons to process [default: same as start_year]"
  ),
  optparse::make_option(
    c("-r", "--rescrape"),
    action = "store",
    default = TRUE,
    type = "logical",
    help = "Rescrape games that already have JSON files [default: TRUE]"
  )
)

opt <- optparse::parse_args(optparse::OptionParser(option_list = option_list))
options(stringsAsFactors = FALSE)
options(scipen = 999)

if (is.na(opt$end_year)) opt$end_year <- opt$start_year
season_vector <- opt$start_year:opt$end_year
rescrape <- opt$rescrape

# ── Logging ──────────────────────────────────────────────────────────────
LOG_FILE <- glue::glue("logs/fastRhockey_pwhl_raw_logfile_{opt$start_year}.log")
logging <- function(msg, level = "INFO") {
  entry <- paste0(format(Sys.time(), "[%Y-%m-%d %H:%M:%S] "), level, ": ", msg)
  cat(entry, "\n", file = LOG_FILE, append = TRUE)
}
logging("=== PWHL Raw Scraper started ===")


RAW_REPO <- "sportsdataverse/fastRhockey-pwhl-raw"
RAW_BRANCH <- "main"
PATH_RAW <- "pwhl/json/raw"
PATH_FINAL <- "pwhl/json/final"


# ═══════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════

.safe_pwhl_api <- function(url) {
  tryCatch(
    {
      res <- httr::RETRY("GET", url, times = 3, pause_min = 1)
      httr::stop_for_status(res)
      raw_text <- httr::content(res, as = "text", encoding = "UTF-8")
      # Strip JSONP callback wrapper: angular.callbacks._X(...)
      raw_text <- sub("^angular\\.callbacks\\._\\w+\\(", "", raw_text)
      raw_text <- sub("\\)\\s*$", "", raw_text)
      jsonlite::parse_json(raw_text, simplifyVector = FALSE)
    },
    error = function(e) NULL
  )
}

.write_json <- function(data, path) {
  jsonlite::write_json(data,
    path = path,
    auto_unbox = TRUE, null = "null", na = "null"
  )
}


# ═══════════════════════════════════════════════════════════════════════
# build_raw_json: fetch HockeyTech endpoints for a PWHL game
#
# Stores:
#   pbp_raw       → raw play-by-play events from statviewfeed
#   game_info_raw → game metadata from gameSummary
#   player_box_raw→ skater + goalie box from gameSummary
#   game_summary  → game center summary (scoring, penalties, shots, stars)
# ═══════════════════════════════════════════════════════════════════════

build_raw_json <- function(gid) {
  # Play-by-play endpoint
  pbp_url <- glue(
    "https://lscluster.hockeytech.com/feed/index.php",
    "?feed=statviewfeed&view=gameCenterPlayByPlay",
    "&game_id={gid}",
    "&key=694cfeed58c932ee&client_code=pwhl&lang=en",
    "&league_id=&callback=angular.callbacks._0"
  )
  pbp_raw <- .safe_pwhl_api(pbp_url)

  # Game summary endpoint (for box scores, game info)
  summary_url <- glue(
    "https://lscluster.hockeytech.com/feed/index.php",
    "?feed=statviewfeed&view=gameSummary",
    "&game_id={gid}",
    "&key=694cfeed58c932ee&site_id=2&client_code=pwhl&lang=en",
    "&league_id=&callback=angular.callbacks._0"
  )
  summary_raw <- .safe_pwhl_api(summary_url)

  # Game center summary (scoring, penalties, shots by period, three stars)
  gc_url <- glue(
    "https://lscluster.hockeytech.com/feed/index.php",
    "?feed=gc&tab=gamesummary",
    "&game_id={gid}",
    "&key=446521baf8c38984&client_code=pwhl&site_id=0&lang=en",
    "&callback=angular.callbacks._0"
  )
  gc_raw <- .safe_pwhl_api(gc_url)

  if (is.null(pbp_raw) && is.null(summary_raw)) {
    return(NULL)
  }

  list(
    pbp_raw = pbp_raw,
    summary_raw = summary_raw,
    gc_raw = gc_raw
  )
}


# ═══════════════════════════════════════════════════════════════════════
# build_final_json: run full fastRhockey processing pipeline
# ═══════════════════════════════════════════════════════════════════════

build_final_json <- function(gid, raw_data = NULL) {
  if (is.null(raw_data)) {
    raw_data <- build_raw_json(gid)
  }
  if (is.null(raw_data)) {
    return(NULL)
  }

  final <- raw_data

  # ── Processed PBP via fastRhockey ──
  tryCatch(
    {
      pbp <- fastRhockey::pwhl_pbp(game_id = gid)
      if (is.data.frame(pbp) && nrow(pbp) > 0) {
        final$pbp <- pbp
      }
    },
    error = function(e) {
      cli::cli_alert_warning("PBP pipeline failed for {gid}: {conditionMessage(e)}")
    }
  )

  # ── Processed player box via fastRhockey ──
  tryCatch(
    {
      box <- fastRhockey::pwhl_player_box(game_id = gid)
      if (is.list(box)) {
        final$skaters <- box$skaters
        final$goalies <- box$goalies
      }
    },
    error = function(e) {
      cli::cli_alert_warning("Player box failed for {gid}: {conditionMessage(e)}")
    }
  )

  # ── Processed game info via fastRhockey ──
  tryCatch(
    {
      info <- fastRhockey::pwhl_game_info(game_id = gid)
      if (is.data.frame(info) && nrow(info) > 0) {
        final$game_info <- info
      }
    },
    error = function(e) {
      cli::cli_alert_warning("Game info failed for {gid}: {conditionMessage(e)}")
    }
  )

  # ── Processed game summary via fastRhockey ──
  tryCatch(
    {
      gs <- fastRhockey::pwhl_game_summary(game_id = gid)
      if (is.list(gs)) {
        final$game_summary <- gs
      }
    },
    error = function(e) {
      cli::cli_alert_warning("Game summary failed for {gid}: {conditionMessage(e)}")
    }
  )

  final
}


# ═══════════════════════════════════════════════════════════════════════
# download_game: raw + final
# ═══════════════════════════════════════════════════════════════════════

download_game <- function(gid, process = TRUE,
                          path_raw = "pwhl/json/raw",
                          path_final = "pwhl/json/final") {
  raw_data <- tryCatch(
    build_raw_json(gid),
    error = function(e) {
      cli::cli_alert_warning("Raw JSON failed for {gid}: {conditionMessage(e)}")
      NULL
    }
  )
  if (!is.null(raw_data)) {
    .write_json(raw_data, glue("{path_raw}/{gid}.json"))
  }

  if (process) {
    final_data <- tryCatch(
      build_final_json(gid, raw_data = raw_data),
      error = function(e) {
        cli::cli_alert_warning("Final JSON failed for {gid}: {conditionMessage(e)}")
        NULL
      }
    )
    if (!is.null(final_data)) {
      .write_json(final_data, glue("{path_final}/{gid}.json"))
    }
  }
}


# ═══════════════════════════════════════════════════════════════════════
# Main loop
# ═══════════════════════════════════════════════════════════════════════

for (season_year in season_vector) {
  cli::cli_h1("Processing {season_year} PWHL season")
  logging(glue("=== {season_year} PWHL season ==="))


  # ── STEP 1: Fetch and save schedule ──────────────────────────────────

  cli::cli_progress_step(
    msg = "Fetching {season_year} PWHL schedule",
    msg_done = "Fetched {season_year} PWHL schedule"
  )

  # Fetch regular season schedule
  sched <- tryCatch(
    fastRhockey::pwhl_schedule(season = season_year, game_type = "regular"),
    error = function(e) data.frame()
  )

  # Also fetch playoffs if available
  sched_playoffs <- tryCatch(
    fastRhockey::pwhl_schedule(season = season_year, game_type = "playoffs"),
    error = function(e) data.frame()
  )

  if (nrow(sched_playoffs) > 0) {
    sched_playoffs$game_type <- "playoffs"
    sched$game_type <- "regular"
    sched <- dplyr::bind_rows(sched, sched_playoffs)
  } else if (nrow(sched) > 0) {
    sched$game_type <- "regular"
  }

  sched <- sched %>%
    dplyr::tibble() %>%
    dplyr::mutate(season = season_year)

  for (d in c("pwhl/schedules/rds", "pwhl/schedules/parquet")) {
    if (!dir.exists(d)) dir.create(d, recursive = TRUE)
  }

  # Filter to completed games (game_status == "Final")
  games <- sched %>%
    dplyr::filter(grepl("Final", .data$game_status, ignore.case = TRUE))

  cli::cli_alert_info("{nrow(games)} completed games in schedule")
  logging(glue("{nrow(games)} completed games in schedule"))

  if (nrow(games) == 0) {
    cli::cli_alert_warning("No completed games. Skipping season.")
    saveRDS(sched, glue("pwhl/schedules/rds/pwhl_schedule_{season_year}.rds"))
    arrow::write_parquet(sched,
      glue("pwhl/schedules/parquet/pwhl_schedule_{season_year}.parquet"),
      compression = "gzip"
    )
    next
  }


  # ── STEP 2: Scrape raw + process final game JSON ────────────────────

  for (d in c(PATH_RAW, PATH_FINAL)) {
    if (!dir.exists(d)) dir.create(d, recursive = TRUE)
  }

  if (rescrape) {
    games_to_scrape <- games
  } else {
    existing_final <- as.integer(gsub("\\.json$", "", list.files(PATH_FINAL)))
    games_to_scrape <- games %>%
      dplyr::filter(!(as.integer(.data$game_id) %in% existing_final))
  }

  cli::cli_progress_step(
    msg = "Scraping {nrow(games_to_scrape)} games for {season_year}",
    msg_done = "Scraped {nrow(games_to_scrape)} games for {season_year}"
  )

  if (nrow(games_to_scrape) > 0) {
    n_games <- nrow(games_to_scrape)
    for (i in seq_len(n_games)) {
      gid <- as.integer(games_to_scrape$game_id[i])
      tryCatch(
        {
          download_game(gid,
            process = TRUE,
            path_raw = PATH_RAW, path_final = PATH_FINAL
          )
        },
        error = function(e) {
          cli::cli_alert_warning("Failed game {gid}: {conditionMessage(e)}")
        }
      )
      if (i %% 25 == 0 || i == n_games) {
        cli::cli_alert_info("  Progress: {i}/{n_games} games")
      }
      # Rate limiting for HockeyTech API
      Sys.sleep(0.5)
    }
  }


  # ── STEP 3: Update schedule with game_json + game_json_url ───────────

  cli::cli_progress_step(
    msg = "Updating {season_year} schedule with JSON links",
    msg_done = "Updated {season_year} schedule with JSON links"
  )

  final_files <- as.integer(gsub("\\.json$", "", list.files(PATH_FINAL)))

  sched <- sched %>%
    dplyr::mutate(
      game_json = as.integer(.data$game_id) %in% final_files,
      game_json_url = dplyr::if_else(
        .data$game_json,
        glue::glue(
          "https://raw.githubusercontent.com/{RAW_REPO}/{RAW_BRANCH}/{PATH_FINAL}/{game_id}.json"
        ),
        NA_character_
      )
    )

  saveRDS(sched, glue("pwhl/schedules/rds/pwhl_schedule_{season_year}.rds"))
  arrow::write_parquet(sched,
    glue("pwhl/schedules/parquet/pwhl_schedule_{season_year}.parquet"),
    compression = "gzip"
  )

  n_raw <- length(list.files(PATH_RAW, pattern = "\\.json$"))
  n_final <- length(list.files(PATH_FINAL, pattern = "\\.json$"))
  cli::cli_alert_success(
    "{sum(sched$game_json)} of {nrow(sched)} games linked ({n_raw} raw, {n_final} final)"
  )
  logging(glue("{sum(sched$game_json)} of {nrow(sched)} games linked ({n_raw} raw, {n_final} final)"))

  rm(games, games_to_scrape, sched)
  gc()
} # end for season_year


# ═══════════════════════════════════════════════════════════════════════
# Build cross-season master schedule
# ═══════════════════════════════════════════════════════════════════════

cli::cli_progress_step(
  msg = "Building master schedule",
  msg_done = "Master schedule built"
)

sched_files <- list.files("pwhl/schedules/rds", pattern = "\\.rds$", full.names = TRUE)
sched_all <- purrr::map_dfr(sched_files, readRDS) %>%
  dplyr::arrange(dplyr::desc(game_date))

saveRDS(sched_all, "pwhl/pwhl_schedule_master.rds")
arrow::write_parquet(sched_all, "pwhl/pwhl_schedule_master.parquet", compression = "gzip")

cli::cli_alert_success(
  "{nrow(sched_all)} total schedule rows, {sum(sched_all$game_json, na.rm = TRUE)} with final JSON"
)
logging(glue("Master: {nrow(sched_all)} schedule rows, {sum(sched_all$game_json, na.rm = TRUE)} with final JSON"))
logging("=== PWHL Raw Scraper complete ===")
cli::cli_h1("All done!")
