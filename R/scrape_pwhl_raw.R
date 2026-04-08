## Scrape raw PWHL game JSON and schedules into fastRhockey-pwhl-raw
##
## NOTE ON SEASON CONVENTION:
##   -s / -e refer to the *end year* of the season (e.g., 2026 for 2025-26).
##   This matches `fastRhockey::most_recent_pwhl_season()` which returns the
##   end year. `fastRhockey::pwhl_schedule()` also takes the end year directly.
##
## Usage:
##   Rscript R/scrape_pwhl_raw.R -s 2026           (single season: 2025-26)
##   Rscript R/scrape_pwhl_raw.R -s 2024 -e 2026   (range: 2023-24 through 2025-26)
##   Rscript R/scrape_pwhl_raw.R -s 2026 -r TRUE   (rescrape existing)
##
## Outputs:
##   pwhl/json/raw/{game_id}.json    — raw API data from HockeyTech endpoints
##   pwhl/json/final/{game_id}.json  — processed via fastRhockey pipeline
##   pwhl/schedules/{rds,parquet}/pwhl_schedule_{end_year}.*
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
    default = fastRhockey::most_recent_pwhl_season(),
    type = "integer",
    help = "Start season's end year to process, e.g. 2026 for 2025-26 [default: most recent]"
  ),
  optparse::make_option(
    c("-e", "--end_year"),
    action = "store",
    default = NA_integer_,
    type = "integer",
    help = "End season's end year to process [default: same as start_year]"
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

cli::cli_alert_info("=== PWHL Raw Scraper started ===")


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
# Flat-dataset parsers
#
# These extract per-game flat data frames directly from the parsed
# `summary_raw` (statviewfeed/gameSummary) and `gc_raw` (gc/gamesummary)
# structures, so the final JSON contains compile-ready tables matching
# the PHF JSON contract: team_box, scoring, penalties, three_stars,
# officials, shots_by_period, shootout, game_rosters, game_info.
#
# Inputs are parsed lists (jsonlite::parse_json(simplifyVector = FALSE)).
# All parsers tolerate missing fields and return NULL or zero-row frames
# rather than erroring.
# ═══════════════════════════════════════════════════════════════════════

# Local null-coalesce so we don't depend on rlang/base R 4.4+ being loaded.
`%||%` <- function(a, b) if (is.null(a)) b else a

.null2na <- function(x, default = NA) {
  if (is.null(x) || length(x) == 0) default else x[[1]]
}

.as_int <- function(x, default = NA_integer_) {
  v <- .null2na(x, default)
  suppressWarnings(as.integer(v))
}

.as_num <- function(x, default = NA_real_) {
  v <- .null2na(x, default)
  suppressWarnings(as.numeric(v))
}

.as_chr <- function(x, default = NA_character_) {
  v <- .null2na(x, default)
  if (is.na(v)) NA_character_ else as.character(v)
}

.parse_game_info <- function(raw, gid) {
  s <- raw$summary_raw
  if (is.null(s)) {
    return(data.frame(game_id = as.integer(gid), stringsAsFactors = FALSE))
  }
  d  <- s$details %||% list()
  ht <- s$homeTeam$info %||% list()
  vt <- s$visitingTeam$info %||% list()
  hs <- s$homeTeam$stats %||% list()
  vs <- s$visitingTeam$stats %||% list()
  data.frame(
    game_id         = as.integer(gid),
    game_number     = .as_chr(d$gameNumber),
    game_date       = .as_chr(d$date),
    game_date_iso   = .as_chr(d$GameDateISO8601),
    start_time      = .as_chr(d$startTime),
    end_time        = .as_chr(d$endTime),
    game_duration   = .as_chr(d$duration),
    game_venue      = .as_chr(d$venue),
    attendance      = .as_int(d$attendance, 0L),
    game_status     = .as_chr(d$status),
    game_season_id  = .as_int(d$seasonId),
    started         = .as_int(d$started, 0L),
    final           = .as_int(d$final, 0L),
    home_team_id    = .as_int(ht$id),
    home_team       = .as_chr(ht$name),
    home_team_abbr  = .as_chr(ht$abbreviation),
    home_score      = .as_int(hs$goals, 0L),
    away_team_id    = .as_int(vt$id),
    away_team       = .as_chr(vt$name),
    away_team_abbr  = .as_chr(vt$abbreviation),
    away_score      = .as_int(vs$goals, 0L),
    has_shootout    = .as_int(s$hasShootout, 0L),
    game_report_url = .as_chr(d$gameReportUrl),
    boxscore_url    = .as_chr(d$textBoxscoreUrl),
    stringsAsFactors = FALSE
  )
}

.parse_team_box <- function(raw, gid) {
  s <- raw$summary_raw
  if (is.null(s)) return(NULL)

  one <- function(side, side_label) {
    info  <- side$info  %||% list()
    stats <- side$stats %||% list()
    rec   <- side$seasonStats$teamRecord %||% list()
    data.frame(
      game_id              = as.integer(gid),
      team_id              = .as_int(info$id),
      team                 = .as_chr(info$name),
      team_abbr            = .as_chr(info$abbreviation),
      team_side            = side_label,
      shots                = .as_int(stats$shots, 0L),
      goals                = .as_int(stats$goals, 0L),
      hits                 = .as_int(stats$hits, 0L),
      pp_goals             = .as_int(stats$powerPlayGoals, 0L),
      pp_opportunities     = .as_int(stats$powerPlayOpportunities, 0L),
      goal_count           = .as_int(stats$goalCount, 0L),
      assist_count         = .as_int(stats$assistCount, 0L),
      penalty_minutes      = .as_int(stats$penaltyMinuteCount, 0L),
      infraction_count     = .as_int(stats$infractionCount, 0L),
      faceoff_attempts     = .as_int(stats$faceoffAttempts, 0L),
      faceoff_wins         = .as_int(stats$faceoffWins, 0L),
      faceoff_win_pct      = .as_num(stats$faceoffWinPercentage),
      season_wins          = .as_int(rec$wins, 0L),
      season_losses        = .as_int(rec$losses, 0L),
      season_ot_wins       = .as_int(rec$OTWins, 0L),
      season_ot_losses     = .as_int(rec$OTLosses, 0L),
      season_so_losses     = .as_int(rec$SOLosses, 0L),
      season_record        = .as_chr(rec$formattedRecord),
      stringsAsFactors     = FALSE
    )
  }

  rbind(
    one(s$homeTeam,     "home"),
    one(s$visitingTeam, "away")
  )
}

.parse_scoring_summary <- function(raw, gid) {
  s <- raw$summary_raw
  periods <- s$periods
  if (is.null(periods) || length(periods) == 0) return(NULL)

  rows <- list()
  for (p in periods) {
    pinfo <- p$info %||% list()
    goals <- p$goals
    if (is.null(goals) || length(goals) == 0) next
    for (g in goals) {
      team   <- g$team        %||% list()
      scorer <- g$scoredBy    %||% list()
      props  <- g$properties  %||% list()
      assists <- g$assists    %||% list()
      a1 <- if (length(assists) >= 1) assists[[1]] else list()
      a2 <- if (length(assists) >= 2) assists[[2]] else list()
      rows[[length(rows) + 1]] <- data.frame(
        game_id            = as.integer(gid),
        period_id          = .as_int(pinfo$id),
        period             = .as_chr(pinfo$longName),
        time               = .as_chr(g$time),
        team_id            = .as_int(team$id),
        team               = .as_chr(team$name),
        team_abbr          = .as_chr(team$abbreviation),
        game_goal_id       = .as_int(g$game_goal_id),
        scorer_goal_number = .as_int(g$scorerGoalNumber),
        scorer_id          = .as_int(scorer$id),
        scorer_first       = .as_chr(scorer$firstName),
        scorer_last        = .as_chr(scorer$lastName),
        scorer_position    = .as_chr(scorer$position),
        assist_1_id        = .as_int(a1$id),
        assist_1_first     = .as_chr(a1$firstName),
        assist_1_last      = .as_chr(a1$lastName),
        assist_2_id        = .as_int(a2$id),
        assist_2_first     = .as_chr(a2$firstName),
        assist_2_last      = .as_chr(a2$lastName),
        is_power_play      = .as_int(props$isPowerPlay, 0L),
        is_short_handed    = .as_int(props$isShortHanded, 0L),
        is_empty_net       = .as_int(props$isEmptyNet, 0L),
        is_penalty_shot    = .as_int(props$isPenaltyShot, 0L),
        is_insurance       = .as_int(props$isInsuranceGoal, 0L),
        is_game_winning    = .as_int(props$isGameWinningGoal, 0L),
        x_location         = .as_num(g$xLocation),
        y_location         = .as_num(g$yLocation),
        stringsAsFactors   = FALSE
      )
    }
  }
  if (length(rows) == 0) return(NULL)
  do.call(rbind, rows)
}

.parse_penalty_summary <- function(raw, gid) {
  s <- raw$summary_raw
  periods <- s$periods
  if (is.null(periods) || length(periods) == 0) return(NULL)

  rows <- list()
  for (p in periods) {
    pinfo    <- p$info %||% list()
    penalties <- p$penalties
    if (is.null(penalties) || length(penalties) == 0) next
    for (pen in penalties) {
      against  <- pen$againstTeam %||% list()
      taken    <- pen$takenBy     %||% list()
      served   <- pen$servedBy    %||% list()
      rows[[length(rows) + 1]] <- data.frame(
        game_id           = as.integer(gid),
        period_id         = .as_int(pinfo$id),
        period            = .as_chr(pinfo$longName),
        time              = .as_chr(pen$time),
        team_id           = .as_int(against$id),
        team              = .as_chr(against$name),
        team_abbr         = .as_chr(against$abbreviation),
        game_penalty_id   = .as_int(pen$game_penalty_id),
        minutes           = .as_num(pen$minutes),
        description       = .as_chr(pen$description),
        rule_number       = .as_chr(pen$ruleNumber),
        is_power_play     = as.integer(isTRUE(pen$isPowerPlay)),
        is_bench          = as.integer(isTRUE(pen$isBench)),
        taken_by_id       = .as_int(taken$id),
        taken_by_first    = .as_chr(taken$firstName),
        taken_by_last     = .as_chr(taken$lastName),
        taken_by_position = .as_chr(taken$position),
        served_by_id      = .as_int(served$id),
        served_by_first   = .as_chr(served$firstName),
        served_by_last    = .as_chr(served$lastName),
        stringsAsFactors  = FALSE
      )
    }
  }
  if (length(rows) == 0) return(NULL)
  do.call(rbind, rows)
}

.parse_three_stars <- function(raw, gid) {
  s <- raw$summary_raw
  mvps <- s$mostValuablePlayers
  if (is.null(mvps) || length(mvps) == 0) return(NULL)
  rows <- list()
  for (i in seq_along(mvps)) {
    m      <- mvps[[i]]
    team   <- m$team %||% list()
    pinfo  <- m$player$info %||% list()
    pstats <- m$player$stats %||% list()
    rows[[i]] <- data.frame(
      game_id          = as.integer(gid),
      star             = as.integer(i),
      team_id          = .as_int(team$id),
      team             = .as_chr(team$name),
      team_abbr        = .as_chr(team$abbreviation),
      player_id        = .as_int(pinfo$id),
      first_name       = .as_chr(pinfo$firstName),
      last_name        = .as_chr(pinfo$lastName),
      jersey_number    = .as_int(pinfo$jerseyNumber),
      position         = .as_chr(pinfo$position),
      is_goalie        = as.integer(isTRUE(m$isGoalie)),
      is_home          = .as_int(m$homeTeam, 0L),
      goals            = .as_int(pstats$goals, 0L),
      assists          = .as_int(pstats$assists, 0L),
      points           = .as_int(pstats$points, 0L),
      shots            = .as_int(pstats$shots, 0L),
      saves            = .as_int(pstats$saves, 0L),
      shots_against    = .as_int(pstats$shotsAgainst, 0L),
      goals_against    = .as_int(pstats$goalsAgainst, 0L),
      time_on_ice      = .as_chr(pstats$toi %||% pstats$timeOnIce),
      stringsAsFactors = FALSE
    )
  }
  do.call(rbind, rows)
}

.parse_officials <- function(raw, gid) {
  s <- raw$summary_raw
  collect <- function(lst, role) {
    if (is.null(lst) || length(lst) == 0) return(NULL)
    do.call(rbind, lapply(lst, function(o) {
      data.frame(
        game_id          = as.integer(gid),
        role             = role,
        first_name       = .as_chr(o$firstName),
        last_name        = .as_chr(o$lastName),
        jersey_number    = .as_int(o$jerseyNumber),
        official_role    = .as_chr(o$role),
        stringsAsFactors = FALSE
      )
    }))
  }
  out <- rbind(
    collect(s$referees,  "Referee"),
    collect(s$linesmen,  "Linesperson"),
    collect(s$scorekeepers, "Scorekeeper")
  )
  if (is.null(out) || nrow(out) == 0) return(NULL)
  out
}

.parse_shots_by_period <- function(raw, gid) {
  s <- raw$summary_raw
  periods <- s$periods
  if (is.null(periods) || length(periods) == 0) return(NULL)
  rows <- lapply(periods, function(p) {
    pinfo <- p$info %||% list()
    pstat <- p$stats %||% list()
    data.frame(
      game_id          = as.integer(gid),
      period_id        = .as_int(pinfo$id),
      period           = .as_chr(pinfo$longName),
      home_goals       = .as_int(pstat$homeGoals, 0L),
      home_shots       = .as_int(pstat$homeShots, 0L),
      away_goals       = .as_int(pstat$visitingGoals, 0L),
      away_shots       = .as_int(pstat$visitingShots, 0L),
      stringsAsFactors = FALSE
    )
  })
  do.call(rbind, rows)
}

.parse_shootout <- function(raw, gid) {
  s <- raw$summary_raw
  if (is.null(s) || !isTRUE(s$hasShootout)) return(NULL)
  ps <- s$penaltyShots
  if (is.null(ps)) return(NULL)
  collect <- function(lst, side) {
    if (is.null(lst) || length(lst) == 0) return(NULL)
    do.call(rbind, lapply(seq_along(lst), function(i) {
      sh <- lst[[i]]
      shooter <- sh$shooter %||% sh$player %||% list()
      goalie  <- sh$goalie %||% list()
      data.frame(
        game_id          = as.integer(gid),
        round            = as.integer(i),
        team_side        = side,
        shooter_id       = .as_int(shooter$id),
        shooter_first    = .as_chr(shooter$firstName),
        shooter_last     = .as_chr(shooter$lastName),
        goalie_id        = .as_int(goalie$id),
        goalie_first     = .as_chr(goalie$firstName),
        goalie_last      = .as_chr(goalie$lastName),
        is_goal          = as.integer(isTRUE(sh$isGoal)),
        stringsAsFactors = FALSE
      )
    }))
  }
  out <- rbind(
    collect(ps$homeTeam,     "home"),
    collect(ps$visitingTeam, "away")
  )
  if (is.null(out) || nrow(out) == 0) return(NULL)
  out
}

.parse_game_rosters <- function(raw, gid) {
  s <- raw$summary_raw
  if (is.null(s)) return(NULL)

  side <- function(team, side_label) {
    info <- team$info %||% list()
    team_id <- .as_int(info$id)
    team_nm <- .as_chr(info$name)
    team_ab <- .as_chr(info$abbreviation)

    one <- function(p, kind) {
      pinfo  <- p$info  %||% list()
      data.frame(
        game_id          = as.integer(gid),
        team_id          = team_id,
        team             = team_nm,
        team_abbr        = team_ab,
        team_side        = side_label,
        player_type      = kind,
        player_id        = .as_int(pinfo$id),
        first_name       = .as_chr(pinfo$firstName),
        last_name        = .as_chr(pinfo$lastName),
        jersey_number    = .as_int(pinfo$jerseyNumber),
        position         = .as_chr(pinfo$position),
        birth_date       = .as_chr(pinfo$birthDate),
        starting         = .as_int(p$starting, 0L),
        status           = .as_chr(p$status),
        stringsAsFactors = FALSE
      )
    }

    rbind(
      if (length(team$skaters) > 0) do.call(rbind, lapply(team$skaters, one, kind = "skater")) else NULL,
      if (length(team$goalies) > 0) do.call(rbind, lapply(team$goalies, one, kind = "goalie")) else NULL
    )
  }

  out <- rbind(
    side(s$homeTeam,     "home"),
    side(s$visitingTeam, "away")
  )
  if (is.null(out) || nrow(out) == 0) return(NULL)
  out
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

  # ── Flat datasets parsed from raw_data (no extra API calls) ──
  # game_info, team_box, scoring_summary, penalty_summary, three_stars,
  # officials, shots_by_period, shootout_summary, game_rosters
  tryCatch(
    {
      gi <- .parse_game_info(raw_data, gid)
      if (is.data.frame(gi) && nrow(gi) > 0) final$game_info <- gi
    },
    error = function(e) cli::cli_alert_warning("game_info parse failed for {gid}: {conditionMessage(e)}")
  )

  tryCatch(
    {
      tb <- .parse_team_box(raw_data, gid)
      if (is.data.frame(tb) && nrow(tb) > 0) final$team_box <- tb
    },
    error = function(e) cli::cli_alert_warning("team_box parse failed for {gid}: {conditionMessage(e)}")
  )

  tryCatch(
    {
      sc <- .parse_scoring_summary(raw_data, gid)
      if (is.data.frame(sc) && nrow(sc) > 0) final$scoring_summary <- sc
    },
    error = function(e) cli::cli_alert_warning("scoring_summary parse failed for {gid}: {conditionMessage(e)}")
  )

  tryCatch(
    {
      pn <- .parse_penalty_summary(raw_data, gid)
      if (is.data.frame(pn) && nrow(pn) > 0) final$penalty_summary <- pn
    },
    error = function(e) cli::cli_alert_warning("penalty_summary parse failed for {gid}: {conditionMessage(e)}")
  )

  tryCatch(
    {
      ts <- .parse_three_stars(raw_data, gid)
      if (is.data.frame(ts) && nrow(ts) > 0) final$three_stars <- ts
    },
    error = function(e) cli::cli_alert_warning("three_stars parse failed for {gid}: {conditionMessage(e)}")
  )

  tryCatch(
    {
      of <- .parse_officials(raw_data, gid)
      if (is.data.frame(of) && nrow(of) > 0) final$officials <- of
    },
    error = function(e) cli::cli_alert_warning("officials parse failed for {gid}: {conditionMessage(e)}")
  )

  tryCatch(
    {
      sp <- .parse_shots_by_period(raw_data, gid)
      if (is.data.frame(sp) && nrow(sp) > 0) final$shots_by_period <- sp
    },
    error = function(e) cli::cli_alert_warning("shots_by_period parse failed for {gid}: {conditionMessage(e)}")
  )

  tryCatch(
    {
      so <- .parse_shootout(raw_data, gid)
      if (is.data.frame(so) && nrow(so) > 0) final$shootout_summary <- so
    },
    error = function(e) cli::cli_alert_warning("shootout_summary parse failed for {gid}: {conditionMessage(e)}")
  )

  tryCatch(
    {
      gr <- .parse_game_rosters(raw_data, gid)
      if (is.data.frame(gr) && nrow(gr) > 0) final$game_rosters <- gr
    },
    error = function(e) cli::cli_alert_warning("game_rosters parse failed for {gid}: {conditionMessage(e)}")
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
cli::cli_alert_info("=== PWHL Raw Scraper complete ===")
cli::cli_h1("All done!")
