## Recompute the derived pbp geometry in every pwhl/json/final/{game_id}.json.
##
## Two fastRhockey fixes change values computed at scrape time (spec: sdv-internal-refs
## hockeytech/CANVAS.md):
##   - #75 (#51): x/y_coord_fixed, x/y_coord_right and x/y_coord_vertical are rotations
##     of the feet frame; the old arithmetic put home-team events off the rink.
##   - #76 (#52): an empty-net goal's shot_distance / shot_angle (and scoring_chance) is
##     measured to the net its team attacks; an own-half one was measured to the nearer net.
## The inputs (x/y_coord_original, team_id, home_team_id, event, empty_net) are stored in
## each final JSON, so these nine columns are recomputed by fastRhockey's own
## hockeytech_add_coord_transforms(), hockeytech_shot_distance_angle() and
## hockeytech_scoring_chances() without re-fetching anything, exactly as
## hockeytech_enrich_pbp() calls them. Nothing else in a file changes: it is read and
## written as scrape_pwhl_raw.R reads and writes it (a no-op round trip is byte-identical).
##
## Usage (from the repo root):
##   Rscript R/recompute_pbp_geometry.R                    # installed fastRhockey
##   Rscript R/recompute_pbp_geometry.R <fastRhockey-dir>   # a source checkout (pkgload)
## Requires a fastRhockey that includes #75 and #76 (checked below).

args <- commandArgs(trailingOnly = TRUE)
if (length(args) >= 1) pkgload::load_all(args[1], quiet = TRUE)
ns <- asNamespace("fastRhockey")
coord_transform <- get("hockeytech_add_coord_transforms", envir = ns)
shot_geometry <- get("hockeytech_shot_distance_angle", envir = ns)
scoring_chances <- get("hockeytech_scoring_chances", envir = ns)

probe <- coord_transform(data.frame(x_coord = 60, y_coord = 30, team_id = "1", home_team_id = "1"))
if (!isTRUE(all.equal(probe$x_coord_fixed, 80)) || !isTRUE(all.equal(probe$x_coord_right, 80))) {
  stop("this fastRhockey predates #75: x_coord_fixed must be -x (canvas 60 -> 80)", call. = FALSE)
}
en <- shot_geometry(data.frame(event = "goal", x_coord = 80, y_coord = 0, team_id = "1",
                               home_team_id = "1", empty_net = "1"))
if (!isTRUE(all.equal(en$shot_distance, 169))) {
  stop("this fastRhockey predates #76: an own-half empty-net goal must measure to the attacking net",
       call. = FALSE)
}

DERIVED <- c("x_coord_fixed", "y_coord_fixed", "x_coord_right", "y_coord_right",
             "x_coord_vertical", "y_coord_vertical", "shot_distance", "shot_angle", "scoring_chance")
field <- function(rows, key) {
  vapply(rows, function(r) {
    v <- r[[key]]
    if (is.null(v) || length(v) != 1L) NA_character_ else as.character(v)
  }, character(1))
}

files <- sort(Sys.glob("pwhl/json/final/*.json"))
changed <- 0L
for (path in files) {
  d <- jsonlite::parse_json(paste(readLines(path, warn = FALSE, encoding = "UTF-8"), collapse = ""),
                            simplifyVector = FALSE)
  rows <- d$pbp
  if (!is.list(rows) || length(rows) == 0L) next
  ox <- suppressWarnings(as.numeric(field(rows, "x_coord_original")))
  oy <- suppressWarnings(as.numeric(field(rows, "y_coord_original")))
  pbp <- data.frame(
    x_coord = ox, y_coord = oy,
    event = field(rows, "event"),
    team_id = field(rows, "team_id"),
    home_team_id = field(rows, "home_team_id"),
    empty_net = field(rows, "empty_net"),
    stringsAsFactors = FALSE
  )
  out <- coord_transform(pbp)
  # The stored feet must match a fresh transform of the stored pixels, or the inputs are
  # not what the scraper saw.
  stored_x <- suppressWarnings(as.numeric(field(rows, "x_coord")))
  ok <- !is.na(stored_x) & !is.na(out$x_coord)
  if (any(abs(stored_x[ok] - out$x_coord[ok]) > 1e-3)) {
    stop("stored x_coord disagrees with x_coord_original in ", path, call. = FALSE)
  }
  # Shot geometry on the feet frame, as hockeytech_enrich_pbp() does it.
  geo <- pbp
  geo$x_coord <- ox / 3 - 100
  geo$y_coord <- 42.5 - (oy * 85 / 300)
  geo <- scoring_chances(shot_geometry(geo))
  out$shot_distance <- geo$shot_distance
  out$shot_angle <- geo$shot_angle
  out$scoring_chance <- geo$scoring_chance
  for (i in seq_along(rows)) {
    for (col in DERIVED) rows[[i]][[col]] <- out[[col]][i]
  }
  d$pbp <- rows
  before <- readBin(path, "raw", file.info(path)$size)
  jsonlite::write_json(d, path = path, auto_unbox = TRUE, null = "null", na = "null")
  if (!identical(before, readBin(path, "raw", file.info(path)$size))) changed <- changed + 1L
}
cat(sprintf("recomputed %s in %d of %d final JSON files\n",
            paste(DERIVED, collapse = ", "), changed, length(files)))
