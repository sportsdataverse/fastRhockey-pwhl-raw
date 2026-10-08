## Recompute the derived pbp coordinate columns in every pwhl/json/final/{game_id}.json.
##
## fastRhockey #75 (sdv-internal-refs hockeytech/CANVAS.md, #51) redefined
## x/y_coord_fixed, x/y_coord_right and x/y_coord_vertical as rotations of the feet frame:
## the old arithmetic put home-team events off the rink (home x_coord_right 191-290 ft).
## The inputs (x/y_coord_original, team_id, home_team_id) are stored in each final JSON, so
## the six columns are recomputed by fastRhockey's own hockeytech_add_coord_transforms()
## without re-fetching anything. Nothing else in a file changes: it is read and written
## exactly as scrape_pwhl_raw.R reads and writes it (a no-op round trip is byte-identical).
##
## Usage (from the repo root):
##   Rscript R/recompute_derived_coords.R                    # installed fastRhockey
##   Rscript R/recompute_derived_coords.R <fastRhockey-dir>   # a source checkout (pkgload)
## Requires a fastRhockey that includes #75 (x_coord_fixed == -x for every event).

args <- commandArgs(trailingOnly = TRUE)
if (length(args) >= 1) {
  pkgload::load_all(args[1], quiet = TRUE)
  transform <- get("hockeytech_add_coord_transforms", envir = asNamespace("fastRhockey"))
} else {
  transform <- fastRhockey:::hockeytech_add_coord_transforms
}
probe <- transform(data.frame(x_coord = 60, y_coord = 30, team_id = "1", home_team_id = "1"))
if (!isTRUE(all.equal(probe$x_coord_fixed, 80)) || !isTRUE(all.equal(probe$x_coord_right, 80))) {
  stop("this fastRhockey predates #75: x_coord_fixed must be -x (canvas 60 -> 80)", call. = FALSE)
}

DERIVED <- c("x_coord_fixed", "y_coord_fixed", "x_coord_right", "y_coord_right",
             "x_coord_vertical", "y_coord_vertical")
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
  pbp <- data.frame(
    x_coord = suppressWarnings(as.numeric(field(rows, "x_coord_original"))),
    y_coord = suppressWarnings(as.numeric(field(rows, "y_coord_original"))),
    team_id = field(rows, "team_id"),
    home_team_id = field(rows, "home_team_id"),
    stringsAsFactors = FALSE
  )
  out <- transform(pbp)
  # The stored feet must match a fresh transform of the stored pixels, or the inputs are not
  # what the scraper saw.
  stored_x <- suppressWarnings(as.numeric(field(rows, "x_coord")))
  ok <- !is.na(stored_x) & !is.na(out$x_coord)
  if (any(abs(stored_x[ok] - out$x_coord[ok]) > 1e-3)) {
    stop("stored x_coord disagrees with x_coord_original in ", path, call. = FALSE)
  }
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
