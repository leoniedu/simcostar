# Database helpers --------------------------------------------------------

#' Connect to the cache database and initialise tables
#' @noRd
.simcosta_db_connect <- function() {
  path <- .simcosta_db_path()
  con <- DBI::dbConnect(RSQLite::SQLite(), path)
  .simcosta_db_init(con)
  con
}

#' Create cache tables if they don't exist
#' @noRd
.simcosta_db_init <- function(con) {
  DBI::dbExecute(con, "
    CREATE TABLE IF NOT EXISTS observations (
      boia_id   INTEGER,
      time      INTEGER,
      endpoint  TEXT,
      variable  TEXT,
      value     REAL,
      PRIMARY KEY (boia_id, time, endpoint, variable)
    )
  ")

  DBI::dbExecute(con, "
    CREATE TABLE IF NOT EXISTS coverage (
      boia_id       INTEGER,
      endpoint      TEXT,
      start_ts      INTEGER,
      end_ts        INTEGER,
      downloaded_at INTEGER
    )
  ")

  invisible(TRUE)
}
