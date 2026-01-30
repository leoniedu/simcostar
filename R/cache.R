#' Inspect the simcostar cache
#'
#' Returns a summary of cached data including the number of rows per
#' endpoint and coverage intervals.
#'
#' @return Invisibly, a list with elements `db_path`, `db_size_mb`,
#'   `rows`, and `coverage`.
#' @export
#' @examples
#' \dontrun{
#' simcosta_cache_info()
#' }
simcosta_cache_info <- function() {
  path <- .simcosta_db_path()

  if (!file.exists(path)) {
    cli::cli_inform("No cache database found at {.file {path}}.")
    return(invisible(list(db_path = path, db_size_mb = 0)))
  }

  con <- DBI::dbConnect(RSQLite::SQLite(), path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  rows <- DBI::dbGetQuery(
    con,
    "SELECT endpoint, COUNT(*) AS n FROM observations GROUP BY endpoint"
  )
  total <- sum(rows$n)

  cov <- DBI::dbGetQuery(
    con,
    "SELECT * FROM coverage ORDER BY boia_id, endpoint, start_ts"
  )

  size_mb <- round(file.info(path)$size / 1024^2, 2)

  info <- list(
    db_path    = path,
    db_size_mb = size_mb,
    rows       = rows,
    coverage   = cov
  )

  bullets <- c(
    "simcostar cache",
    "*" = "Path: {.file {path}}",
    "*" = "Size: {size_mb} MB",
    "*" = "Total rows: {total}",
    "*" = "Coverage entries: {nrow(cov)}"
  )

  for (i in seq_len(nrow(rows))) {
    ep <- rows$endpoint[i]
    n  <- rows$n[i]
    bullets <- c(bullets, " " = "{ep}: {n} rows")
  }

  cli::cli_inform(bullets)

  invisible(info)
}


#' Clear the simcostar cache
#'
#' Deletes cached data. By default removes all data; optionally filter by
#' buoy ID or endpoint.
#'
#' @param boia_id Optional integer buoy ID. If `NULL` (default), all buoys
#'   are cleared.
#' @param endpoint Optional endpoint name (`"standard"` or `"currents"`).
#'   If `NULL` (default), both endpoints are cleared.
#' @param everything If `TRUE`, delete the entire cache database file.
#'   Overrides `boia_id` and `endpoint`.
#'
#' @return Invisibly, `TRUE`.
#' @export
#' @examples
#' \dontrun{
#' # Clear everything
#' simcosta_clear_cache(everything = TRUE)
#'
#' # Clear only buoy 515 standard data
#' simcosta_clear_cache(boia_id = 515, endpoint = "standard")
#' }
simcosta_clear_cache <- function(boia_id = NULL, endpoint = NULL,
                                 everything = FALSE) {
  path <- .simcosta_db_path()

  if (everything) {
    if (file.exists(path)) {
      file.remove(path)
      cli::cli_inform("Cache database deleted.")
    } else {
      cli::cli_inform("No cache database found.")
    }
    return(invisible(TRUE))
  }

  if (!file.exists(path)) {
    cli::cli_inform("No cache database found.")
    return(invisible(TRUE))
  }

  con <- DBI::dbConnect(RSQLite::SQLite(), path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  where_parts <- character()
  params <- list()

  if (!is.null(boia_id)) {
    where_parts <- c(where_parts, "boia_id = ?")
    params <- c(params, list(as.integer(boia_id)))
  }

  if (!is.null(endpoint)) {
    endpoint <- match.arg(endpoint, c("standard", "currents"))
    where_parts <- c(where_parts, "endpoint = ?")
    params <- c(params, list(endpoint))
  }

  where_sql <- ""
  if (length(where_parts)) {
    where_sql <- paste(" WHERE", paste(where_parts, collapse = " AND "))
  }

  # Same WHERE clause works for both tables (both have boia_id + endpoint)
  DBI::dbExecute(con, paste0("DELETE FROM observations", where_sql),
                 params = params)
  DBI::dbExecute(con, paste0("DELETE FROM coverage", where_sql),
                 params = params)

  cli::cli_inform("Cache cleared.")
  invisible(TRUE)
}
