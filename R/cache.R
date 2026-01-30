#' Inspect the simcostar cache
#'
#' Returns a summary of cached data including the number of rows per table
#' and coverage intervals.
#'
#' @return Invisibly, a list with elements `db_path`, `db_size_mb`,
#'   `standard_rows`, `currents_rows`, and `coverage`.
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

  std_n <- DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM standard_data")$n
  cur_n <- DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM currents_data")$n
  cov <- DBI::dbGetQuery(
    con,
    "SELECT * FROM coverage ORDER BY boia_id, endpoint, start_ts"
  )

  size_mb <- round(file.info(path)$size / 1024^2, 2)

  info <- list(
    db_path       = path,
    db_size_mb    = size_mb,
    standard_rows = std_n,
    currents_rows = cur_n,
    coverage      = cov
  )

  cli::cli_inform(c(
    "simcostar cache",
    "*" = "Path: {.file {path}}",
    "*" = "Size: {size_mb} MB",
    "*" = "Standard rows: {std_n}",
    "*" = "Currents rows: {cur_n}",
    "*" = "Coverage entries: {nrow(cov)}"
  ))

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
  }

  # Clear coverage
  cov_where <- where_parts
  cov_params <- params
  if (!is.null(endpoint)) {
    cov_where <- c(cov_where, "endpoint = ?")
    cov_params <- c(cov_params, list(endpoint))
  }

  cov_sql <- "DELETE FROM coverage"
  if (length(cov_where)) {
    cov_sql <- paste(cov_sql, "WHERE", paste(cov_where, collapse = " AND "))
  }
  DBI::dbExecute(con, cov_sql, params = cov_params)

  # Clear data tables
  tables <- if (is.null(endpoint)) {
    c("standard_data", "currents_data")
  } else {
    paste0(endpoint, "_data")
  }

  for (tbl in tables) {
    sql <- paste("DELETE FROM", tbl)
    if (length(where_parts)) {
      sql <- paste(sql, "WHERE", paste(where_parts, collapse = " AND "))
    }
    DBI::dbExecute(con, sql, params = params)
  }

  cli::cli_inform("Cache cleared.")
  invisible(TRUE)
}
