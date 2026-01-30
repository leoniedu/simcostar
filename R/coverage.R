# Coverage tracking -------------------------------------------------------

#' Get existing coverage intervals for a buoy + endpoint
#' @noRd
.simcosta_get_coverage <- function(con, boia_id, endpoint) {
  DBI::dbGetQuery(
    con,
    "SELECT start_ts, end_ts FROM coverage
     WHERE boia_id = ? AND endpoint = ?",
    params = list(boia_id, endpoint)
  )
}

#' Find time ranges not yet covered by the cache
#'
#' @param existing Data frame with `start_ts` and `end_ts` columns.
#' @param start Integer Unix timestamp.
#' @param end Integer Unix timestamp.
#' @return `data.table` with `start` and `end` columns.
#' @noRd
.simcosta_missing_ranges <- function(existing, start, end) {
  if (!nrow(existing)) {
    return(data.table::data.table(start = start, end = end))
  }

  existing <- data.table::as.data.table(existing)
  data.table::setorder(existing, start_ts)

  out <- vector("list", nrow(existing) + 1L)
  n <- 0L
  cursor <- start

  for (i in seq_len(nrow(existing))) {
    s <- existing$start_ts[i]
    e <- existing$end_ts[i]

    if (s > cursor) {
      n <- n + 1L
      out[[n]] <- data.table::data.table(start = cursor, end = s - 1L)
    }

    cursor <- max(cursor, e + 1L)
  }

  if (cursor <= end) {
    n <- n + 1L
    out[[n]] <- data.table::data.table(start = cursor, end = end)
  }

  data.table::rbindlist(out[seq_len(n)])
}

#' Record a downloaded range in the coverage table
#' @noRd
.simcosta_register_coverage <- function(con, boia_id, endpoint, start_ts,
                                        end_ts) {
  DBI::dbExecute(
    con,
    "INSERT INTO coverage (boia_id, endpoint, start_ts, end_ts, downloaded_at)
     VALUES (?, ?, ?, ?, ?)",
    params = list(boia_id, endpoint, start_ts, end_ts, as.integer(Sys.time()))
  )
}
