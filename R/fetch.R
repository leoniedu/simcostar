#' Download and cache SIMCOSTA buoy data
#'
#' Downloads oceanographic and meteorological buoy data from the SIMCOSTA
#' platform and stores the results in a local SQLite cache.
#' Repeated calls only fetch time windows not yet in the cache.
#'
#' Two endpoints are supported:
#' - **standard**: in-transit variables (wave height, water temperature,
#'   salinity, chlorophyll, etc.)
#' - **currents**: current profiles with direction
#'
#' @section API configuration:
#'
#' The base URL defaults to `https://simcosta.furg.br/api` and can be
#' overridden via `options(simcostar.api_url = "...")` or the
#' `SIMCOSTA_API_URL` environment variable.
#'
#' Requests are rate-limited to 1 per second by default (configurable via
#' `options(simcostar.rate_limit)`) and retried up to 3 times on transient
#' failures.
#'
#' @param boia_id Integer buoy identifier.
#' @param start Start of the time window. Accepts `POSIXct`, `Date`,
#'   `character` (parsed as UTC), or `numeric` Unix timestamp.
#' @param end End of the time window (same types as `start`).
#' @param endpoint Character vector of endpoints to fetch. One or both of
#'   `"standard"` and `"currents"`. Defaults to both.
#' @param verbose If `TRUE` (default), print progress messages.
#'
#' @return A named list with elements `standard` and/or `currents`,
#'   each a `data.frame`.
#' @export
#' @examples
#' \dontrun{
#' res <- simcosta_fetch(
#'   boia_id = 515,
#'   start   = "2025-01-01",
#'   end     = "2025-01-02"
#' )
#' head(res$standard)
#' head(res$currents)
#' }
simcosta_fetch <- function(
    boia_id,
    start,
    end,
    endpoint = c("standard", "currents"),
    verbose = TRUE) {
  boia_id <- as.integer(boia_id)
  start <- .as_unix(start)
  end <- .as_unix(end)
  endpoint <- match.arg(endpoint, several.ok = TRUE)

  if (end < start) {
    cli::cli_abort("{.arg end} must be >= {.arg start}.")
  }

  con <- .simcosta_db_connect()
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  out <- list()

  if ("standard" %in% endpoint) {
    out$standard <- .simcosta_fetch_standard(
      con, boia_id, start, end, verbose
    )
  }

  if ("currents" %in% endpoint) {
    out$currents <- .simcosta_fetch_currents(
      con, boia_id, start, end, verbose
    )
  }

  out
}


# Time conversion ---------------------------------------------------------

#' Convert various time representations to integer Unix timestamp
#' @noRd
.as_unix <- function(x) {
  if (inherits(x, "POSIXt")) return(as.integer(x))
  if (inherits(x, "Date")) return(as.integer(as.POSIXct(x, tz = "UTC")))
  if (is.character(x)) return(as.integer(as.POSIXct(x, tz = "UTC")))
  if (is.numeric(x)) return(as.integer(x))
  cli::cli_abort(
    "{.arg start}/{.arg end} must be POSIXct, Date, character, or numeric."
  )
}


# Standard endpoint -------------------------------------------------------

#' @noRd
.simcosta_fetch_standard <- function(con, boia_id, start, end, verbose) {
  existing <- .simcosta_get_coverage(con, boia_id, "standard")
  miss <- .simcosta_missing_ranges(existing, start, end)

  if (nrow(miss)) {
    for (i in seq_len(nrow(miss))) {
      s <- miss$start[i]
      e <- miss$end[i]

      if (verbose) {
        cli::cli_inform(
          "Downloading standard data for buoy {boia_id}: {s} \u2013 {e}"
        )
      }

      raw <- tryCatch(
        .simcosta_api_get(
          "intrans_data",
          boiaID = boia_id,
          type   = "json",
          time1  = s,
          time2  = e,
          params = paste(.simcosta_standard_params(), collapse = ",")
        ),
        error = function(err) {
          cli::cli_warn(
            "API request failed for range {s}\u2013{e}: {conditionMessage(err)}"
          )
          NULL
        }
      )

      # Register coverage for the requested range so we don't re-try
      .simcosta_register_coverage(con, boia_id, "standard", s, e)

      if (is.null(raw) || !length(raw)) next

      dt <- data.table::as.data.table(raw)

      if (!nrow(dt) || !"time" %in% names(dt)) next

      long <- data.table::melt(
        dt,
        id.vars       = "time",
        variable.name = "variable",
        value.name    = "value",
        measure.vars  = setdiff(names(dt), "time")
      )

      long[, boia_id := boia_id]

      DBI::dbWriteTable(
        con,
        "standard_data",
        long[, list(boia_id, time, variable, value)],
        append = TRUE
      )
    }
  }

  DBI::dbGetQuery(
    con,
    "SELECT * FROM standard_data
     WHERE boia_id = ? AND time BETWEEN ? AND ?",
    params = list(boia_id, start, end)
  )
}


# Currents endpoint -------------------------------------------------------

#' @noRd
.simcosta_fetch_currents <- function(con, boia_id, start, end, verbose) {
  existing <- .simcosta_get_coverage(con, boia_id, "currents")
  miss <- .simcosta_missing_ranges(existing, start, end)

  if (nrow(miss)) {
    for (i in seq_len(nrow(miss))) {
      s <- miss$start[i]
      e <- miss$end[i]

      if (verbose) {
        cli::cli_inform(
          "Downloading currents for buoy {boia_id}: {s} \u2013 {e}"
        )
      }

      raw <- tryCatch(
        .simcosta_api_get(
          "intrans_data",
          boiaID = boia_id,
          type   = "json",
          time1  = s,
          time2  = e,
          params = "perfil_correntes",
          extras = "dir_n"
        ),
        error = function(err) {
          cli::cli_warn(
            "API request failed for range {s}\u2013{e}: {conditionMessage(err)}"
          )
          NULL
        }
      )

      .simcosta_register_coverage(con, boia_id, "currents", s, e)

      if (is.null(raw) || !length(raw)) next

      dt <- data.table::as.data.table(raw)

      if (!nrow(dt)) next

      required <- c("time", "depth", "speed", "direction")
      if (!all(required %in% names(dt))) next

      dt[, boia_id := boia_id]

      DBI::dbWriteTable(
        con,
        "currents_data",
        dt[, list(boia_id, time, depth, speed, direction)],
        append = TRUE
      )
    }
  }

  DBI::dbGetQuery(
    con,
    "SELECT * FROM currents_data
     WHERE boia_id = ? AND time BETWEEN ? AND ?",
    params = list(boia_id, start, end)
  )
}
