#' Download and cache SIMCOSTA buoy data
#'
#' Downloads oceanographic and meteorological buoy data from the SIMCOSTA
#' platform and stores the results in a local SQLite cache.
#' Repeated calls only fetch time windows not yet in the cache.
#'
#' All data is returned in long format with columns `boia_id`, `time`,
#' `variable`, and `value`. Standard variables include wave height, water
#' temperature, salinity, chlorophyll, etc. Current-direction profiles are
#' stored as `direction_cell_1`, `direction_cell_2`, etc.
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
#' @param use_cache If `TRUE` (default), use the local SQLite cache. If `FALSE`,
#'   always fetch from the API and do not read from or write to the cache.
#' @param verbose If `TRUE` (default), print progress messages.
#'
#' @return A `data.frame` in long format with columns `boia_id`, `time`,
#'   `variable`, and `value`.
#' @export
#' @examples
#' \dontrun{
#' res <- simcosta_fetch(
#'   boia_id = 515,
#'   start   = "2025-01-01",
#'   end     = "2025-01-02"
#' )
#' head(res)
#' }
simcosta_fetch <- function(
  boia_id,
  start,
  end,
  endpoint = c("standard", "currents"),
  use_cache = TRUE,
  verbose = TRUE
) {
  boia_id <- as.integer(boia_id)
  start <- .as_unix(start)
  end <- .as_unix(end)
  endpoint <- match.arg(endpoint, several.ok = TRUE)

  if (end < start) {
    cli::cli_abort("{.arg end} must be >= {.arg start}.")
  }

  if (!use_cache) {
    return(.simcosta_fetch_nocache(boia_id, start, end, endpoint, verbose))
  }

  con <- .simcosta_db_connect()
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  if ("standard" %in% endpoint) {
    .simcosta_download_standard(con, boia_id, start, end, verbose)
  }

  if ("currents" %in% endpoint) {
    .simcosta_download_currents(con, boia_id, start, end, verbose)
  }

  endpoint_sql <- paste0("'", endpoint, "'", collapse = ", ")
  DBI::dbGetQuery(
    con,
    paste0(
      "SELECT boia_id, time, variable, value FROM observations
       WHERE boia_id = ? AND endpoint IN (", endpoint_sql, ")
       AND time BETWEEN ? AND ?"
    ),
    params = list(boia_id, start, end)
  )
}


# Time conversion ---------------------------------------------------------

#' Convert various time representations to integer Unix timestamp
#' @noRd
.as_unix <- function(x) {
  if (inherits(x, "POSIXt")) {
    return(as.integer(x))
  }
  if (inherits(x, "Date")) {
    return(as.integer(as.POSIXct(x, tz = "UTC")))
  }
  if (is.character(x)) {
    return(as.integer(as.POSIXct(x, tz = "UTC")))
  }
  if (is.numeric(x)) {
    return(as.integer(x))
  }
  cli::cli_abort(
    "{.arg start}/{.arg end} must be POSIXct, Date, character, or numeric."
  )
}


# Standard endpoint -------------------------------------------------------

#' Columns returned by the API that are not measurement variables
#' @noRd
.simcosta_meta_cols <- function() {
  c("timestamp", "YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND")
}

#' Parse ISO 8601 timestamp to integer Unix time
#' @noRd
.parse_timestamp <- function(x) {
  as.integer(as.POSIXct(
    sub("Z$", "", x),
    format = "%Y-%m-%dT%H:%M:%OS",
    tz = "UTC"
  ))
}

#' @noRd
.simcosta_download_standard <- function(con, boia_id, start, end, verbose) {
  existing <- .simcosta_get_coverage(con, boia_id, "standard")
  miss <- .simcosta_missing_ranges(existing, start, end)

  if (!nrow(miss)) return(invisible())

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
        type = "json",
        time1 = s,
        time2 = e,
        params = paste(.simcosta_standard_params(), collapse = ",")
      ),
      error = function(err) {
        cli::cli_warn(
          "API request failed for range {s}\u2013{e}: {conditionMessage(err)}"
        )
        NULL
      }
    )

    if (is.null(raw) || !length(raw)) {
      next
    }

    dt <- data.table::as.data.table(raw)

    if (!nrow(dt) || !"timestamp" %in% names(dt)) {
      next
    }

    # Convert ISO 8601 timestamp to Unix time
    dt[, time := .parse_timestamp(timestamp)]

    # Melt only measurement columns (exclude timestamp metadata)
    measure_cols <- setdiff(names(dt), c(.simcosta_meta_cols(), "time"))
    if (!length(measure_cols)) {
      next
    }

    # Coerce all measure columns to numeric (API may return character for
    # all-NA columns where jsonlite can't infer type)
    for (col in measure_cols) {
      data.table::set(dt, j = col, value = as.numeric(dt[[col]]))
    }

    long <- data.table::melt(
      dt,
      id.vars = "time",
      variable.name = "variable",
      value.name = "value",
      measure.vars = measure_cols
    )

    long[, boia_id := boia_id]
    long[, endpoint := "standard"]

    DBI::dbWriteTable(
      con,
      "observations",
      long[, list(boia_id, time, endpoint, variable, value)],
      append = TRUE
    )

    .simcosta_register_coverage(con, boia_id, "standard", s, e)
  }

  invisible()
}


# Currents endpoint -------------------------------------------------------

#' @noRd
.simcosta_download_currents <- function(con, boia_id, start, end, verbose) {
  existing <- .simcosta_get_coverage(con, boia_id, "currents")
  miss <- .simcosta_missing_ranges(existing, start, end)

  if (!nrow(miss)) return(invisible())

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
        type = "json",
        time1 = s,
        time2 = e,
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

    if (is.null(raw) || !length(raw)) {
      next
    }

    dt <- data.table::as.data.table(raw)

    if (!nrow(dt) || !"timestamp" %in% names(dt)) {
      next
    }

    dt[, time := .parse_timestamp(timestamp)]

    # API returns wide-format: Avg_Cell(NNN)_dir_n columns
    dir_cols <- grep("^Avg_Cell\\(\\d+\\)_dir_n$", names(dt), value = TRUE)
    if (!length(dir_cols)) {
      next
    }

    long <- data.table::melt(
      dt,
      id.vars = "time",
      variable.name = "cell_name",
      value.name = "direction",
      measure.vars = dir_cols
    )

    # Extract cell number and encode as variable name
    long[, variable := paste0(
      "direction_cell_",
      sub(".*\\((\\d+)\\).*", "\\1", cell_name)
    )]
    long[, cell_name := NULL]
    long[, value := as.numeric(direction)]
    long[, direction := NULL]
    long[, boia_id := boia_id]
    long[, endpoint := "currents"]

    DBI::dbWriteTable(
      con,
      "observations",
      long[, list(boia_id, time, endpoint, variable, value)],
      append = TRUE
    )

    .simcosta_register_coverage(con, boia_id, "currents", s, e)
  }

  invisible()
}


# No-cache path -----------------------------------------------------------

#' Fetch directly from API without cache
#' @noRd
.simcosta_fetch_nocache <- function(boia_id, start, end, endpoint, verbose) {
  parts <- list()

  if ("standard" %in% endpoint) {
    if (verbose) {
      cli::cli_inform(
        "Downloading standard data for buoy {boia_id}: {start} \u2013 {end}"
      )
    }

    raw <- tryCatch(
      .simcosta_api_get(
        "intrans_data",
        boiaID = boia_id,
        type = "json",
        time1 = start,
        time2 = end,
        params = paste(.simcosta_standard_params(), collapse = ",")
      ),
      error = function(err) {
        cli::cli_warn(
          "API request failed for range {start}\u2013{end}: {conditionMessage(err)}"
        )
        NULL
      }
    )

    if (!is.null(raw) && length(raw)) {
      dt <- data.table::as.data.table(raw)
      if (nrow(dt) && "timestamp" %in% names(dt)) {
        dt[, time := .parse_timestamp(timestamp)]
        measure_cols <- setdiff(names(dt), c(.simcosta_meta_cols(), "time"))
        if (length(measure_cols)) {
          for (col in measure_cols) {
            data.table::set(dt, j = col, value = as.numeric(dt[[col]]))
          }
          long <- data.table::melt(
            dt,
            id.vars = "time",
            variable.name = "variable",
            value.name = "value",
            measure.vars = measure_cols
          )
          long[, boia_id := boia_id]
          parts <- c(parts, list(long[, list(boia_id, time, variable, value)]))
        }
      }
    }
  }

  if ("currents" %in% endpoint) {
    if (verbose) {
      cli::cli_inform(
        "Downloading currents for buoy {boia_id}: {start} \u2013 {end}"
      )
    }

    raw <- tryCatch(
      .simcosta_api_get(
        "intrans_data",
        boiaID = boia_id,
        type = "json",
        time1 = start,
        time2 = end,
        params = "perfil_correntes",
        extras = "dir_n"
      ),
      error = function(err) {
        cli::cli_warn(
          "API request failed for range {start}\u2013{end}: {conditionMessage(err)}"
        )
        NULL
      }
    )

    if (!is.null(raw) && length(raw)) {
      dt <- data.table::as.data.table(raw)
      if (nrow(dt) && "timestamp" %in% names(dt)) {
        dt[, time := .parse_timestamp(timestamp)]
        dir_cols <- grep("^Avg_Cell\\(\\d+\\)_dir_n$", names(dt), value = TRUE)
        if (length(dir_cols)) {
          long <- data.table::melt(
            dt,
            id.vars = "time",
            variable.name = "cell_name",
            value.name = "direction",
            measure.vars = dir_cols
          )
          long[, variable := paste0(
            "direction_cell_",
            sub(".*\\((\\d+)\\).*", "\\1", cell_name)
          )]
          long[, cell_name := NULL]
          long[, value := as.numeric(direction)]
          long[, direction := NULL]
          long[, boia_id := boia_id]
          parts <- c(parts, list(long[, list(boia_id, time, variable, value)]))
        }
      }
    }
  }

  if (length(parts)) {
    as.data.frame(data.table::rbindlist(parts))
  } else {
    data.frame(
      boia_id = integer(),
      time = integer(),
      variable = character(),
      value = double()
    )
  }
}
