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
#' Requests are retried up to 3 times on transient failures.
#'
#' @param boia_id Integer buoy identifier.
#' @param start Start of the time window. Accepts `POSIXct`, `Date`,
#'   `character` (parsed as UTC), or `numeric` Unix timestamp.
#' @param end End of the time window (same types as `start`).
#' @param endpoint Character vector of endpoints to fetch. One or both of
#'   `"standard"` and `"currents"`. Defaults to both.
#' @param use_cache If `TRUE` (default), use the local SQLite cache. If `FALSE`,
#'   always fetch from the API and do not read from or write to the cache.
#' @param wide If `TRUE` (default), return data in wide format with friendly
#'   column names via [simcosta_wide()]. If `FALSE`, return long format with
#'   columns `boia_id`, `time`, `variable`, `value`.
#' @param verbose If `TRUE` (default), print progress messages.
#'
#' @return A `data.frame`. If `wide = TRUE`, one column per variable with
#'   descriptive names and a `datetime` POSIXct column. If `wide = FALSE`,
#'   long format with columns `boia_id`, `time`, `variable`, `value`.
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
  wide = TRUE,
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
    out <- .simcosta_fetch_nocache(boia_id, start, end, endpoint, verbose)
    if (wide) out <- simcosta_wide(out)
    return(out)
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
  out <- DBI::dbGetQuery(
    con,
    paste0(
      "SELECT boia_id, time, variable, value FROM observations
       WHERE boia_id = ? AND endpoint IN (", endpoint_sql, ")
       AND time BETWEEN ? AND ?"
    ),
    params = list(boia_id, start, end)
  )

  if (wide) out <- simcosta_wide(out)
  out
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


# Wide format -------------------------------------------------------------

#' Variable name mapping from API names to friendly snake_case
#' @noRd
.simcosta_var_map <- function() {
  c(
    # Wave height
    Havg             = "wave_height",
    Hsig             = "wave_height_sig",
    HM0              = "wave_height_m0",
    Hmax             = "wave_height_max",
    H10              = "wave_height_10",
    # Wave period
    Tavg             = "wave_period",
    T10              = "wave_period_10",
    Tsig             = "wave_period_sig",
    Tp               = "wave_peak_period",
    Tp5              = "wave_peak_period_5",
    ZCN              = "zero_crossing_count",
    # Wave direction
    Avg_Wv_Dir_N     = "wave_direction",
    Avg_Wv_Spread_N  = "wave_spread",
    # Wind / air
    Avg_Wnd_Dir_N    = "wind_direction",
    Avg_Wnd_Sp       = "wind_speed",
    Avg_Air_Tmp      = "air_temperature",
    # Water quality
    Avg_Sal          = "salinity",
    Avg_W_Tmp1       = "water_temp_1",
    Avg_W_Tmp2       = "water_temp_2",
    Avg_Chl          = "chlorophyll",
    Avg_DO           = "dissolved_oxygen",
    Avg_Turb         = "turbidity",
    Avg_CDOM         = "cdom",
    # Currents
    C_Avg_Spd        = "current_speed_kmh",
    C_Avg_Dir_N      = "current_direction",
    C_Cell_2_North_Speed = "current_cell2_north_speed",
    tidbits_temp     = "tidbit_temperature"
  )
}

#' Unit conversions applied after pivoting to wide format
#' @noRd
.simcosta_unit_conversions <- function() {
  list(
    # C_Avg_Spd is in mm/s; convert to km/h
    C_Avg_Spd = function(x) x * 3.6e-3
  )
}

#' Reshape SIMCOSTA data to wide format with friendly names
#'
#' Converts the long-format output of [simcosta_fetch()] to a wide
#' data frame with one column per variable, human-readable column names,
#' and a POSIXct `datetime` column.
#'
#' @param data A data frame returned by [simcosta_fetch()], with columns
#'   `boia_id`, `time`, `variable`, `value`.
#'
#' @return A `data.frame` in wide format with a `datetime` column (POSIXct,
#'   UTC) and one column per variable using descriptive snake_case names.
#'   Variables without a known mapping keep their original API name.
#' @export
#' @examples
#' \dontrun{
#' simcosta_fetch(515, "2025-01-01", "2025-01-02") |>
#'   simcosta_wide()
#' }
simcosta_wide <- function(data) {
  if (!nrow(data)) {
    return(data.frame(boia_id = integer(), datetime = as.POSIXct(character())))
  }

  dt <- data.table::as.data.table(data)

  # Apply unit conversions before pivoting
  conversions <- .simcosta_unit_conversions()
  for (var in names(conversions)) {
    dt[variable == var, value := conversions[[var]](value)]
  }

  # Pivot to wide
  wide <- data.table::dcast(dt, boia_id + time ~ variable, value.var = "value")

  # Convert time to POSIXct datetime
  wide[, datetime := as.POSIXct(time, origin = "1970-01-01", tz = "UTC")]
  wide[, time := NULL]

  # Rename columns using mapping
  var_map <- .simcosta_var_map()

  # Handle dynamic direction_cell_N -> current_dir_cell_N
  dir_cols <- grep("^direction_cell_", names(wide), value = TRUE)
  for (col in dir_cols) {
    n <- sub("^direction_cell_", "", col)
    var_map[[col]] <- paste0("current_dir_cell_", n)
  }

  old_names <- names(wide)
  new_names <- ifelse(
    old_names %in% names(var_map),
    var_map[old_names],
    old_names
  )
  data.table::setnames(wide, old_names, new_names)

  # Reorder: boia_id, datetime first
  front <- c("boia_id", "datetime")
  rest <- setdiff(names(wide), front)
  data.table::setcolorder(wide, c(front, sort(rest)))

  as.data.frame(wide)
}
