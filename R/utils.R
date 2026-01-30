# Internal helpers --------------------------------------------------------

#' Get the SIMCOSTA API base URL
#'
#' Configurable via `options(simcostar.api_url = "...")` or the
#' `SIMCOSTA_API_URL` environment variable.
#' @noRd
.simcosta_base_url <- function() {
  getOption(
    "simcostar.api_url",
    Sys.getenv("SIMCOSTA_API_URL", "https://simcosta.furg.br/api")
  )
}

#' Cache directory for simcostar
#' @noRd
.simcosta_cache_dir <- function() {
  tools::R_user_dir("simcostar", "cache")
}

#' Path to the SQLite cache database
#' @noRd
.simcosta_db_path <- function() {
  dir <- .simcosta_cache_dir()
  dir.create(dir, recursive = TRUE, showWarnings = FALSE)
  file.path(dir, "simcosta.sqlite")
}

#' Make an API request with retry and rate limiting
#'
#' Requests are throttled (default 1 req/s, configurable via
#' `options(simcostar.rate_limit)`) and retried up to 3 times on transient
#' failures with exponential backoff.
#' @param endpoint API endpoint path (e.g. `"intrans_data"`).
#' @param ... Query parameters forwarded to [httr2::req_url_query()].
#' @return Parsed JSON (list or data.frame).
#' @noRd
.simcosta_api_get <- function(endpoint, ...) {
  url <- paste0(.simcosta_base_url(), "/", endpoint)
  rate <- getOption("simcostar.rate_limit", 1)

  req <- httr2::request(url) |>
    httr2::req_url_query(...) |>
    httr2::req_user_agent("simcostar R package (https://github.com/leoniedu/simcostar)") |>
    httr2::req_retry(max_tries = 3) |>
    httr2::req_throttle(rate = rate)

  resp <- httr2::req_perform(req)
  txt <- httr2::resp_body_string(resp)
  jsonlite::fromJSON(txt, flatten = TRUE)
}

#' Standard variable parameters for the intrans_data endpoint
#' @noRd
.simcosta_standard_params <- function() {
  c(
    "H10", "Havg", "Hsig", "HM0", "Avg_Wv_Dir_N", "Hmax", "ZCN", "Tp5",
    "Tavg", "T10", "Tsig", "Avg_Wv_Spread_N", "Tp", "Avg_Sal",
    "Avg_W_Tmp1", "Avg_W_Tmp2", "Avg_CDOM", "Avg_Chl", "Avg_DO",
    "Avg_Turb", "C_Avg_Dir_N", "tidbits_temp", "C_Avg_Spd",
    "C_Cell_2_North_Speed",
    "Avg_Wnd_Dir_N", "Avg_Wnd_Sp", "Avg_Air_Tmp"
  )
}
