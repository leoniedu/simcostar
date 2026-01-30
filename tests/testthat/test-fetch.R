# Helper: skip if the SIMCOSTA API is unreachable
skip_if_api_down <- function() {
  res <- tryCatch(
    httr2::request("https://simcosta.furg.br/api/intrans_data") |>
      httr2::req_url_query(
        boiaID = 515, type = "json",
        time1 = 1735689600, time2 = 1735693200,
        params = "Hsig"
      ) |>
      httr2::req_perform(),
    error = function(e) NULL
  )
  if (is.null(res) || httr2::resp_status(res) >= 400) {
    skip("SIMCOSTA API is unavailable")
  }
}

test_that("simcosta_fetch returns data from the API (standard)", {
  skip_if_api_down()
  tmp <- withr::local_tempfile(fileext = ".sqlite")
  local_mocked_bindings(.simcosta_db_path = function() tmp)

  res <- simcosta_fetch(
    boia_id  = 515,
    start    = "2025-01-01",
    end      = "2025-01-02",
    endpoint = "standard",
    wide     = FALSE,
    verbose  = FALSE
  )

  expect_s3_class(res, "data.frame")
  expect_named(res, c("boia_id", "time", "variable", "value"))
  expect_true(nrow(res) > 0)
  expect_true(all(res$boia_id == 515L))
})

test_that("simcosta_fetch returns data from the API (currents)", {
  skip_if_api_down()
  tmp <- withr::local_tempfile(fileext = ".sqlite")
  local_mocked_bindings(.simcosta_db_path = function() tmp)

  res <- simcosta_fetch(
    boia_id  = 515,
    start    = "2025-01-01",
    end      = "2025-01-02",
    endpoint = "currents",
    wide     = FALSE,
    verbose  = FALSE
  )

  expect_s3_class(res, "data.frame")
  expect_named(res, c("boia_id", "time", "variable", "value"))
  if (nrow(res) > 0) {
    expect_true(all(grepl("^direction_cell_", res$variable)))
  }
})

test_that("simcosta_fetch serves cached data on second call", {
  skip_if_api_down()
  tmp <- withr::local_tempfile(fileext = ".sqlite")
  local_mocked_bindings(.simcosta_db_path = function() tmp)

  first <- simcosta_fetch(
    boia_id  = 515,
    start    = "2025-01-01",
    end      = "2025-01-02",
    endpoint = "standard",
    wide     = FALSE,
    verbose  = FALSE
  )

  # Second call should use cache (no API hit for same range)
  second <- simcosta_fetch(
    boia_id  = 515,
    start    = "2025-01-01",
    end      = "2025-01-02",
    endpoint = "standard",
    wide     = FALSE,
    verbose  = FALSE
  )

  expect_equal(first, second)
})

test_that("simcosta_fetch returns both endpoints together", {
  skip_if_api_down()
  tmp <- withr::local_tempfile(fileext = ".sqlite")
  local_mocked_bindings(.simcosta_db_path = function() tmp)

  res <- simcosta_fetch(
    boia_id  = 515,
    start    = "2025-01-01",
    end      = "2025-01-02",
    endpoint = c("standard", "currents"),
    wide     = FALSE,
    verbose  = FALSE
  )

  expect_s3_class(res, "data.frame")
  expect_named(res, c("boia_id", "time", "variable", "value"))
})

test_that("simcosta_fetch works without cache", {
  skip_if_api_down()

  res <- simcosta_fetch(
    boia_id  = 515,
    start    = "2025-01-01",
    end      = "2025-01-02",
    endpoint = "standard",
    use_cache = FALSE,
    wide     = FALSE,
    verbose  = FALSE
  )

  expect_s3_class(res, "data.frame")
  expect_named(res, c("boia_id", "time", "variable", "value"))
  expect_true(nrow(res) > 0)
})

test_that("simcosta_wide reshapes to wide format with friendly names", {
  long <- data.frame(
    boia_id  = rep(1L, 4),
    time     = rep(1735689600L, 4),
    variable = c("Havg", "Avg_Sal", "C_Avg_Spd", "Avg_W_Tmp1"),
    value    = c(1.5, 35.0, 1000, 22.3)
  )

  wide <- simcosta_wide(long)

  expect_s3_class(wide, "data.frame")
  expect_true("datetime" %in% names(wide))
  expect_s3_class(wide$datetime, "POSIXct")
  expect_false("time" %in% names(wide))

  # Check renamed columns
  expect_true("wave_height" %in% names(wide))
  expect_true("salinity" %in% names(wide))
  expect_true("water_temp_1" %in% names(wide))
  expect_true("current_speed_kmh" %in% names(wide))

  # Check unit conversion: 1000 mm/s * 3.6e-3 = 3.6 km/h
  expect_equal(wide$current_speed_kmh, 3.6)
})

test_that("simcosta_wide handles direction_cell_N columns", {
  long <- data.frame(
    boia_id  = rep(1L, 2),
    time     = rep(1735689600L, 2),
    variable = c("direction_cell_1", "direction_cell_2"),
    value    = c(90.0, 180.0)
  )

  wide <- simcosta_wide(long)

  expect_true("current_dir_cell_1" %in% names(wide))
  expect_true("current_dir_cell_2" %in% names(wide))
  expect_equal(wide$current_dir_cell_1, 90.0)
})

test_that("simcosta_wide handles empty input", {
  empty <- data.frame(
    boia_id  = integer(),
    time     = integer(),
    variable = character(),
    value    = double()
  )

  wide <- simcosta_wide(empty)

  expect_s3_class(wide, "data.frame")
  expect_equal(nrow(wide), 0)
})

test_that("simcosta_fetch returns wide format by default", {
  skip_if_api_down()

  wide <- simcosta_fetch(
    boia_id   = 515,
    start     = "2025-01-01",
    end       = "2025-01-02",
    endpoint  = "standard",
    use_cache = FALSE,
    verbose   = FALSE
  )

  expect_s3_class(wide, "data.frame")
  expect_true("datetime" %in% names(wide))
  expect_true(nrow(wide) > 0)
  # Should have at least some friendly names
  expect_true(any(c("wave_height", "salinity", "water_temp_1") %in% names(wide)))
})
