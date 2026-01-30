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
    verbose  = FALSE
  )

  # Second call should use cache (no API hit for same range)
  second <- simcosta_fetch(
    boia_id  = 515,
    start    = "2025-01-01",
    end      = "2025-01-02",
    endpoint = "standard",
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
    verbose  = FALSE
  )

  expect_s3_class(res, "data.frame")
  expect_named(res, c("boia_id", "time", "variable", "value"))
  expect_true(nrow(res) > 0)
})
