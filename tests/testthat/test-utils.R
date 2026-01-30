test_that(".as_unix handles POSIXct", {
  ts <- as.POSIXct("2025-01-01 00:00:00", tz = "UTC")
  expect_equal(simcostar:::.as_unix(ts), as.integer(ts))
})

test_that(".as_unix handles Date", {
  d <- as.Date("2025-01-01")
  expected <- as.integer(as.POSIXct(d, tz = "UTC"))
  expect_equal(simcostar:::.as_unix(d), expected)
})

test_that(".as_unix handles character", {
  expected <- as.integer(as.POSIXct("2025-01-01", tz = "UTC"))
  expect_equal(simcostar:::.as_unix("2025-01-01"), expected)
})

test_that(".as_unix handles numeric", {
  expect_equal(simcostar:::.as_unix(1704067200), 1704067200L)
})

test_that(".as_unix rejects bad types", {
  expect_error(simcostar:::.as_unix(TRUE))
})

test_that("simcosta_fetch validates start <= end", {
  expect_error(simcosta_fetch(1, start = 200, end = 100), "end.*start")
})
