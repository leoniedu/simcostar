test_that("missing_ranges returns full range when no coverage", {
  existing <- data.frame(start_ts = integer(), end_ts = integer())
  result <- simcostar:::.simcosta_missing_ranges(existing, 100L, 200L)
  expect_equal(nrow(result), 1)
  expect_equal(result$start, 100L)
  expect_equal(result$end, 200L)
})

test_that("missing_ranges finds gaps around a single interval", {
  existing <- data.frame(start_ts = 120L, end_ts = 150L)
  result <- simcostar:::.simcosta_missing_ranges(existing, 100L, 200L)
  expect_equal(nrow(result), 2)
  expect_equal(result$start, c(100L, 151L))
  expect_equal(result$end, c(119L, 200L))
})

test_that("missing_ranges returns empty when fully covered", {
  existing <- data.frame(start_ts = 100L, end_ts = 200L)
  result <- simcostar:::.simcosta_missing_ranges(existing, 100L, 200L)
  expect_equal(nrow(result), 0)
})

test_that("missing_ranges handles multiple intervals", {
  existing <- data.frame(
    start_ts = c(110L, 160L),
    end_ts   = c(130L, 180L)
  )
  result <- simcostar:::.simcosta_missing_ranges(existing, 100L, 200L)
  expect_equal(nrow(result), 3)
  expect_equal(result$start, c(100L, 131L, 181L))
  expect_equal(result$end, c(109L, 159L, 200L))
})

test_that("missing_ranges handles overlapping existing intervals", {
  existing <- data.frame(
    start_ts = c(100L, 120L),
    end_ts   = c(140L, 160L)
  )
  result <- simcostar:::.simcosta_missing_ranges(existing, 100L, 200L)
  expect_equal(nrow(result), 1)
  expect_equal(result$start, 161L)
  expect_equal(result$end, 200L)
})
