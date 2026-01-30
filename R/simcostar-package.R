#' @keywords internal
"_PACKAGE"

## data.table compatibility
#' @importFrom data.table :=
NULL

## Suppress R CMD check notes for data.table column references
utils::globalVariables(c(
  "boia_id", "time", "variable", "value",
  "depth", "speed", "direction"
))
