#' @details
#' Fields will be automatically converted for improved consistenty in these situations:
#'
#' - SQLite: Fields with names ending in `_date` will be converted to DATE fields. Rationale: SQLite 
#'   does not support DATE fields.
#' - SQLite: Fields with names ending in `_datetime` will be converted to POSIXct fields. Rationale: 
#'   SQLite does not support DATETIME fields.
#' - BigQuery and Snowflake: Integer fields will be converted to Integer if it fits in an integer, or
#'   will remain Integer64 otherwise. Rationale: these platforms do not distinguish between INT and 
#'   BIGINT.
