# Copyright 2025 Observational Health Data Sciences and Informatics
#
# This file is part of DatabaseConnector
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' @importMethodsFrom DBI dbSendQuery dbGetQuery
#' @importClassesFrom duckdb duckdb_connection
#' @export
setClass(
  "muckdb",
  contains = "duckdb_connection",
  slots = c(
    muckdbPlatform = "character",
    muckdbSqlglot = "ANY"
  )
)


#' muckdb: Mock DBMS connection using DuckDB and sqlglot
#'
#' This class allows testing SQL logic as if running on any platform (e.g., Hive, BigQuery),
#' but actually executes against DuckDB using Python's sqlglot for SQL dialect translation.
#' @examples
#' # Connect to a mock Hive platform using DuckDB and sqlglot
#' conn <- muckdbConnect(platform = "hive")
#'
#' # Create a table using Hive SQL syntax
#' DBI::dbGetQuery(conn, "CREATE TABLE myTable AS SELECT 1 AS x")
#'
#' # Query the table using Hive SQL syntax
#' result <- DBI::dbGetQuery(conn, "SELECT x FROM myTable")
#' print(result)
#'
#' # Disconnect when done
#' DBI::dbDisconnect(conn)
#' @param platform The DBMS dialect to emulate (e.g., "hive", "bigquery", "spark", "oracle", ...)
#' @param dbDir Path to DuckDB database file (or ":memory:")
#' @export
muckdbConnect <- function(platform = "duckdb", ...) {
  if (!reticulate::py_module_available("sqlglot"))
    stop("Python module 'sqlglot' is required.")
  sqlglot <- reticulate::import("sqlglot")

  duckdbCon <- DBI::dbConnect(duckdb::duckdb(), ...)

  # Get all slot names/values from parent
  parentSlots <- slotNames(duckdbCon)
  parentSlotValues <- lapply(parentSlots, function(s) slot(duckdbCon, s))
  names(parentSlotValues) <- parentSlots

  # Add muckdb-specific slots
  allSlots <- c(
    list(
      muckdbPlatform = platform,
      muckdbSqlglot = sqlglot
    ),
    parentSlotValues
  )

  do.call("new", c("muckdb", allSlots))
}

.transpile <- function(conn, statement, ...) {
  platform <- conn@muckdbPlatform
  if (platform == "postgresql") {
    platform <- "postgres"
  } else if (platform == "sql server") {
    platform <- "tsql"
  }

  sqlglot <- conn@muckdbSqlglot
  translated <- sqlglot$transpile(statement, read = platform, write = "duckdb")[[1]]
  callNextMethod(conn, translated, ...)
}

#' @export
setMethod(
  "dbSendQuery",
  signature(conn = "muckdb", statement = "character"),
  .transpile
)

#' @export
setMethod(
  "dbGetQuery",
  signature(conn = "muckdb", statement = "character"),
  .transpile
)


#' @export
setClass(
  "muckdb_driver",
  contains = "duckdb_driver",
  slots = list(
    muckdbPlatform = "character",
    muckdbSqlglot = "ANY"
  )
)

muckdb <- function(platform = "duckdb", dbdir = ":memory:", ...) {
  ensure_installed("reticulate")
  if (!reticulate::py_module_available("sqlglot"))
    stop("Python module 'sqlglot' is required. Install via pip: pip install sqlglot or reticulate::install_python('sqlglot')")
  sqlglot <- reticulate::import("sqlglot")

  # Construct the underlying duckdb driver
  duckdbDrv <- duckdb::duckdb(dbdir = dbdir, ...)

  # Copy all slots from duckdb_driver
  parentSlots <- slotNames(duckdbDrv)
  parentSlotValues <- lapply(parentSlots, function(s) slot(duckdbDrv, s))
  names(parentSlotValues) <- parentSlots

  # Add muckdb-specific slots
  allSlots <- c(
    list(
      muckdbPlatform = platform,
      muckdbSqlglot = sqlglot
    ),
    parentSlotValues
  )

  do.call("new", c("muckdb_driver", allSlots))
}

setMethod(
  "dbConnect",
  signature(drv = "muckdb_driver"),
  function(drv, dbdir = ":memory:", ...) {
    # Create the underlying duckdb_connection
    duckdbCon <- DBI::dbConnect(duckdb::duckdb(), dbdir = dbdir, ...)
    # Copy all parent slots
    parent_slots <- slotNames(duckdbCon)
    parent_slot_values <- lapply(parent_slots, function(s) slot(duckdbCon, s))
    names(parent_slot_values) <- parent_slots

    # Add muckdb-specific slots from the driver
    all_slots <- c(
      list(
        muckdbPlatform = drv@muckdbPlatform,
        muckdbSqlglot = drv@muckdbSqlglot
      ),
      parent_slot_values
    )

    do.call("new", c("muckdb", all_slots))
  }
)
