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
#' @param platform The DBMS dialect to emulate (e.g., "hive", "bigquery", "spark")
#' @param dbDir Path to DuckDB database file (or ":memory:")
#' @export
muckdbConnect <- function(platform = "duckdb", ...) {
  if (!reticulate::py_module_available("sqlglot"))
    stop("Python module 'sqlglot' is required. Install via pip: pip install sqlglot or reticulate::install_python('sqlglot')")
  sqlglot <- reticulate::import("sqlglot")
  con <- DBI::dbConnect(duckdb::duckdb(), ...)
  attr(con, "muckdbPlatform") <- platform
  attr(con, "muckdbSqlglot") <- sqlglot
  class(con) <- c("muckdb", class(con))
  con
}

#' @export
dbSendQuery.muckdb <- function(conn, statement, ...) {
  platform <- attr(conn, "muckdbPlatform")
  sqlglot <- attr(conn, "muckdbSqlglot")
  translated <- sqlglot$transpile(statement, read = platform, write = "duckdb")[[1]]
  DBI::dbSendQuery(conn = unclass(conn), statement = translated, ...)
}

#' @export
dbGetQuery.muckdb <- function(conn, statement, ...) {
  platform <- attr(conn, "muckdbPlatform")
  sqlglot <- attr(conn, "muckdbSqlglot")
  translated <- sqlglot$transpile(statement, read = platform, write = "duckdb")[[1]]
  DBI::dbGetQuery(conn = unclass(conn), statement = translated, ...)
}

