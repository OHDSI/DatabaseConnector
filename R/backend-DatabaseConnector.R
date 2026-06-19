# Copyright 2026 Observational Health Data Sciences and Informatics
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

#' Declare dbplyr backend edition
#' 
#' @param con Database connection
#' @export
#' @importFrom dbplyr dbplyr_edition
dbplyr_edition.DatabaseConnectorConnection <- function(con) {
  2L
}

#' @rdname dbplyr_edition.DatabaseConnectorConnection
#' @export
dbplyr_edition.DatabaseConnectorJdbcConnection <- function(con) {
  2L
}

#' @rdname dbplyr_edition.DatabaseConnectorConnection
#' @export
dbplyr_edition.DatabaseConnectorDbiConnection <- function(con) {
  2L
}

# Export a sql_dialect method for JDBC connections that allows dplyr code to 
# be correctly translated to SQL code.
#' @export
#' @importFrom dbplyr sql_dialect 
sql_dialect.DatabaseConnectorJdbcConnection <- function(con) { 
  switch(dbms(con), 
         "oracle"     = dbplyr::dialect_oracle(), 
         "postgresql" = dbplyr::dialect_postgres(), 
         "redshift"   = dbplyr::dialect_redshift(), 
         "sql server" = dbplyr::dialect_mssql(), 
         "bigquery"   = {
           # Use a lightweight dummy S3 object to safely dispatch to bigrquery
           dummy_con <- structure(list(), class = c("BigQueryConnection", "DBIConnection"))
           dbplyr::sql_dialect(dummy_con)
         }, 
         "spark"      = dbplyr::dialect_spark_sql(), 
         "snowflake"  = dbplyr::dialect_snowflake(), 
         "synapse"    = dbplyr::dialect_mssql(), 
         "iris"       = dbplyr::dialect_postgres(), 
         rlang::abort("Sql dialect is not supported!")) 
}

# Export a sql_translation method to explicitly preserve external package translations
#' @export
#' @importFrom dbplyr sql_translation
sql_translation.DatabaseConnectorJdbcConnection <- function(con) {
  if (dbms(con) == "bigquery") {
    return(utils::getFromNamespace("sql_translation.BigQueryConnection", "bigrquery")(con))
  }
  # Fall back to new 2nd edition dbplyr behavior for built-in dialects
  NextMethod()
}

# In addition to JDBC connections, DatabaseConnector also wraps duckdb and sqlite DBI connections
#' @export
#' @importFrom dbplyr sql_dialect 
sql_dialect.DatabaseConnectorDbiConnection <- function(con) { 
  switch(dbms(con), 
         "sqlite" = dbplyr::dialect_sqlite(), 
         "duckdb" = {
           # DuckDB's dialect function requires the actual S4 duckdb_connection object, 
           # which is wrapped inside the dbiConnection slot.
           dbplyr::sql_dialect(con@dbiConnection)
         }, 
         NextMethod()) 
}

#' @export
#' @importFrom dbplyr sql_translation
sql_translation.DatabaseConnectorDbiConnection <- function(con) {
  if (dbms(con) == "duckdb") {
    # Pass the real S4 duckdb_connection object for translations
    return(utils::getFromNamespace("sql_translation.duckdb_connection", "duckdb")(con@dbiConnection))
  }
  # Fall back to new 2nd edition dbplyr behavior for built-in dialects
  NextMethod()
}

#' @importFrom dbplyr sql_escape_logical
#' @export
dbplyr::sql_escape_logical

#' @export
sql_escape_logical.DatabaseConnectorJdbcConnection <- function(con, x) {
  if (dbms(con) == "sql server") {
    dplyr::if_else(x, "1", "0", missing = "NULL")
  } else {
    NextMethod()
  }
}
