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
           requireNamespace("bigrquery", quietly = TRUE)
           
           # Explicitly fetch the S4 class definition from the bigrquery namespace.
           # This prevents R CMD check from failing to find the class in the search path.
           bq_class <- methods::getClass("BigQueryConnection", where = asNamespace("bigrquery"))
           
           # Instantiate the dummy object using the explicit class definition
           dummy_con <- methods::new(bq_class, use_legacy_sql = FALSE)
           
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

# For wrapped duckdb connections, borrow the duckdb package's dbplyr backend as a whole by
# delegating these methods to their duckdb_connection implementations (called with the real
# duckdb connection). Other wrapped DBI connections (e.g. SQLite) use the default behavior.
# `tbl` is deliberately not delegated, because its duckdb method relies on NextMethod().
duckdbConnectionMethods <- list(
  list(generic = "sql_translation", package = "dbplyr"),
  list(generic = "sql_escape_date", package = "dbplyr"),
  list(generic = "sql_escape_datetime", package = "dbplyr"),
  list(generic = "sql_expr_matches", package = "dbplyr"),
  list(generic = "db_connection_describe", package = "dbplyr")
)

registerDuckdbBackend <- function() {
  delegate <- function(generic, getCon, class = "duckdb_connection") {
    force(generic)
    force(class)
    function(x, ...) {
      con <- getCon(x)
      if (dbms(con) == "duckdb") {
        # Older duckdb versions may not define every method; then use the default
        method <- tryCatch(utils::getFromNamespace(paste0(generic, ".", class), "duckdb"),
                           error = function(e) NULL)
        if (!is.null(method)) {
          if (inherits(x, "DatabaseConnectorConnection")) {
            return(method(x@dbiConnection, ...))
          }
          return(method(x, ...))
        }
      }
      NextMethod()
    }
  }
  for (spec in duckdbConnectionMethods) {
    registerS3method(spec$generic,
                     "DatabaseConnectorDbiConnection",
                     delegate(spec$generic, identity),
                     envir = asNamespace(spec$package))
  }
  # dplyr is only suggested, so register its method when (if) it is loaded
  registerSliceSample <- function(...) {
    registerS3method("slice_sample",
                     "tbl_DatabaseConnectorDbiConnection",
                     delegate("slice_sample", dbplyr::remote_con, "tbl_duckdb_connection"),
                     envir = asNamespace("dplyr"))
  }
  if ("dplyr" %in% loadedNamespaces()) {
    registerSliceSample()
  } else {
    setHook(packageEvent("dplyr", "onLoad"), registerSliceSample)
  }
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