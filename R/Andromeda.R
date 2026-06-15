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

#' Retrieves data to a local Andromeda object
#'
#' @description
#' This function sends SQL to the server, and returns the results in a local Andromeda object
#'
#' @template Connection
#' @param sql                    The SQL to be sent.
#' @template ErrorReportFile
#' @template SnakeCaseToCamelCase
#' @template Andromeda
#' @template IntegerAsNumeric
#'
#' @details
#' Retrieves data from the database server and stores it in a local Andromeda object. This allows
#' very large data sets to be retrieved without running out of memory. If an error occurs during SQL
#' execution, this error is written to a file to facilitate debugging. Null values in the database are
#' converted to NA values in R.If a table with the same name already exists in the local Andromeda
#' object it is replaced.
#'
#' @return
#' Invisibly returns the andromeda. The Andromeda object will have a table added with the query
#' results.
#'
#' @examples
#' \dontrun{
#' andromeda <- Andromeda::andromeda()
#' connectionDetails <- createConnectionDetails(
#'   dbms = "postgresql",
#'   server = "localhost",
#'   user = "root",
#'   password = "blah",
#'   schema = "cdm_v4"
#' )
#' conn <- connect(connectionDetails)
#' querySqlToAndromeda(
#'   connection = conn,
#'   sql = "SELECT * FROM person;",
#'   andromeda = andromeda,
#'   andromedaTableName = "foo"
#' )
#' disconnect(conn)
#'
#' andromeda$foo
#' }
#' @export
querySqlToAndromeda <- function(
    connection,
    sql,
    andromeda,
    andromedaTableName,
    errorReportFile = file.path(getwd(), "errorReportSql.txt"),
    snakeCaseToCamelCase = FALSE,
    appendToTable = FALSE,
    integerAsNumeric = getOption("databaseConnectorIntegerAsNumeric", default = TRUE),
    integer64AsNumeric = getOption("databaseConnectorInteger64AsNumeric", default = TRUE)
) {
  if (!DBI::dbIsValid(connection)) {
    stop("Connection is closed")
  }
  if (!inherits(andromeda, "Andromeda")) {
    stop("The andromeda argument must be an Andromeda object.")
  }
  if (packageVersion("Andromeda") < "0.6.0") {
    stop(sprintf("Andromeda version 0.6.0 or higher required, but version %s found", packageVersion("Andromeda")))
  }
  
  # Calling splitSql, because this will also strip trailing semicolons (which cause Oracle to crash).
  sqlStatements <- SqlRender::splitSql(sql)
  if (length(sqlStatements) > 1) {
    stop(paste(
      "A query that returns a result can only consist of one SQL statement, but",
      length(sqlStatements),
      "statements were found"
    ))
  }
  
  tryCatch({
    logTrace(paste("Querying SQL:", truncateSql(sql)))
    startTime <- Sys.time()
    queryResult <- DBI::dbSendQuery(connection, sql)
    
    on.exit(DBI::dbClearResult(queryResult))
    first <- TRUE
    # Even if there are no rows, we want to get an empty table with the right fields, hence check
    # for `first`:
    while (!DBI::dbHasCompleted(queryResult) || first) {
      batch <- DBI::dbFetch(queryResult, n = DBFETCH_BATCH_SIZE)
      batch <- convertAllInteger64ToNumeric(batch, integerAsNumeric, integer64AsNumeric)
      batch <- convertFields(batch, dbms(connection))
      if (snakeCaseToCamelCase) {
        colnames(batch) <- SqlRender::snakeCaseToCamelCase(colnames(batch))
      }
      if (first && !appendToTable) {
        andromeda[[andromedaTableName]] <- batch
        first <- FALSE
      } else {
        Andromeda::appendToTable(andromeda[[andromedaTableName]], batch)
      }
    }
    delta <- Sys.time() - startTime
    logTrace(paste("Querying SQL took", delta, attr(delta, "units")))
    invisible(andromeda)
  },
  error = function(err) {
    .createErrorReport(dbms(connection), err, sql, errorReportFile)
  })
}

#' Render, translate, and query to local Andromeda
#'
#' @description
#' This function renders, and translates SQL, sends it to the server, and returns the results as an
#' `Andromeda` object
#'
#' @template Connection
#' @param sql                    The SQL to be send.
#' @template ErrorReportFile
#' @template SnakeCaseToCamelCase
#' @template Andromeda
#' @template TempEmulationSchema
#' @template IntegerAsNumeric
#' @param ...                    Parameters that will be used to render the SQL.
#'
#' @details
#' This function calls the `render` and `translate` functions in the `SqlRender` package
#' before calling [querySqlToAndromeda()].
#'
#' @return
#' Invisibly returns the andromeda. The Andromeda object will have a table added with the query
#' results.
#'
#' @examples
#' \dontrun{
#' connectionDetails <- createConnectionDetails(
#'   dbms = "postgresql",
#'   server = "localhost",
#'   user = "root",
#'   password = "blah",
#'   schema = "cdm_v4"
#' )
#' conn <- connect(connectionDetails)
#' renderTranslatequerySqlToAndromeda(conn,
#'   sql = "SELECT * FROM @@schema.person",
#'   schema = "cdm_synpuf",
#'   andromeda = andromeda,
#'   andromedaTableName = "foo"
#' )
#' disconnect(conn)
#'
#' andromeda$foo
#' }
#' @export
renderTranslateQuerySqlToAndromeda <- function(
    connection,
    sql,
    andromeda,
    andromedaTableName,
    errorReportFile = file.path(
      getwd(),
      "errorReportSql.txt"
    ),
    snakeCaseToCamelCase = FALSE,
    appendToTable = FALSE,
    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
    integerAsNumeric = getOption("databaseConnectorIntegerAsNumeric", default = TRUE),
    integer64AsNumeric = getOption("databaseConnectorInteger64AsNumeric", default = TRUE),
    ...
) {
  if (is(connection, "Pool")) {
    connection <- pool::poolCheckout(connection)
    on.exit(pool::poolReturn(connection))
  }
  sql <- SqlRender::render(sql, ...)
  sql <- SqlRender::translate(sql,
                              targetDialect = dbms(connection),
                              tempEmulationSchema = tempEmulationSchema
  )
  return(querySqlToAndromeda(
    connection = connection,
    sql = sql,
    andromeda = andromeda,
    andromedaTableName = andromedaTableName,
    errorReportFile = errorReportFile,
    snakeCaseToCamelCase = snakeCaseToCamelCase,
    appendToTable = appendToTable,
    integerAsNumeric = integerAsNumeric,
    integer64AsNumeric = integer64AsNumeric
  ))
}
