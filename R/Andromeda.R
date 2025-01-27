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

#' Low level function for retrieving data to a local Andromeda object
#'
#' @description
#' This is the equivalent of the [querySqlToAndromeda()] function, except no error report is
#' written when an error occurs.
#'
#' @template Connection
#' @param query                The SQL statement to retrieve the data
#' @param datesAsString        Should dates be imported as character vectors, our should they be
#'                             converted to R's date format?
#' @template Andromeda
#' @template SnakeCaseToCamelCase
#' @template IntegerAsNumeric
#' 
#' @details
#' Retrieves data from the database server and stores it in a local Andromeda object This allows
#' very large data sets to be retrieved without running out of memory. Null values in the database are
#' converted to NA values in R. If a table with the same name already exists in the local Andromeda
#' object it is replaced.
#'
#' @return
#' Invisibly returns the andromeda. The Andromeda object will have a table added with the query
#' results.
#'
#' @export
lowLevelQuerySqlToAndromeda <- function(connection,
                                        query,
                                        andromeda,
                                        andromedaTableName,
                                        datesAsString = FALSE,
                                        appendToTable = FALSE,
                                        snakeCaseToCamelCase = FALSE,
                                        integerAsNumeric = getOption("databaseConnectorIntegerAsNumeric",
                                                                     default = TRUE
                                        ),
                                        integer64AsNumeric = getOption("databaseConnectorInteger64AsNumeric",
                                                                       default = TRUE
                                        )) {
  UseMethod("lowLevelQuerySqlToAndromeda", connection)
}

#' @export
lowLevelQuerySqlToAndromeda.default <- function(connection,
                                                query,
                                                andromeda,
                                                andromedaTableName,
                                                datesAsString = FALSE,
                                                appendToTable = FALSE,
                                                snakeCaseToCamelCase = FALSE,
                                                integerAsNumeric = getOption("databaseConnectorIntegerAsNumeric",
                                                                             default = TRUE
                                                ),
                                                integer64AsNumeric = getOption("databaseConnectorInteger64AsNumeric",
                                                                               default = TRUE
                                                )) {
  if (rJava::is.jnull(connection@jConnection)) {
    stop("Connection is closed")
  }
  logTrace(paste("Querying SQL:", truncateSql(query)))
  startTime <- Sys.time()
  
  batchedQuery <- rJava::.jnew(
    "org.ohdsi.databaseConnector.BatchedQuery",
    connection@jConnection,
    query,
    dbms(connection)
  )
  
  on.exit(rJava::.jcall(batchedQuery, "V", "clear"))
  
  columnTypes <- rJava::.jcall(batchedQuery, "[I", "getColumnTypes")
  if (length(columnTypes) == 0) {
    stop("No columns found")
  }
  if (any(columnTypes == 5)) {
    validateInt64Query()
  }
  first <- TRUE
  while (!rJava::.jcall(batchedQuery, "Z", "isDone")) {
    rJava::.jcall(batchedQuery, "V", "fetchBatch")
    batch <- parseJdbcColumnData(batchedQuery,
                                 columnTypes = columnTypes,
                                 datesAsString = datesAsString,
                                 integer64AsNumeric = integer64AsNumeric,
                                 integerAsNumeric = integerAsNumeric
    )
    if (snakeCaseToCamelCase) {
      colnames(batch) <- SqlRender::snakeCaseToCamelCase(colnames(batch))
    }
    if (first && !appendToTable) {
      andromeda[[andromedaTableName]] <- batch
    } else {
      Andromeda::appendToTable(andromeda[[andromedaTableName]], batch)
    }
    first <- FALSE
  }
  delta <- Sys.time() - startTime
  logTrace(paste("Querying SQL took", delta, attr(delta, "units")))
  
  invisible(andromeda)
}

#' @export
lowLevelQuerySqlToAndromeda.DatabaseConnectorDbiConnection <- function(connection,
                                                                       query,
                                                                       andromeda,
                                                                       andromedaTableName,
                                                                       datesAsString = FALSE,
                                                                       appendToTable = FALSE,
                                                                       snakeCaseToCamelCase = FALSE,
                                                                       integerAsNumeric = getOption("databaseConnectorIntegerAsNumeric",
                                                                                                    default = TRUE
                                                                       ),
                                                                       integer64AsNumeric = getOption("databaseConnectorInteger64AsNumeric",
                                                                                                      default = TRUE
                                                                       )) {
  logTrace(paste("Querying SQL:", truncateSql(query)))
  startTime <- Sys.time()
  
  batchSize <- 100000
  resultSet <- DBI::dbSendQuery(connection@dbiConnection, query)
  on.exit(DBI::dbClearResult(resultSet))
  first <- TRUE
  while (first || !DBI::dbHasCompleted(resultSet)) {
    batch <- DBI::dbFetch(resultSet, batchSize)
    if (integerAsNumeric) {
      for (i in seq_len(ncol(batch))) {
        if (is(batch[[i]], "integer")) {
          batch[[i]] <- as.numeric(batch[[i]])
        }
      }
    }
    if (integer64AsNumeric) {
      for (i in seq_len(ncol(batch))) {
        if (is(batch[[i]], "integer64")) {
          batch[[i]] <- convertInteger64ToNumeric(batch[[i]])
        }
      }
    }
    batch <- convertFields(dbms(connection), batch)
    if (snakeCaseToCamelCase) {
      colnames(batch) <- SqlRender::snakeCaseToCamelCase(colnames(batch))
    }
    if (first && !appendToTable) {
      andromeda[[andromedaTableName]] <- batch
    } else {
      Andromeda::appendToTable(andromeda[[andromedaTableName]], batch)
    }
    first <- FALSE
  }
  delta <- Sys.time() - startTime
  logTrace(paste("Querying SQL took", delta, attr(delta, "units")))
  
  invisible(andromeda)
}

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
querySqlToAndromeda <- function(connection,
                                sql,
                                andromeda,
                                andromedaTableName,
                                errorReportFile = file.path(getwd(), "errorReportSql.txt"),
                                snakeCaseToCamelCase = FALSE,
                                appendToTable = FALSE,
                                integerAsNumeric = getOption("databaseConnectorIntegerAsNumeric",
                                                             default = TRUE
                                ),
                                integer64AsNumeric = getOption("databaseConnectorInteger64AsNumeric",
                                                               default = TRUE
                                )) {
  if (inherits(
    connection,
    "DatabaseConnectorJdbcConnection"
  ) && rJava::is.jnull(connection@jConnection)) {
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
  tryCatch(
    {
      andromeda <- lowLevelQuerySqlToAndromeda(
        connection = connection,
        query = sqlStatements[1],
        andromeda = andromeda,
        andromedaTableName = andromedaTableName,
        appendToTable = appendToTable, 
        snakeCaseToCamelCase = snakeCaseToCamelCase,
        integerAsNumeric = integerAsNumeric,
        integer64AsNumeric = integer64AsNumeric
      )
      invisible(andromeda)
    },
    error = function(err) {
      .createErrorReport(dbms(connection), err$message, sql, errorReportFile)
    }
  )
}

#' Render, translate, and query to local Andromeda
#'
#' @description
#' This function renders, and translates SQL, sends it to the server, and returns the results as an
#' ffdf object
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
renderTranslateQuerySqlToAndromeda <- function(connection,
                                               sql,
                                               andromeda,
                                               andromedaTableName,
                                               errorReportFile = file.path(
                                                 getwd(),
                                                 "errorReportSql.txt"
                                               ),
                                               snakeCaseToCamelCase = FALSE,
                                               appendToTable = FALSE,
                                               oracleTempSchema = NULL,
                                               tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                               integerAsNumeric = getOption("databaseConnectorIntegerAsNumeric",
                                                                            default = TRUE
                                               ),
                                               integer64AsNumeric = getOption("databaseConnectorInteger64AsNumeric",
                                                                              default = TRUE
                                               ),
                                               ...) {
  if (is(connection, "Pool")) {
    connection <- pool::poolCheckout(connection)
    on.exit(pool::poolReturn(connection))
  }
  if (!is.null(oracleTempSchema) && oracleTempSchema != "") {
    warn("The 'oracleTempSchema' argument is deprecated. Use 'tempEmulationSchema' instead.",
         .frequency = "regularly",
         .frequency_id = "oracleTempSchema"
    )
    tempEmulationSchema <- oracleTempSchema
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
