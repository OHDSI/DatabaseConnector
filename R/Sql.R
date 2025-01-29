# @file Sql.R
#
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

#' Get available Java heap space
#'
#' @description
#' For debugging purposes: get the available Java heap space.
#'
#' @return
#' The Java heap space (in bytes).
#'
#' @export
getAvailableJavaHeapSpace <- function() {
  availableSpace <- rJava::J("org.ohdsi.databaseConnector.BatchedQuery")$getAvailableHeapSpace()
  return(availableSpace)
}

.systemInfo <- function() {
  si <- sessionInfo()
  lines <- c()
  lines <- c(lines, "R version:")
  lines <- c(lines, si$R.version$version.string)
  lines <- c(lines, "")
  lines <- c(lines, "Platform:")
  lines <- c(lines, si$R.version$platform)
  lines <- c(lines, "")
  lines <- c(lines, "Attached base packages:")
  lines <- c(lines, paste("-", si$basePkgs))
  lines <- c(lines, "")
  lines <- c(lines, "Other attached packages:")
  for (pkg in si$otherPkgs) {
    lines <- c(
      lines,
      paste("- ", pkg$Package, " (", pkg$Version, ")", sep = "")
    )
  }
  return(paste(lines, collapse = "\n"))
}

.createErrorReport <- function(dbms, message, sql, fileName) {
  report <- c("DBMS:\n", dbms, "\n\nError:\n", message, "\n\nSQL:\n", sql, "\n\n", .systemInfo())
  fileConn <- file(fileName)
  writeChar(report, fileConn, eos = NULL)
  close(fileConn)
  abort(paste("Error executing SQL:",
              message,
              paste("An error report has been created at ", fileName),
              sep = "\n"
  ), call. = FALSE)
}

validateInt64Query <- function() {
  # Validate that communication of 64-bit integers with Java is correct:
  array <- rJava::J("org.ohdsi.databaseConnector.BatchedQuery")$validateInteger64()
  oldClass(array) <- "integer64"
  if (!all.equal(array, bit64::as.integer64(c(1, -1, 8589934592, -8589934592)))) {
    abort("Error converting 64-bit integers between R and Java")
  }
}

convertInteger64ToNumeric <- function(x) {
  if (length(x) == 0) {
    return(numeric(0))
  }
  maxInt64 <- bit64::as.integer64(2)^53
  if (any(x >= maxInt64 | x <= -maxInt64, na.rm = TRUE)) {
    abort("The data contains integers >= 2^53, and converting those to R's numeric type leads to precision loss. Consider using smaller integers, converting the integers to doubles on the database side, or using `options(databaseConnectorInteger64AsNumeric = FALSE)`.")
  }
  return(bit64::as.double.integer64(x))
}

parseJdbcColumnData <- function(batchedQuery,
                                columnTypes = NULL,
                                datesAsString = FALSE,
                                integerAsNumeric = getOption("databaseConnectorIntegerAsNumeric",
                                                             default = TRUE
                                ),
                                integer64AsNumeric = getOption("databaseConnectorInteger64AsNumeric",
                                                               default = TRUE
                                )) {
  if (is.null(columnTypes)) {
    columnTypes <- rJava::.jcall(batchedQuery, "[I", "getColumnTypes")
  }
  columns <- vector("list", length(columnTypes))
  for (i in seq_along(columnTypes)) {
    if (columnTypes[i] == 1) {
      column <- rJava::.jcall(batchedQuery, "[D", "getNumeric", as.integer(i))
    } else if (columnTypes[i] == 5) {
      column <- rJava::.jcall(batchedQuery, "[D", "getInteger64", as.integer(i))
      oldClass(column) <- "integer64"
      if (integer64AsNumeric) {
        column <- convertInteger64ToNumeric(column)
      }
    } else if (columnTypes[i] == 6) {
      column <- rJava::.jcall(batchedQuery, "[I", "getInteger", as.integer(i))
      if (integerAsNumeric) {
        column <- as.numeric(column)
      }
    } else if (columnTypes[i] == 3) {
      column <- rJava::.jcall(batchedQuery, "[I", "getInteger", as.integer(i))
      column <- as.Date(column, origin = "1970-01-01")
      if (datesAsString) {
        column <- format(column, "%Y-%m-%d")
      }
    } else if (columnTypes[i] == 4) {
      column <- rJava::.jcall(batchedQuery, "[D", "getNumeric", as.integer(i))
      column <- as.POSIXct(column, origin = "1970-01-01")
    } else {
      column <- rJava::.jcall(batchedQuery, "[Ljava/lang/String;", "getString", i)
      if (!datesAsString) {
        if (columnTypes[i] == 4) {
          column <- as.POSIXct(column)
        }
      }
    }
    columns[[i]] <- column
  }
  names(columns) <- rJava::.jcall(batchedQuery, "[Ljava/lang/String;", "getColumnNames")
  # More efficient than as.data.frame, as it avoids converting row.names to character:
  columns <- structure(columns, class = "data.frame", row.names = seq_len(length(columns[[1]])))
  return(columns)
}

#' Low level function for retrieving data to a data frame
#'
#' @description
#' This is the equivalent of the [querySql()] function, except no error report is written
#' when an error occurs.
#'
#' @template Connection
#' @param query           The SQL statement to retrieve the data
#' @param datesAsString   Logical: Should dates be imported as character vectors, our should they be converted
#'                        to R's date format?
#' @template IntegerAsNumeric
#'
#' @details
#' Retrieves data from the database server and stores it in a data frame. Null values in the database are converted
#' to NA values in R.
#'
#' @return
#' A data frame containing the data retrieved from the server
#'
#' @export
lowLevelQuerySql <- function(connection,
                             query,
                             datesAsString = FALSE,
                             integerAsNumeric = getOption("databaseConnectorIntegerAsNumeric", default = TRUE),
                             integer64AsNumeric = getOption("databaseConnectorInteger64AsNumeric", default = TRUE)) {
  UseMethod("lowLevelQuerySql", connection)
}

#' @export
lowLevelQuerySql.default <- function(connection,
                                     query,
                                     datesAsString = FALSE,
                                     integerAsNumeric = getOption("databaseConnectorIntegerAsNumeric", default = TRUE),
                                     integer64AsNumeric = getOption("databaseConnectorInteger64AsNumeric", default = TRUE)) {
  if (rJava::is.jnull(connection@jConnection)) {
    abort("Connection is closed")
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
  columns <- getAllBatches(batchedQuery = batchedQuery,
                           datesAsString = datesAsString,
                           integer64AsNumeric = integer64AsNumeric,
                           integerAsNumeric = integerAsNumeric)
  delta <- Sys.time() - startTime
  logTrace(paste("Querying SQL took", delta, attr(delta, "units")))
  return(columns)
}

getAllBatches <- function(batchedQuery, datesAsString, integer64AsNumeric, integerAsNumeric) {
  columnTypes <- rJava::.jcall(batchedQuery, "[I", "getColumnTypes")
  if (any(columnTypes == 5)) {
    validateInt64Query()
  }
  columns <- data.frame()
  while (!rJava::.jcall(batchedQuery, "Z", "isDone")) {
    rJava::.jcall(batchedQuery, "V", "fetchBatch")
    batch <- parseJdbcColumnData(batchedQuery,
                                 columnTypes = columnTypes,
                                 datesAsString = datesAsString,
                                 integer64AsNumeric = integer64AsNumeric,
                                 integerAsNumeric = integerAsNumeric
    )
    columns <- rbind(columns, batch)
  }
  return(columns)
}

#' @export
lowLevelQuerySql.DatabaseConnectorDbiConnection <- function(connection,
                                                            query,
                                                            datesAsString = FALSE,
                                                            integerAsNumeric = getOption("databaseConnectorIntegerAsNumeric", default = TRUE),
                                                            integer64AsNumeric = getOption("databaseConnectorInteger64AsNumeric", default = TRUE)) {
  logTrace(paste("Querying SQL:", truncateSql(query)))
  startTime <- Sys.time()
  
  columns <- DBI::dbGetQuery(connection@dbiConnection, query)
  if (integerAsNumeric) {
    for (i in seq_len(ncol(columns))) {
      if (is(columns[[i]], "integer")) {
        columns[[i]] <- as.numeric(columns[[i]])
      }
    }
  }
  if (integer64AsNumeric) {
    for (i in seq_len(ncol(columns))) {
      if (is(columns[[i]], "integer64")) {
        columns[[i]] <- convertInteger64ToNumeric(columns[[i]])
      }
    }
  }
  delta <- Sys.time() - startTime
  logTrace(paste("Querying SQL took", delta, attr(delta, "units")))
  return(columns)
}

#' Execute SQL code
#'
#' @description
#' This function executes a single SQL statement.
#'
#' @template Connection
#' @param sql          The SQL to be executed
#'
#' @export
lowLevelExecuteSql <- function(connection, sql) {
  UseMethod("lowLevelExecuteSql", connection)
}

ddlExecutionTimes <- new.env()
insertExecutionTimes <- new.env()

delayIfNecessary <- function(sql, regex, executionTimes, threshold) {
  regexGroups <- stringr::str_match(sql, stringr::regex(regex, ignore_case = TRUE))
  tableName <- regexGroups[3]
  if (!is.na(tableName) && !is.null(tableName)) {
    currentTime <- Sys.time()
    lastExecutedTime <- executionTimes[[tableName]]
    if (!is.na(lastExecutedTime) && !is.null(lastExecutedTime)) {
      delta <- difftime(currentTime, lastExecutedTime, units = "secs") 
      if (delta < threshold) {
        Sys.sleep(threshold - delta)
      }
    }
    executionTimes[[tableName]] <- currentTime
  }
}

delayIfNecessaryForDdl <- function(sql) {
  regexForDdl <- "(^CREATE\\s+TABLE\\s+IF\\s+EXISTS|^CREATE\\s+TABLE|^DROP\\s+TABLE\\s+IF\\s+EXISTS|^DROP\\s+TABLE)\\s+([a-zA-Z0-9_$#-]*\\.?\\s*(?:[a-zA-Z0-9_]+)*)"
  delayIfNecessary(sql, regexForDdl, ddlExecutionTimes, 5)
}

delayIfNecessaryForInsert <- function(sql) {
  regexForInsert <- "(^INSERT\\s+INTO)\\s+([a-zA-Z0-9_$#-]*\\.?\\s*(?:[a-zA-Z0-9_]+)*)"
  delayIfNecessary(sql, regexForInsert, insertExecutionTimes, 5)
}

#' @export
lowLevelExecuteSql.default <- function(connection, sql) {
  logTrace(paste("Executing SQL:", truncateSql(sql)))
  startTime <- Sys.time()
  
  statement <- rJava::.jcall(connection@jConnection, "Ljava/sql/Statement;", "createStatement")
  on.exit(rJava::.jcall(statement, "V", "close"))
  if ((dbms(connection) == "spark") || (dbms(connection) == "iris")) {
    # For some queries the DataBricks JDBC driver will throw an error saying no ROWCOUNT is returned
    # when using executeLargeUpdate, so using execute instead.
    # Also use this approach for IRIS JDBC driver, which does not support executeLargeUpdate() directly.
    rJava::.jcall(statement, "Z", "execute", as.character(sql), check = FALSE)
    rowsAffected <- rJava::.jcall(statement, "I", "getUpdateCount", check = FALSE)
    if (rowsAffected == -1) {
      rowsAffected <- 0
    }
  } else {
    rowsAffected <- rJava::.jcall(statement, "J", "executeLargeUpdate", as.character(sql), check = FALSE)
  }
  
  if (dbms(connection) == "bigquery") {
    delayIfNecessaryForDdl(sql)
    delayIfNecessaryForInsert(sql)
  }
  
  delta <- Sys.time() - startTime
  logTrace(paste("Executing SQL took", delta, attr(delta, "units")))
  invisible(rowsAffected)
}

#' @export
lowLevelExecuteSql.DatabaseConnectorDbiConnection <- function(connection, sql) {
  logTrace(paste("Executing SQL:", truncateSql(sql)))
  startTime <- Sys.time()
  
  rowsAffected <- DBI::dbExecute(connection@dbiConnection, sql)
  
  delta <- Sys.time() - startTime
  logTrace(paste("Executing SQL took", delta, attr(delta, "units")))
  
  invisible(rowsAffected)
}

supportsBatchUpdates <- function(connection) {
  if (!inherits(connection, "DatabaseConnectorJdbcConnection")) {
    return(FALSE)
  }
  tryCatch(
    {
      dbmsMeta <- rJava::.jcall(connection@jConnection, "Ljava/sql/DatabaseMetaData;", "getMetaData", check = FALSE)
      if (!is.jnull(dbmsMeta)) {
        if (rJava::.jcall(dbmsMeta, "Z", "supportsBatchUpdates")) {
          # inform("JDBC driver supports batch updates")
          return(TRUE)
        } else {
          inform("JDBC driver does not support batch updates. Sending updates one at a time.")
        }
      }
    },
    error = function(err) {
      inform(paste("JDBC driver 'supportsBatchUpdates' threw exception", err$message))
    }
  )
  return(FALSE)
}

#' Execute SQL code
#'
#' @description
#' This function executes SQL consisting of one or more statements.
#'
#' @template Connection
#' @param sql                 The SQL to be executed
#' @param profile             When true, each separate statement is written to file prior to sending to
#'                            the server, and the time taken to execute a statement is displayed.
#' @param progressBar         When true, a progress bar is shown based on the statements in the SQL
#'                            code.
#' @param reportOverallTime   When true, the function will display the overall time taken to execute
#'                            all statements.
#' @template ErrorReportFile
#' @param runAsBatch          When true the SQL statements are sent to the server as a single batch, and
#'                            executed there. This will be faster if you have many small SQL statements, but
#'                            there will be no progress bar, and no per-statement error messages. If the
#'                            database platform does not support batched updates the query is executed without
#'                            batching.
#'
#' @details
#' This function splits the SQL in separate statements and sends it to the server for execution. If an
#' error occurs during SQL execution, this error is written to a file to facilitate debugging.
#' Optionally, a progress bar is shown and the total time taken to execute the SQL is displayed.
#' Optionally, each separate SQL statement is written to file, and the execution time per statement is
#' shown to aid in detecting performance issues.
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
#' executeSql(conn, "CREATE TABLE x (k INT); CREATE TABLE y (k INT);")
#' disconnect(conn)
#' }
#' @export
executeSql <- function(connection,
                       sql,
                       profile = FALSE,
                       progressBar = !as.logical(Sys.getenv("TESTTHAT", unset = FALSE)),
                       reportOverallTime = TRUE,
                       errorReportFile = file.path(getwd(), "errorReportSql.txt"),
                       runAsBatch = FALSE) {
  if (inherits(connection, "DatabaseConnectorJdbcConnection") && rJava::is.jnull(connection@jConnection)) {
    abort("Connection is closed")
  }
  
  startTime <- Sys.time()
  dbms <- dbms(connection)
  
  if (inherits(connection, "DatabaseConnectorJdbcConnection") &&
      dbms == "redshift" &&
      rJava::.jcall(connection@jConnection, "Z", "getAutoCommit")) {
    # Turn off autocommit for RedShift to avoid this issue:
    # https://github.com/OHDSI/DatabaseConnector/issues/90
    trySettingAutoCommit(connection, FALSE)
    on.exit(trySettingAutoCommit(connection, TRUE))
  }
  
  batched <- runAsBatch && supportsBatchUpdates(connection)
  sqlStatements <- SqlRender::splitSql(sql)
  rowsAffected <- c()
  if (batched) {
    batchSize <- 1000
    for (start in seq(1, length(sqlStatements), by = batchSize)) {
      end <- min(start + batchSize - 1, length(sqlStatements))
      
      statement <- rJava::.jcall(connection@jConnection, "Ljava/sql/Statement;", "createStatement")
      batchSql <- c()
      for (i in start:end) {
        sqlStatement <- sqlStatements[i]
        batchSql <- c(batchSql, sqlStatement)
        rJava::.jcall(statement, "V", "addBatch", as.character(sqlStatement), check = FALSE)
      }
      if (profile) {
        SqlRender::writeSql(paste(batchSql, collapse = "\n\n"), sprintf("statements_%s_%s.sql", start, end))
      }
      logTrace(paste("Executing SQL:", truncateSql(batchSql)))
      tryCatch(
        {
          startQuery <- Sys.time()
          # InterSystems IRIS JDBC supports batch updates but does not have a separate
          # executeLargeBatch() method
          if (dbms == "iris") {
            rowsAffected <- c(rowsAffected, rJava::.jcall(statement, "[I", "executeBatch"))
          } else {
            rowsAffected <- c(rowsAffected, rJava::.jcall(statement, "[J", "executeLargeBatch"))
          }
          delta <- Sys.time() - startQuery
          if (profile) {
            inform(paste("Statements", start, "through", end, "took", delta, attr(delta, "units")))
          }
          logTrace(paste("Statements took", delta, attr(delta, "units")))
        },
        error = function(err) {
          .createErrorReport(dbms, err$message, paste(batchSql, collapse = "\n\n"), errorReportFile)
        },
        finally = {
          rJava::.jcall(statement, "V", "close")
        }
      )
    }
  } else {
    if (progressBar) {
      pb <- txtProgressBar(style = 3)
    }
    
    for (i in 1:length(sqlStatements)) {
      sqlStatement <- sqlStatements[i]
      if (profile) {
        fileConn <- file(paste("statement_", i, ".sql", sep = ""))
        writeChar(sqlStatement, fileConn, eos = NULL)
        close(fileConn)
      }
      tryCatch(
        {
          startQuery <- Sys.time()
          rowsAffected <- c(rowsAffected, lowLevelExecuteSql(connection, sqlStatement))
          delta <- Sys.time() - startQuery
          if (profile) {
            inform(paste("Statement ", i, "took", delta, attr(delta, "units")))
          }
        },
        error = function(err) {
          .createErrorReport(dbms, err$message, sqlStatement, errorReportFile)
        }
      )
      if (progressBar) {
        setTxtProgressBar(pb, i / length(sqlStatements))
      }
    }
    if (progressBar) {
      close(pb)
    }
  }
  # Spark throws error 'Cannot use commit while Connection is in auto-commit mode.'. However, also throws error when trying to set autocommit on or off:
  if (dbms != "spark" && inherits(connection, "DatabaseConnectorJdbcConnection") && !rJava::.jcall(connection@jConnection, "Z", "getAutoCommit")) {
    rJava::.jcall(connection@jConnection, "V", "commit")
  }
  
  if (reportOverallTime) {
    delta <- Sys.time() - startTime
    inform(paste("Executing SQL took", signif(delta, 3), attr(delta, "units")))
  }
    invisible(rowsAffected)
}

convertFields <- function(dbms, result) {
  if (dbms == "impala") {
    for (colname in colnames(result)) {
      if (grepl("DATE$", colname, ignore.case = TRUE)) {
        result[[colname]] <- as.Date(result[[colname]], "%Y-%m-%d")
      }
    }
  }
  if (dbms == "sqlite") {
    for (colname in colnames(result)) {
      if (grepl("DATE$", colname, ignore.case = TRUE)) {
        result[[colname]] <- as.Date(as.POSIXct(as.numeric(result[[colname]]), origin = "1970-01-01", tz = "GMT"))
      }
      if (grepl("DATETIME$", colname, ignore.case = TRUE)) {
        result[[colname]] <- as.POSIXct(as.numeric(result[[colname]]), origin = "1970-01-01", tz = "GMT")
      }
    }
  }
  if (dbms %in% c("bigquery", "snowflake")) {
    # BigQuery and Snowflake don't have INT fields, only INT64. For more consistent behavior with other
    # platforms, if it fits in an integer, convert it to an integer:
    if (ncol(result) > 0) {
      for (i in 1:ncol(result)) {
        if (bit64::is.integer64(result[[i]]) &&
            (all(is.na(result[[i]])) || (
              min(result[[i]], na.rm = TRUE) > -.Machine$integer.max &&
              max(result[[i]], na.rm = TRUE) < .Machine$integer.max))) {
          result[[i]] <- as.integer(result[[i]])
        }
      }
    }
  }
  return(result)
}

trySettingAutoCommit <- function(connection, value) {
  tryCatch(
    {
      rJava::.jcall(connection@jConnection, "V", "setAutoCommit", value)
    },
    error = function(cond) {
      # do nothing
    }
  )
}

#' Retrieve data to a data.frame
#'
#' @description
#' This function sends SQL to the server, and returns the results.
#'
#' @template Connection
#' @param sql                  The SQL to be send.
#' @template ErrorReportFile
#' @template SnakeCaseToCamelCase
#' @template IntegerAsNumeric
#'
#' @details
#' This function sends the SQL to the server and retrieves the results. If an error occurs during SQL
#' execution, this error is written to a file to facilitate debugging. Null values in the database are converted
#' to NA values in R.
#'
#' @return
#' A data frame.
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
#' count <- querySql(conn, "SELECT COUNT(*) FROM person")
#' disconnect(conn)
#' }
#' @export
querySql <- function(connection,
                     sql,
                     errorReportFile = file.path(getwd(), "errorReportSql.txt"),
                     snakeCaseToCamelCase = FALSE,
                     integerAsNumeric = getOption("databaseConnectorIntegerAsNumeric", default = TRUE),
                     integer64AsNumeric = getOption("databaseConnectorInteger64AsNumeric", default = TRUE)) {
  if (inherits(connection, "DatabaseConnectorJdbcConnection") && rJava::is.jnull(connection@jConnection)) {
    abort("Connection is closed")
  }
  # Calling splitSql, because this will also strip trailing semicolons (which cause Oracle to crash).
  sqlStatements <- SqlRender::splitSql(sql)
  if (length(sqlStatements) > 1) {
    abort(paste(
      "A query that returns a result can only consist of one SQL statement, but",
      length(sqlStatements),
      "statements were found"
    ))
  }
  tryCatch(
    {
      result <- lowLevelQuerySql(
        connection = connection,
        query = sqlStatements[1],
        integerAsNumeric = integerAsNumeric,
        integer64AsNumeric = integer64AsNumeric
      )
      colnames(result) <- toupper(colnames(result))
      result <- convertFields(dbms(connection), result)
      if (snakeCaseToCamelCase) {
        colnames(result) <- SqlRender::snakeCaseToCamelCase(colnames(result))
      }
      return(result)
    },
    error = function(err) {
      .createErrorReport(dbms(connection), err$message, sql, errorReportFile)
    }
  )
}

#' Render, translate, execute SQL code
#'
#' @description
#' This function renders, translates, and executes SQL consisting of one or more statements.
#'
#' @template Connection
#' @param sql                 The SQL to be executed
#' @param profile             When true, each separate statement is written to file prior to sending to
#'                            the server, and the time taken to execute a statement is displayed.
#' @param progressBar         When true, a progress bar is shown based on the statements in the SQL
#'                            code.
#' @param reportOverallTime   When true, the function will display the overall time taken to execute
#'                            all statements.
#' @template ErrorReportFile
#' @param runAsBatch          When true the SQL statements are sent to the server as a single batch, and
#'                            executed there. This will be faster if you have many small SQL statements, but
#'                            there will be no progress bar, and no per-statement error messages. If the
#'                            database platform does not support batched updates the query is executed as
#'                            ordinarily.
#' @template TempEmulationSchema
#' @param ...                 Parameters that will be used to render the SQL.
#'
#' @details
#' This function calls the `render` and `translate` functions in the `SqlRender` package before
#' calling [executeSql()].
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
#' renderTranslateExecuteSql(connection,
#'   sql = "SELECT * INTO #temp FROM @@schema.person;",
#'   schema = "cdm_synpuf"
#' )
#' disconnect(conn)
#' }
#' @export
renderTranslateExecuteSql <- function(connection,
                                      sql,
                                      profile = FALSE,
                                      progressBar = TRUE,
                                      reportOverallTime = TRUE,
                                      errorReportFile = file.path(getwd(), "errorReportSql.txt"),
                                      runAsBatch = FALSE,
                                      oracleTempSchema = NULL,
                                      tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
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
  sql <- SqlRender::translate(sql, targetDialect = dbms(connection), tempEmulationSchema = tempEmulationSchema)
  executeSql(
    connection = connection,
    sql = sql,
    profile = profile,
    progressBar = progressBar,
    reportOverallTime = reportOverallTime,
    errorReportFile = errorReportFile,
    runAsBatch = runAsBatch
  )
}

#' Render, translate, and query to data.frame
#'
#' @description
#' This function renders, and translates SQL, sends it to the server, and returns the results as a data.frame.
#'
#' @template Connection
#' @param sql                  The SQL to be send.
#' @template ErrorReportFile
#' @template SnakeCaseToCamelCase
#' @template TempEmulationSchema
#' @template IntegerAsNumeric
#' @param ...                  Parameters that will be used to render the SQL.
#'
#' @details
#' This function calls the `render` and `translate` functions in the `SqlRender` package before
#' calling [querySql()].
#'
#' @return
#' A data frame.
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
#' persons <- renderTranslatequerySql(conn,
#'   sql = "SELECT TOP 10 * FROM @@schema.person",
#'   schema = "cdm_synpuf"
#' )
#' disconnect(conn)
#' }
#' @export
renderTranslateQuerySql <- function(connection,
                                    sql,
                                    errorReportFile = file.path(getwd(), "errorReportSql.txt"),
                                    snakeCaseToCamelCase = FALSE,
                                    oracleTempSchema = NULL,
                                    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                    integerAsNumeric = getOption("databaseConnectorIntegerAsNumeric", default = TRUE),
                                    integer64AsNumeric = getOption("databaseConnectorInteger64AsNumeric", default = TRUE),
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
  sql <- SqlRender::translate(sql, targetDialect = dbms(connection), tempEmulationSchema = tempEmulationSchema)
  return(querySql(
    connection = connection,
    sql = sql,
    errorReportFile = errorReportFile,
    snakeCaseToCamelCase = snakeCaseToCamelCase,
    integerAsNumeric = integerAsNumeric,
    integer64AsNumeric = integer64AsNumeric
  ))
}


#' Test a character vector of SQL names for SQL reserved words
#'
#' This function checks a character vector against a predefined list of reserved SQL words.
#'
#' @param sqlNames A character vector containing table or field names to check.
#' @param warn (logical) Should a warn be thrown if invalid SQL names are found?
#'
#' @return A logical vector with length equal to sqlNames that is TRUE for each name that is reserved and FALSE otherwise
#'
#' @export
isSqlReservedWord <- function(sqlNames, warn = FALSE) {
  if (!is.character(sqlNames)) {
    abort("sqlNames should be a character vector")
  }
  sqlNames <- gsub("^#", "", sqlNames)
  sqlReservedWords <- read.csv(system.file("csv", "sqlReservedWords.csv", package = "DatabaseConnector"), stringsAsFactors = FALSE)
  nameIsReserved <- toupper(sqlNames) %in% sqlReservedWords$reservedWords
  badSqlNames <- sqlNames[nameIsReserved]
  if (length(badSqlNames == 1) & warn) {
    warn(paste(badSqlNames, "is a reserved keyword in SQL and should not be used as a table or field name."))
  } else if (length(badSqlNames) > 1 & warn) {
    warn(paste(paste(badSqlNames, collapse = ","), "are reserved keywords in SQL and should not be used as table or field names."))
  }
  return(nameIsReserved)
}

#' Render, translate, and perform process to batches of data.
#'
#' @description
#' This function renders, and translates SQL, sends it to the server, processes the data in batches with a call back
#' function. Note that this function should perform a row-wise operation. This is designed to work with massive data
#' that won't fit in to memory.
#'
#' The batch sizes are determined by the java virtual machine and will depend on the data.
#'
#' @template Connection
#' @param sql                  The SQL to be send.
#' @param fun                  Function to apply to batch. Must take data.frame and integer position as parameters.
#' @param args                 List of arguments to be passed to function call.
#' @template ErrorReportFile
#' @template SnakeCaseToCamelCase
#' @template TempEmulationSchema
#' @template IntegerAsNumeric
#' @param ...                  Parameters that will be used to render the SQL.
#'
#' @details
#' This function calls the `render` and `translate` functions in the `SqlRender` package before
#' calling [querySql()].
#'
#' @return
#' Invisibly returns a list of outputs from each call to the provided function.
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
#' connection <- connect(connectionDetails)
#'
#' # First example: write data to a large CSV file:
#' filepath <- "myBigFile.csv"
#' writeBatchesToCsv <- function(data, position, ...) {
#'   write.csv(data, filepath, append = position != 1)
#'   return(NULL)
#' }
#' renderTranslateQueryApplyBatched(connection,
#'   "SELECT * FROM @schema.person;",
#'   schema = "cdm_synpuf",
#'   fun = writeBatchesToCsv
#' )
#'
#' # Second example: write data to Andromeda
#' # (Alternative to querySqlToAndromeda if some local computation needs to be applied)
#' bigResults <- Andromeda::andromeda()
#' writeBatchesToAndromeda <- function(data, position, ...) {
#'   data$p <- EmpiricalCalibration::computeTraditionalP(data$logRr, data$logSeRr)
#'   if (position == 1) {
#'     bigResults$rrs <- data
#'   } else {
#'     Andromeda::appendToTable(bigResults$rrs, data)
#'   }
#'   return(NULL)
#' }
#' sql <- "SELECT target_id, comparator_id, log_rr, log_se_rr FROM @schema.my_results;"
#' renderTranslateQueryApplyBatched(connection,
#'   sql,
#'   fun = writeBatchesToAndromeda,
#'   schema = "my_results",
#'   snakeCaseToCamelCase = TRUE
#' )
#'
#' disconnect(connection)
#' }
#'
#' @export
renderTranslateQueryApplyBatched <- function(connection,
                                             sql,
                                             fun,
                                             args = list(),
                                             errorReportFile = file.path(getwd(), "errorReportSql.txt"),
                                             snakeCaseToCamelCase = FALSE,
                                             oracleTempSchema = NULL,
                                             tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                             integerAsNumeric = getOption("databaseConnectorIntegerAsNumeric", default = TRUE),
                                             integer64AsNumeric = getOption("databaseConnectorInteger64AsNumeric", default = TRUE),
                                             ...) {
  UseMethod("renderTranslateQueryApplyBatched", connection)
}

#' @export
renderTranslateQueryApplyBatched.default <- function(connection,
                                                     sql,
                                                     fun,
                                                     args = list(),
                                                     errorReportFile = file.path(getwd(), "errorReportSql.txt"),
                                                     snakeCaseToCamelCase = FALSE,
                                                     oracleTempSchema = NULL,
                                                     tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                                     integerAsNumeric = getOption("databaseConnectorIntegerAsNumeric", default = TRUE),
                                                     integer64AsNumeric = getOption("databaseConnectorInteger64AsNumeric", default = TRUE),
                                                     ...) {
  if (!is.function(fun)) {
    abort("fun argument must be a function")
  }
  if (!is.null(oracleTempSchema) && oracleTempSchema != "") {
    warn("The 'oracleTempSchema' argument is deprecated. Use 'tempEmulationSchema' instead.",
         .frequency = "regularly",
         .frequency_id = "oracleTempSchema"
    )
    tempEmulationSchema <- oracleTempSchema
  }
  sql <- SqlRender::render(sql, ...)
  sql <- SqlRender::translate(sql, targetDialect = dbms(connection), tempEmulationSchema = tempEmulationSchema)
  sql <- SqlRender::splitSql(sql)
  if (length(sql) > 1) {
    abort(paste(
      "A query that returns a result can only consist of one SQL statement, but",
      length(sql),
      "statements were found"
    ))
  }
  tryCatch(
    {
      queryResult <- dbSendQuery(connection, sql)
    },
    error = function(err) {
      .createErrorReport(dbms(connection), err$message, sql, errorReportFile)
    }
  )
  on.exit(dbClearResult(queryResult))
  
  columnTypes <- rJava::.jcall(queryResult@content, "[I", "getColumnTypes")
  if (any(columnTypes == 5)) {
    validateInt64Query()
  }
  
  results <- list()
  position <- 1
  while (!rJava::.jcall(queryResult@content, "Z", "isDone")) {
    batch <- dbFetch(queryResult,
                     columnTypes = columnTypes,
                     integerAsNumeric = integerAsNumeric,
                     integer64AsNumeric = integer64AsNumeric
    )
    if (snakeCaseToCamelCase) {
      colnames(batch) <- SqlRender::snakeCaseToCamelCase(colnames(batch))
    }
    rowCount <- nrow(batch)
    if (rowCount > 0) {
      result <- do.call(fun, append(list(batch, position), args))
      results[[length(results) + 1]] <- result
    }
    position <- position + rowCount
  }
  invisible(results)
}


#' @export
renderTranslateQueryApplyBatched.DatabaseConnectorDbiConnection <- function(connection,
                                                                            sql,
                                                                            fun,
                                                                            args = list(),
                                                                            errorReportFile = file.path(getwd(), "errorReportSql.txt"),
                                                                            snakeCaseToCamelCase = FALSE,
                                                                            oracleTempSchema = NULL,
                                                                            tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                                                            integerAsNumeric = getOption("databaseConnectorIntegerAsNumeric", default = TRUE),
                                                                            integer64AsNumeric = getOption("databaseConnectorInteger64AsNumeric", default = TRUE),
                                                                            ...) {
  if (!is.function(fun)) {
    abort("fun argument must be a function")
  }
  if (!is.null(oracleTempSchema) && oracleTempSchema != "") {
    warn("The 'oracleTempSchema' argument is deprecated. Use 'tempEmulationSchema' instead.",
         .frequency = "regularly",
         .frequency_id = "oracleTempSchema"
    )
    tempEmulationSchema <- oracleTempSchema
  }
  
  sql <- SqlRender::render(sql, ...)
  sql <- SqlRender::translate(sql, targetDialect = dbms(connection), tempEmulationSchema = tempEmulationSchema)
  sql <- SqlRender::splitSql(sql)
  if (length(sql) > 1) {
    abort(paste(
      "A query that returns a result can only consist of one SQL statement, but",
      length(sql),
      "statements were found"
    ))
  }
  results <- lowLevelQuerySql(connection,
                              sql,
                              integerAsNumeric = integerAsNumeric,
                              integer64AsNumeric = integer64AsNumeric
  )
  if (snakeCaseToCamelCase) {
    colnames(results) <- SqlRender::snakeCaseToCamelCase(colnames(results))
  }
  
  # Note that the DBI connection implementation only ever processes a single batch
  position <- 1
  results <- list(do.call(fun, append(list(results, position), args)))
  invisible(results)
}


#' Drop all emulated temp tables.
#'
#' @description
#' On some DBMSs, like Oracle and BigQuery, `DatabaseConnector` through `SqlRender` emulates temp tables
#' in a schema provided by the user. Ideally, these tables are deleted by the application / R script creating them,
#' but for various reasons orphan temp tables may remain. This function drops all emulated temp tables created in this
#' session only.
#'
#' @template Connection
#' @param tempEmulationSchema  Some database platforms like Oracle and Impala do not truly support temp tables. To
#'                             emulate temp tables, provide a schema with write privileges where temp tables
#'                             can be created.
#'
#' @return
#' Invisibly returns the list of deleted emulated temp tables.
#'
#' @export
dropEmulatedTempTables <- function(connection,
                                   tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")) {
  if (is(connection, "Pool")) {
    connection <- pool::poolCheckout(connection)
    on.exit(pool::poolReturn(connection))
  }
  if (!requiresTempEmulation(dbms(connection))) {
    # No temp tables emulated: do nothing
    return()
  }
  if (is.null(tempEmulationSchema))
    abort("The `tempEmulationSchema` must be specified.")
  prefix <- SqlRender::getTempTablePrefix()
  tableNames <- getTableNames(connection, tempEmulationSchema)
  tableNames <- tableNames[grepl(sprintf("^%s", prefix), tableNames, ignore.case = TRUE)]
  if (length(tableNames) > 0) {
    inform(sprintf("Dropping tables '%s' from schema '%s'.", paste(tableNames, collapse = "', '"), tempEmulationSchema))
    tableNames <- tolower(paste(tempEmulationSchema, tableNames, sep = "."))
    if (dbms(connection) == "spark") {
      sql <- paste(sprintf("DROP TABLE %s;", tableNames), collapse = "\n")
    } else {
      sql <- paste(sprintf("TRUNCATE TABLE %s; DROP TABLE %s;", tableNames, tableNames), collapse = "\n")
    }
    sql <- SqlRender::translate(sql, dbms(connection))
    executeSql(connection, sql)
  }
  invisible(tableNames)
}

#' Does the DBMS require temp table emulation?
#'
#' @param dbms The type of DBMS running on the server. See [connect()] or [createConnectionDetails()] for 
#' valid values.
#'
#' @return
#' TRUE if the DBMS requires temp table emulation, FALSE otherwise.
#'
#' @examples
#' requiresTempEmulation("postgresql")
#' requiresTempEmulation("oracle")
#' 
#' @export
requiresTempEmulation <- function(dbms){
  return(dbms %in% c("oracle", "spark", "impala", "bigquery", "snowflake"))
}

#' Assert the temp emulation schema is set
#'
#' @description 
#' Asserts the temp emulation schema is set for DBMSs requiring temp table emulation. 
#' 
#' If you know your code uses temp tables, it is a good idea to call this function first,
#' so it can throw an informative error if the user forgot to set the temp emulation schema.
#' 
#' @param dbms                The type of DBMS running on the server. See [connect()] or 
#'                            [createConnectionDetails()] for valid values.
#' @param tempEmulationSchema The temp emulation schema specified by the user. 
#'
#' @return
#' Does not return anything. Throws an error if the DBMS requires temp emulation but the 
#' temp emulation schema is not set.
#' 
#' @export
assertTempEmulationSchemaSet <- function(dbms,
                                         tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")) {
  if (requiresTempEmulation(dbms) && (is.null(tempEmulationSchema) || tempEmulationSchema == "")) {
    rlang::abort(c(
      sprintf("Temp table emulation is required for %s but the temp emulation schema is not set.", dbms),
      "i" = "Please use options(sqlRenderTempEmulationSchema = \"some_schema\") to specify a schema where you have write access."
    ))
  }
  invisible(NULL)
}
