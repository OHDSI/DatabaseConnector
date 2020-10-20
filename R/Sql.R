# @file Sql.R
#
# Copyright 2020 Observational Health Data Sciences and Informatics
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
  for (pkg in si$otherPkgs) lines <- c(lines,
                                       paste("- ", pkg$Package, " (", pkg$Version, ")", sep = ""))
  return(paste(lines, collapse = "\n"))
}

.createErrorReport <- function(dbms, message, sql, fileName) {
  report <- c("DBMS:\n", dbms, "\n\nError:\n", message, "\n\nSQL:\n", sql, "\n\n", .systemInfo())
  fileConn <- file(fileName)
  writeChar(report, fileConn, eos = NULL)
  close(fileConn)
  stop(paste("Error executing SQL:",
             message,
             paste("An error report has been created at ", fileName),
             sep = "\n"), call. = FALSE)
}

validateInt64Query <- function() {
  # Validate that communication of 64-bit integers with Java is correct:
  array <- rJava::J("org.ohdsi.databaseConnector.BatchedQuery")$validateInteger64()
  oldClass(array) <- "integer64"
  if (!all.equal(array, bit64::as.integer64(c(1, -1, 8589934592, -8589934592)))) {
    stop("Error converting 64-bit integers between R and Java")
  }
}


#' Low level function for retrieving data to a data frame
#'
#' @description
#' This is the equivalent of the \code{\link{querySql}} function, except no error report is written
#' when an error occurs.
#'
#' @param connection      The connection to the database server.
#' @param query           The SQL statement to retrieve the data
#' @param datesAsString   Should dates be imported as character vectors, our should they be converted
#'                        to R's date format?
#'
#' @details
#' Retrieves data from the database server and stores it in a data frame. Null values in the database are converted
#' to NA values in R.
#'
#' @return
#' A data frame containing the data retrieved from the server
#'
#' @export
lowLevelQuerySql <- function(connection, query, datesAsString = FALSE) {
  UseMethod("lowLevelQuerySql", connection)
}

#' @export
lowLevelQuerySql.default <- function(connection, query, datesAsString = FALSE) {
  if (rJava::is.jnull(connection@jConnection))
    stop("Connection is closed")
  
  
  batchedQuery <- rJava::.jnew("org.ohdsi.databaseConnector.BatchedQuery",
                               connection@jConnection,
                               query)
  
  on.exit(rJava::.jcall(batchedQuery, "V", "clear"))
  
  columnTypes <- rJava::.jcall(batchedQuery, "[I", "getColumnTypes")
  if (any(columnTypes == 5)) {
    validateInt64Query()
  }
  columns <- vector("list", length(columnTypes))
  while (!rJava::.jcall(batchedQuery, "Z", "isDone")) {
    rJava::.jcall(batchedQuery, "V", "fetchBatch")
    for (i in seq.int(length(columnTypes))) {
      if (columnTypes[i] == 1) {
        column <- rJava::.jcall(batchedQuery,
                                "[D",
                                "getNumeric",
                                as.integer(i))
        # rJava doesn't appear to be able to return NAs, so converting NaNs to NAs:
        column[is.nan(column)] <- NA
        columns[[i]] <- c(columns[[i]], column)
      } else if (columnTypes[i] == 5) {
        columns[[i]] <- rJava::.jcall(batchedQuery,
                                "[D",
                                "getInteger64",
                                as.integer(i)) 
        oldClass(columns[[i]]) <- "integer64"
      } else if (columnTypes[i] == 6) {
        columns[[i]] <- rJava::.jcall(batchedQuery,
                                "[I",
                                "getInteger",
                                as.integer(i))
      } else {
        columns[[i]] <- c(columns[[i]],
                          rJava::.jcall(batchedQuery, "[Ljava/lang/String;", "getString", i))
      }
    }
  }
  if (!datesAsString) {
    for (i in seq.int(length(columnTypes))) {
      if (columnTypes[i] == 3) {
        columns[[i]] <- as.Date(columns[[i]])
      } else if (columnTypes[i] == 4) {
        columns[[i]] <- as.POSIXct(columns[[i]])
      }
    }
  }
  names(columns) <- rJava::.jcall(batchedQuery, "[Ljava/lang/String;", "getColumnNames")
  attr(columns, "row.names") <- c(NA_integer_, length(columns[[1]]))
  class(columns) <- "data.frame"
  return(columns)
}

#' @export
lowLevelQuerySql.DatabaseConnectorDbiConnection <- function(connection, query, datesAsString = FALSE) {
  return(DBI::dbGetQuery(connection@dbiConnection, query))
}

#' Execute SQL code
#'
#' @description
#' This function executes a single SQL statement.
#'
#' @param connection   The connection to the database server.
#' @param sql          The SQL to be executed
#'
#' @export
lowLevelExecuteSql <- function(connection, sql) {
  UseMethod("lowLevelExecuteSql", connection)
}

delayIfNecessary <- function(sql, regex, executionTimeList, threshold) {
  regexGroups <- stringr::str_match(sql, stringr::regex(regex, ignore_case = TRUE))
  tableName <- regexGroups[3]
  if (!is.na(tableName) && !is.null(tableName)) {
    currentTime <- Sys.time();
    lastExecutedTime <- executionTimeList[[tableName]]
    if (!is.na(lastExecutedTime) && !is.null(lastExecutedTime)) {
      delta <- currentTime - lastExecutedTime
      if (delta < threshold) {
        Sys.sleep(threshold - delta)
      }
    }
    
    executionTimeList[[tableName]] = currentTime
  }
  return(executionTimeList)
}

delayIfNecessaryForDdl <- function(sql) {
  ddlList <- getOption("ddlList")
  if (is.null(ddlList)) {
    ddlList <- list()
  }
  
  regexForDdl = "(^CREATE\\s+TABLE\\s+IF\\s+EXISTS|^CREATE\\s+TABLE|^DROP\\s+TABLE\\s+IF\\s+EXISTS|^DROP\\s+TABLE)\\s+([a-zA-Z0-9_$#-]*\\.?\\s*(?:[a-zA-Z0-9_]+)*)"
  updatedList <- delayIfNecessary(sql, regexForDdl, ddlList, 5);
  options(ddlList = updatedList)
}

delayIfNecessaryForInsert <- function(sql) {
  insetList <- getOption("insetList")
  if (is.null(insetList)) {
    insetList <- list()
  }
  
  regexForInsert = "(^INSERT\\s+INTO)\\s+([a-zA-Z0-9_$#-]*\\.?\\s*(?:[a-zA-Z0-9_]+)*)"
  updatedList <- delayIfNecessary(sql, regexForInsert, insetList, 5);
  options(insetList = updatedList)
}

#' @export
lowLevelExecuteSql.default <- function(connection, sql) {
  statement <- rJava::.jcall(connection@jConnection, "Ljava/sql/Statement;", "createStatement")
  on.exit(rJava::.jcall(statement, "V", "close"))
  hasResultSet <- rJava::.jcall(statement, "Z", "execute", as.character(sql), check = FALSE)
  
  if (connection@dbms == "bigquery") {
    delayIfNecessaryForDdl(sql)
    delayIfNecessaryForInsert(sql)
  }
  
  rowsAffected <- 0
  if (!hasResultSet) {
    rowsAffected <- rJava::.jcall(statement, "I", "getUpdateCount", check = FALSE)
  }
  invisible(rowsAffected)
}

#' @export
lowLevelExecuteSql.DatabaseConnectorDbiConnection <- function(connection, sql) {
  rowsAffected <- DBI::dbExecute(connection@dbiConnection, sql)
  invisible(rowsAffected)
}

supportsBatchUpdates <- function(connection) {
  if (!inherits(connection, "DatabaseConnectorJdbcConnection")) {
    return(FALSE)
  }
  tryCatch({
    dbmsMeta <- rJava::.jcall(connection@jConnection, "Ljava/sql/DatabaseMetaData;", "getMetaData", check = FALSE)
    if (!is.jnull(dbmsMeta)) {
      if (rJava::.jcall(dbmsMeta, "Z", "supportsBatchUpdates")) {
        # writeLines("JDBC driver supports batch updates")
        return(TRUE);
      } else {
        writeLines("JDBC driver does not support batch updates. Sending updates one at a time.")
      }
    }
  }, error = function(err) {
    writeLines(paste("JDBC driver 'supportsBatchUpdates' threw exception", err$message))
  })
  return(FALSE);
}

#' Execute SQL code
#'
#' @description
#' This function executes SQL consisting of one or more statements.
#'
#' @param connection          The connection to the database server.
#' @param sql                 The SQL to be executed
#' @param profile             When true, each separate statement is written to file prior to sending to
#'                            the server, and the time taken to execute a statement is displayed.
#' @param progressBar         When true, a progress bar is shown based on the statements in the SQL
#'                            code.
#' @param reportOverallTime   When true, the function will display the overall time taken to execute
#'                            all statements.
#' @param errorReportFile     The file where an error report will be written if an error occurs. Defaults to
#'                            'errorReportSql.txt' in the current working directory.
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
#' connectionDetails <- createConnectionDetails(dbms = "postgresql",
#'                                              server = "localhost",
#'                                              user = "root",
#'                                              password = "blah",
#'                                              schema = "cdm_v4")
#' conn <- connect(connectionDetails)
#' executeSql(conn, "CREATE TABLE x (k INT); CREATE TABLE y (k INT);")
#' disconnect(conn)
#' }
#' @export
executeSql <- function(connection,
                       sql,
                       profile = FALSE,
                       progressBar = TRUE,
                       reportOverallTime = TRUE, 
                       errorReportFile = file.path(getwd(), "errorReportSql.txt"),
                       runAsBatch = FALSE) {
  if (inherits(connection, "DatabaseConnectorJdbcConnection") && rJava::is.jnull(connection@jConnection))
    stop("Connection is closed")
  
  startTime <- Sys.time()
  
  if (inherits(connection, "DatabaseConnectorJdbcConnection") && 
      connection@dbms == "redshift" &&
      rJava::.jcall(connection@jConnection, "Z", "getAutoCommit")) {
    # Turn off autocommit for RedShift to avoid this issue:
    # https://github.com/OHDSI/DatabaseConnector/issues/90
    trySettingAutoCommit(connection, FALSE)
    on.exit(trySettingAutoCommit(connection, TRUE))
  }
  
  batched <- runAsBatch && supportsBatchUpdates(connection)
  sqlStatements <- SqlRender::splitSql(sql)
  if (batched) {
    batchSize <- 1000
    rowsAffected <- 0
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
      tryCatch({
        startQuery <- Sys.time()
        rowsAffected <- c(rowsAffected, rJava::.jcall(statement, "[I", "executeBatch"))
        delta <- Sys.time() - startQuery
        writeLines(paste("Statements", start, "through", end,  "took", delta, attr(delta, "units")))
      }, error = function(err) {
        .createErrorReport(connection@dbms, err$message, paste(batchSql, collapse = "\n\n"), errorReportFile)
      }, finally = {rJava::.jcall(statement, "V", "close")})
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
      tryCatch({
        startQuery <- Sys.time()
        lowLevelExecuteSql(connection, sqlStatement)
        if (profile) {
          delta <- Sys.time() - startQuery
          writeLines(paste("Statement ", i, "took", delta, attr(delta, "units")))
        }
      }, error = function(err) {
        .createErrorReport(connection@dbms, err$message, sqlStatement, errorReportFile)
      })
      if (progressBar) {
        setTxtProgressBar(pb, i/length(sqlStatements))
      }
    }
    if (progressBar) {
      close(pb)
    }
  }
  
  if (inherits(connection, "DatabaseConnectorJdbcConnection") && !rJava::.jcall(connection@jConnection, "Z", "getAutoCommit")) {
    rJava::.jcall(connection@jConnection, "V", "commit")
  }

  if (reportOverallTime) {
    delta <- Sys.time() - startTime
    writeLines(paste("Executing SQL took", signif(delta, 3), attr(delta, "units")))
  }
  if (batched) {
    invisible(rowsAffected)
  }
}

convertFields <- function(dbms, result) {
  if (dbms == "impala") {
    for (colname in colnames(result)) {
      if (grepl("DATE", colname)) {
        result[[colname]] <- as.Date(result[[colname]], "%Y-%m-%d")
      }
    }
  }
  if (dbms == "sqlite") {
    for (colname in colnames(result)) {
      if (grepl("DATE$", colname)) {
        result[[colname]] <- as.Date(as.POSIXct(result[[colname]], origin = "1970-01-01", tz = "GMT"))
      }
      if (grepl("DATETIME$", colname)) {
        result[[colname]] <- as.POSIXct(result[[colname]], origin = "1970-01-01", tz = "GMT")
      }
      
    }
  } 
  return(result)
}

trySettingAutoCommit <- function(connection, value) {
  tryCatch({
    rJava::.jcall(connection@jConnection, "V", "setAutoCommit", value)
  }, error = function(cond) {
    # do nothing
  })
}

#' Retrieve data to a data.frame
#'
#' @description
#' This function sends SQL to the server, and returns the results.
#'
#' @param connection           The connection to the database server.
#' @param sql                  The SQL to be send.
#' @param errorReportFile      The file where an error report will be written if an error occurs. Defaults to
#'                             'errorReportSql.txt' in the current working directory.
#' @param snakeCaseToCamelCase If true, field names are assumed to use snake_case, and are converted to camelCase.  
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
#' connectionDetails <- createConnectionDetails(dbms = "postgresql",
#'                                              server = "localhost",
#'                                              user = "root",
#'                                              password = "blah",
#'                                              schema = "cdm_v4")
#' conn <- connect(connectionDetails)
#' count <- querySql(conn, "SELECT COUNT(*) FROM person")
#' disconnect(conn)
#' }
#' @export
querySql <- function(connection, sql, errorReportFile = file.path(getwd(), "errorReportSql.txt"), snakeCaseToCamelCase = FALSE) {
  if (inherits(connection, "DatabaseConnectorJdbcConnection") && rJava::is.jnull(connection@jConnection))
    stop("Connection is closed")
  # Calling splitSql, because this will also strip trailing semicolons (which cause Oracle to crash).
  sqlStatements <- SqlRender::splitSql(sql)
  if (length(sqlStatements) > 1)
    stop(paste("A query that returns a result can only consist of one SQL statement, but",
               length(sqlStatements),
               "statements were found"))
  tryCatch({
    result <- lowLevelQuerySql(connection, sqlStatements[1])
    colnames(result) <- toupper(colnames(result))
    result <- convertFields(connection@dbms, result)
    if (snakeCaseToCamelCase) {
      colnames(result) <- SqlRender::snakeCaseToCamelCase(colnames(result))
    }
    return(result)
  }, error = function(err) {
    .createErrorReport(connection@dbms, err$message, sql, errorReportFile)
  })
}

#' Render, translate, execute SQL code
#'
#' @description
#' This function renders, translates, and executes SQL consisting of one or more statements.
#'
#' @param connection          The connection to the database server.
#' @param sql                 The SQL to be executed
#' @param profile             When true, each separate statement is written to file prior to sending to
#'                            the server, and the time taken to execute a statement is displayed.
#' @param progressBar         When true, a progress bar is shown based on the statements in the SQL
#'                            code.
#' @param reportOverallTime   When true, the function will display the overall time taken to execute
#'                            all statements.
#' @param errorReportFile     The file where an error report will be written if an error occurs. Defaults to
#'                            'errorReportSql.txt' in the current working directory.
#' @param runAsBatch          When true the SQL statements are sent to the server as a single batch, and 
#'                            executed there. This will be faster if you have many small SQL statements, but
#'                            there will be no progress bar, and no per-statement error messages. If the 
#'                            database platform does not support batched updates the query is executed as 
#'                            ordinarily.
#' @param oracleTempSchema    A schema that can be used to create temp tables in when using Oracle or Impala.
#' @param ...                 Parameters that will be used to render the SQL.
#'
#' @details
#' This function calls the \code{render} and \code{translate} functions in the SqlRender package before 
#' calling \code{\link{executeSql}}.
#'
#' @examples
#' \dontrun{
#' connectionDetails <- createConnectionDetails(dbms = "postgresql",
#'                                              server = "localhost",
#'                                              user = "root",
#'                                              password = "blah",
#'                                              schema = "cdm_v4")
#' conn <- connect(connectionDetails)
#' renderTranslateExecuteSql(connection, 
#'                           sql = "SELECT * INTO #temp FROM @@schema.person;",
#'                           schema = "cdm_synpuf")
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
                                      ...) {
  sql <- SqlRender::render(sql, ...)
  sql <- SqlRender::translate(sql, targetDialect = connection@dbms, oracleTempSchema = oracleTempSchema)
  executeSql(connection = connection,
             sql = sql,
             profile = profile,
             progressBar = progressBar,
             reportOverallTime = reportOverallTime,
             errorReportFile = errorReportFile,
             runAsBatch = runAsBatch)
}

#' Render, translate, and query to data.frame
#'
#' @description
#' This function renders, and translates SQL, sends it to the server, and returns the results as a data.frame.
#'
#' @param connection           The connection to the database server.
#' @param sql                  The SQL to be send.
#' @param errorReportFile      The file where an error report will be written if an error occurs. Defaults to
#'                             'errorReportSql.txt' in the current working directory.
#' @param snakeCaseToCamelCase If true, field names are assumed to use snake_case, and are converted to camelCase.  
#' @param oracleTempSchema     A schema that can be used to create temp tables in when using Oracle or Impala.
#' @param ...                  Parameters that will be used to render the SQL.
#'
#' @details
#' This function calls the \code{render} and \code{translate} functions in the SqlRender package before 
#' calling \code{\link{querySql}}.
#'
#' @return
#' A data frame.
#'
#' @examples
#' \dontrun{
#' connectionDetails <- createConnectionDetails(dbms = "postgresql",
#'                                              server = "localhost",
#'                                              user = "root",
#'                                              password = "blah",
#'                                              schema = "cdm_v4")
#' conn <- connect(connectionDetails)
#' persons <- renderTranslatequerySql(conn, 
#'                                    sql = "SELECT TOP 10 * FROM @@schema.person",
#'                                    schema = "cdm_synpuf")
#' disconnect(conn)
#' }
#' @export
renderTranslateQuerySql <- function(connection, 
                                    sql, 
                                    errorReportFile = file.path(getwd(), "errorReportSql.txt"), 
                                    snakeCaseToCamelCase = FALSE,
                                    oracleTempSchema = NULL,
                                    ...) {
  sql <- SqlRender::render(sql, ...)
  sql <- SqlRender::translate(sql, targetDialect = connection@dbms, oracleTempSchema = oracleTempSchema)
  return(querySql(connection = connection,
                  sql = sql,
                  errorReportFile = errorReportFile,
                  snakeCaseToCamelCase = snakeCaseToCamelCase))
}


#' Test a character vector of SQL names for SQL reserved words
#' 
#' This function checks a character vector against a predefined list of reserved SQL words.
#'
#' @param sqlNames A character vector containing table or field names to check.
#' @param warn (logical) Should a warning be thrown if invalid SQL names are found?
#'
#' @return A logical vector with length equal to sqlNames that is TRUE for each name that is reserved and FALSE otherwise
#' 
#' @export
isSqlReservedWord <- function(sqlNames, warn = FALSE){
  stopifnot(is.character(sqlNames))
  sqlNames <- gsub("^#", "", sqlNames)
  sqlReservedWords <- read.csv(system.file("csv", "sqlReservedWords.csv", package = "DatabaseConnector"), stringsAsFactors = FALSE)
  nameIsReserved <- toupper(sqlNames) %in% sqlReservedWords$reservedWords
  badSqlNames <- sqlNames[nameIsReserved]
  if (length(badSqlNames == 1) & warn) {
    warning(paste(badSqlNames, "is a reserved keyword in SQL and should not be used as a table or field name."))
  } else if (length(badSqlNames) > 1 & warn) {
    warning(paste(paste(badSqlNames, collapse = ","), "are reserved keywords in SQL and should not be used as table or field names."))
  } 
  return(nameIsReserved)
}
