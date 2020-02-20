# @file Sql.R
#
# Copyright 2019 Observational Health Data Sciences and Informatics
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

as.POSIXct.ff_vector <- function(x, ...) {
  chunks <- bit::chunk(x)
  i <- chunks[[1]]
  result <- ff::as.ff(as.POSIXct(x[i], ...))
  for (i in chunks[-1]) {
    result <- ffbase::ffappend(result, ff::as.ff(as.POSIXct(x[i], ...)))
  }
  return(result)
}

#' Low level function for retrieving data to an ffdf object
#'
#' @description
#' This is the equivalent of the \code{\link{querySql.ffdf}} function, except no error report is
#' written when an error occurs.
#'
#' @param connection      The connection to the database server.
#' @param query           The SQL statement to retrieve the data
#' @param datesAsString   Should dates be imported as character vectors, our should they be converted
#'                        to R's date format?
#'
#' @details
#' Retrieves data from the database server and stores it in an ffdf object. This allows very large
#' data sets to be retrieved without running out of memory. Null values in the database are converted
#' to NA values in R.
#'
#' @return
#' A ffdf object containing the data. If there are 0 rows, a regular data frame is returned instead
#' (ffdf cannot have 0 rows)
#'
#' @export
lowLevelQuerySql.ffdf <- function(connection, query = "", datesAsString = FALSE) {
  UseMethod("lowLevelQuerySql.ffdf", connection)
}

#' @export
lowLevelQuerySql.ffdf.default <- function(connection, query = "", datesAsString = FALSE) {
  if (rJava::is.jnull(connection@jConnection))
    stop("Connection is closed")
  batchedQuery <- rJava::.jnew("org.ohdsi.databaseConnector.BatchedQuery",
                               connection@jConnection,
                               query)
  
  on.exit(rJava::.jcall(batchedQuery, "V", "clear"))
  
  columnTypes <- rJava::.jcall(batchedQuery, "[I", "getColumnTypes")
  if (length(columnTypes) == 0)
    stop("No columns found")
  columns <- vector("list", length(columnTypes))
  while (!rJava::.jcall(batchedQuery, "Z", "isDone")) {
    rJava::.jcall(batchedQuery, "V", "fetchBatch")
    if (is.null(columns[[1]]) && rJava::.jcall(batchedQuery, "Z", "isEmpty")) {
      # Empty result set: return data frame instead because ffdf can't have zero rows
      for (i in seq.int(length(columnTypes))) {
        if (columnTypes[i] == 1) {
          columns[[i]] <- vector("numeric", length = 0)
        } else {
          columns[[i]] <- vector("character", length = 0)
        }
      }
      names(columns) <- rJava::.jcall(batchedQuery, "[Ljava/lang/String;", "getColumnNames")
      attr(columns, "row.names") <- c(NA_integer_, length(columns[[1]]))
      class(columns) <- "data.frame"
      return(columns)
    } else {
      for (i in seq.int(length(columnTypes))) {
        if (columnTypes[i] == 1) {
          column <- rJava::.jcall(batchedQuery,
                                  "[D",
                                  "getNumeric",
                                  as.integer(i))
          # rJava doesn't appear to be able to return NAs, so converting NaNs to NAs:
          column[is.nan(column)] <- NA
          columns[[i]] <- ffbase::ffappend(columns[[i]], column)
        } else {
          columns[[i]] <- ffbase::ffappend(columns[[i]], factor(rJava::.jcall(batchedQuery,
                                                                              "[Ljava/lang/String;",
                                                                              "getString",
                                                                              i)))
        }
      }
    }
  }
  if (!datesAsString) {
    for (i in seq.int(length(columnTypes))) {
      if (columnTypes[i] == 3) {
        columns[[i]] <- ffbase::as.Date.ff_vector(columns[[i]])
      } else  if (columnTypes[i] == 4) {
        columns[[i]] <- as.POSIXct.ff_vector(columns[[i]])
      }
    }
  }
  ffdf <- do.call(ff::ffdf, columns)
  names(ffdf) <- rJava::.jcall(batchedQuery, "[Ljava/lang/String;", "getColumnNames")
  return(ffdf)
}

#' @export
lowLevelQuerySql.ffdf.DatabaseConnectorDbiConnection <- function(connection, query = "", datesAsString = FALSE) {
  results <- lowLevelQuerySql(connection, query)
  for (i in 1:ncol(results)) {
    if (class(results[, i]) == "character") {
      results[, i] <- as.factor(results[, i])
    }
  }
  if (nrow(results) == 0) {
    return(results)
  } else {
    return(ff::as.ffdf(results))
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
lowLevelQuerySql <- function(connection, query = "", datesAsString = FALSE) {
  # Not using UseMethod pattern to avoid note about lowLevelQuerySql.ffdf being a method:
  if (inherits(connection, "DatabaseConnectorDbiConnection")) {
    results <- DBI::dbGetQuery(connection@dbiConnection, query)
    # For compatibility with JDBC:
    for (i in 1:ncol(results)) {
      if (class(results[, i]) == "integer64") {
        results[, i] <- as.numeric(results[, i])
      }
    }
    return(results)
  }
  
  if (rJava::is.jnull(connection@jConnection))
    stop("Connection is closed")
  batchedQuery <- rJava::.jnew("org.ohdsi.databaseConnector.BatchedQuery",
                               connection@jConnection,
                               query)
  
  on.exit(rJava::.jcall(batchedQuery, "V", "clear"))
  
  columnTypes <- rJava::.jcall(batchedQuery, "[I", "getColumnTypes")
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

#' @export
lowLevelExecuteSql.default <- function(connection, sql) {
  statement <- rJava::.jcall(connection@jConnection, "Ljava/sql/Statement;", "createStatement")
  on.exit(rJava::.jcall(statement, "V", "close"))
  hasResultSet <- rJava::.jcall(statement, "Z", "execute", as.character(sql), check = FALSE)
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
        writeLines("JDBC driver supports batch updates")
        return(TRUE);
      } else {
        writeLines("JDBC driver does not support batch updates")
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
#'                            'errorReport.txt' in the current working directory.
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
                       errorReportFile = file.path(getwd(), "errorReport.txt"),
                       runAsBatch = FALSE) {
  if (inherits(connection, "DatabaseConnectorJdbcConnection") && rJava::is.jnull(connection@jConnection))
    stop("Connection is closed")
  
  if (connection@dbms == "spark") {
    sql <- SqlRender::sparkHandleInsert(connection = connection, sql = sql)
  }
  
  batched <- runAsBatch && supportsBatchUpdates(connection)
  if (profile || batched) {
    progressBar <- FALSE
  }
  sqlStatements <- SqlRender::splitSql(sql)
  if (progressBar) {
    pb <- txtProgressBar(style = 3)
  }
  start <- Sys.time()
  if (batched) {
    statement <- rJava::.jcall(connection@jConnection, "Ljava/sql/Statement;", "createStatement")
    on.exit(rJava::.jcall(statement, "V", "close"))
  }
  if (inherits(connection, "DatabaseConnectorJdbcConnection") && 
      connection@dbms == "redshift" &&
      rJava::.jcall(connection@jConnection, "Z", "getAutoCommit")) {
    # Turn off autocommit for RedShift to avoid this issue:
    # https://github.com/OHDSI/DatabaseConnector/issues/90
    rJava::.jcall(connection@jConnection, "V", "setAutoCommit", FALSE)
    on.exit(rJava::.jcall(connection@jConnection, "V", "setAutoCommit", TRUE), add = TRUE)
  }
  for (i in 1:length(sqlStatements)) {
    sqlStatement <- sqlStatements[i]
    if (profile) {
      fileConn <- file(paste("statement_", i, ".sql", sep = ""))
      writeChar(sqlStatement, fileConn, eos = NULL)
      close(fileConn)
    }
    if (batched) {
      rJava::.jcall(statement, "V", "addBatch", as.character(sqlStatement), check = FALSE)
    } else {
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
    }
    if (progressBar) {
      setTxtProgressBar(pb, i/length(sqlStatements))
    }
  }
  if (batched) {
    tryCatch({
      rowsAffected <- rJava::.jcall(statement, "[I", "executeBatch")
      invisible(rowsAffected)
    }, error = function(err) {
      .createErrorReport(connection@dbms, err$message, sql, errorReportFile)
    })
  }
  if (inherits(connection, "DatabaseConnectorJdbcConnection") && !rJava::.jcall(connection@jConnection, "Z", "getAutoCommit")) {
    rJava::.jcall(connection@jConnection, "V", "commit")
  }
  if (progressBar) {
    close(pb)
  }
  if (reportOverallTime) {
    delta <- Sys.time() - start
    writeLines(paste("Executing SQL took", signif(delta, 3), attr(delta, "units")))
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
        if (ff::is.ffdf(result)) {
          result[[colname]] <- as.POSIXct.ff_vector(result[[colname]], origin = "1970-01-01", tz = "GMT")
        } else {
          result[[colname]] <- as.POSIXct(result[[colname]], origin = "1970-01-01", tz = "GMT")
        }
      }
      
    }
  } 
  return(result)
}

#' Retrieve data to a data.frame
#'
#' @description
#' This function sends SQL to the server, and returns the results.
#'
#' @param connection           The connection to the database server.
#' @param sql                  The SQL to be send.
#' @param errorReportFile      The file where an error report will be written if an error occurs. Defaults to
#'                             'errorReport.txt' in the current working directory.
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
querySql <- function(connection, sql, errorReportFile = file.path(getwd(), "errorReport.txt"), snakeCaseToCamelCase = FALSE) {
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

#' Retrieves data to an ffdf object
#'
#' @description
#' This function sends SQL to the server, and returns the results in an ffdf object.
#'
#' @param connection           The connection to the database server.
#' @param sql                  The SQL to be send.
#' @param errorReportFile      The file where an error report will be written if an error occurs. Defaults to
#'                             'errorReport.txt' in the current working directory.
#' @param snakeCaseToCamelCase If true, field names are assumed to use snake_case, and are converted to camelCase.
#'
#' @details
#' Retrieves data from the database server and stores it in an ffdf object. This allows very large
#' data sets to be retrieved without running out of memory. If an error occurs during SQL execution,
#' this error is written to a file to facilitate debugging. Null values in the database are converted
#' to NA values in R.
#'
#' @return
#' A ffdf object containing the data. If there are 0 rows, a regular data frame is returned instead
#' (ffdf cannot have 0 rows).
#'
#' @examples
#' \dontrun{
#' library(ffbase)
#' connectionDetails <- createConnectionDetails(dbms = "postgresql",
#'                                              server = "localhost",
#'                                              user = "root",
#'                                              password = "blah",
#'                                              schema = "cdm_v4")
#' conn <- connect(connectionDetails)
#' count <- querySql.ffdf(conn, "SELECT COUNT(*) FROM person")
#' disconnect(conn)
#' }
#' @export
querySql.ffdf <- function(connection, sql, errorReportFile = file.path(getwd(), "errorReport.txt"), snakeCaseToCamelCase = FALSE) {
  if (inherits(connection, "DatabaseConnectorJdbcConnection") && rJava::is.jnull(connection@jConnection))
    stop("Connection is closed")
  # Calling splitSql, because this will also strip trailing semicolons (which cause Oracle to crash).
  sqlStatements <- SqlRender::splitSql(sql)
  if (length(sqlStatements) > 1)
    stop(paste("A query that returns a result can only consist of one SQL statement, but",
               length(sqlStatements),
               "statements were found"))
  tryCatch({
    result <- lowLevelQuerySql.ffdf(connection, sqlStatements[1])
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
#'                            'errorReport.txt' in the current working directory.
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
                                      errorReportFile = file.path(getwd(), "errorReport.txt"),
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
#'                             'errorReport.txt' in the current working directory.
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
                                    errorReportFile = file.path(getwd(), "errorReport.txt"), 
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


#' Render, translate, and query to ffdf
#'
#' @description
#' This function renders, and translates SQL, sends it to the server, and returns the results as an ffdf object
#'
#' @param connection           The connection to the database server.
#' @param sql                  The SQL to be send.
#' @param errorReportFile      The file where an error report will be written if an error occurs. Defaults to
#'                             'errorReport.txt' in the current working directory.
#' @param snakeCaseToCamelCase If true, field names are assumed to use snake_case, and are converted to camelCase.  
#' @param oracleTempSchema     A schema that can be used to create temp tables in when using Oracle or Impala.
#' @param ...                  Parameters that will be used to render the SQL.
#'
#' @details
#' This function calls the \code{render} and \code{translate} functions in the SqlRender package before 
#' calling \code{\link{querySql.ffdf}}.
#'
#' @return
#' An ffdf object
#'
#' @examples
#' \dontrun{
#' connectionDetails <- createConnectionDetails(dbms = "postgresql",
#'                                              server = "localhost",
#'                                              user = "root",
#'                                              password = "blah",
#'                                              schema = "cdm_v4")
#' conn <- connect(connectionDetails)
#' persons <- renderTranslatequerySql.ffdf(conn, 
#'                                         sql = "SELECT * FROM @@schema.person",
#'                                         schema = "cdm_synpuf")
#' disconnect(conn)
#' }
#' @export
renderTranslateQuerySql.ffdf <- function(connection, 
                                         sql, 
                                         errorReportFile = file.path(getwd(), "errorReport.txt"), 
                                         snakeCaseToCamelCase = FALSE,
                                         oracleTempSchema = NULL,
                                         ...) {
  sql <- SqlRender::render(sql, ...)
  sql <- SqlRender::translate(sql, targetDialect = connection@dbms, oracleTempSchema = oracleTempSchema)
  return(querySql.ffdf(connection = connection,
                       sql = sql,
                       errorReportFile = errorReportFile,
                       snakeCaseToCamelCase = snakeCaseToCamelCase))
}
