# @file Sql.R
#
# Copyright 2017 Observational Health Data Sciences and Informatics
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

.createErrorReport <- function(dbms, message, sql) {
  report <- c("DBMS:\n", dbms, "\n\nError:\n", message, "\n\nSQL:\n", sql, "\n\n", .systemInfo())
  
  fileName <- paste(getwd(), "/errorReport.txt", sep = "")
  fileConn <- file(fileName)
  writeChar(report, fileConn, eos = NULL)
  close(fileConn)
  stop(paste("Error executing SQL:",
             message,
             paste("An error report has been created at ", fileName),
             sep = "\n"), call. = FALSE)
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
#' data sets to be retrieved without running out of memory.
#'
#' @return
#' A ffdf object containing the data. If there are 0 rows, a regular data frame is returned instead
#' (ffdf cannot have 0 rows)
#'
#' @export
lowLevelQuerySql.ffdf <- function(connection, query = "", datesAsString = FALSE) {
  if (rJava::is.jnull(connection$jConnection))
    stop("Connection is closed")
  batchedQuery <- rJava::.jnew("org.ohdsi.databaseConnector.BatchedQuery",
                               connection$jConnection,
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
          columns[[i]] <- ffbase::ffappend(columns[[i]], rJava::.jcall(batchedQuery,
                                                                       "[D",
                                                                       "getNumeric",
                                                                       as.integer(i)))
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
      }
    }
  }
  ffdf <- do.call(ff::ffdf, columns)
  names(ffdf) <- rJava::.jcall(batchedQuery, "[Ljava/lang/String;", "getColumnNames")
  return(ffdf)
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
#' Retrieves data from the database server and stores it in a data frame.
#'
#' @return
#' A data frame containing the data retrieved from the server
#'
#' @export
lowLevelQuerySql <- function(connection, query = "", datesAsString = FALSE) {
  if (rJava::is.jnull(connection$jConnection))
    stop("Connection is closed")
  batchedQuery <- rJava::.jnew("org.ohdsi.databaseConnector.BatchedQuery",
                               connection$jConnection,
                               query)
  
  on.exit(rJava::.jcall(batchedQuery, "V", "clear"))
  
  columnTypes <- rJava::.jcall(batchedQuery, "[I", "getColumnTypes")
  columns <- vector("list", length(columnTypes))
  while (!rJava::.jcall(batchedQuery, "Z", "isDone")) {
    rJava::.jcall(batchedQuery, "V", "fetchBatch")
    for (i in seq.int(length(columnTypes))) {
      if (columnTypes[i] == 1) {
        columns[[i]] <- c(columns[[i]],
                          rJava::.jcall(batchedQuery, "[D", "getNumeric", as.integer(i)))
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
  statement <- rJava::.jcall(connection$jConnection, "Ljava/sql/Statement;", "createStatement")
  on.exit(rJava::.jcall(statement, "V", "close"))
  rJava::.jcall(statement, "I", "executeUpdate", as.character(sql), check = FALSE)
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
#' connectionDetails <- createConnectionDetails(dbms = "mysql",
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
                       reportOverallTime = TRUE) {
  if (rJava::is.jnull(connection$jConnection))
    stop("Connection is closed")
  if (profile)
    progressBar <- FALSE
  sqlStatements <- SqlRender::splitSql(sql)
  if (progressBar)
    pb <- txtProgressBar(style = 3)
  start <- Sys.time()
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
      .createErrorReport(attr(connection, "dbms"), err$message, sqlStatement)
    })
    if (progressBar)
      setTxtProgressBar(pb, i/length(sqlStatements))
  }
  if (!rJava::.jcall(connection$jConnection, "Z", "getAutoCommit")) {
    rJava::.jcall(connection@jConnection, "V", "commit")
  }
  if (progressBar)
    close(pb)
  if (reportOverallTime) {
    delta <- Sys.time() - start
    writeLines(paste("Executing SQL took", signif(delta, 3), attr(delta, "units")))
  }
}

#' Retrieve data to a data.frame
#'
#' @description
#' This function sends SQL to the server, and returns the results.
#'
#' @param connection   The connection to the database server.
#' @param sql          The SQL to be send.
#'
#' @details
#' This function sends the SQL to the server and retrieves the results. If an error occurs during SQL
#' execution, this error is written to a file to facilitate debugging.
#'
#' @return
#' A data frame.
#'
#' @examples
#' \dontrun{
#' connectionDetails <- createConnectionDetails(dbms = "mysql",
#'                                              server = "localhost",
#'                                              user = "root",
#'                                              password = "blah",
#'                                              schema = "cdm_v4")
#' conn <- connect(connectionDetails)
#' count <- querySql(conn, "SELECT COUNT(*) FROM person")
#' disconnect(conn)
#' }
#' @export
querySql <- function(connection, sql) {
  if (rJava::is.jnull(connection$jConnection))
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
    if (attr(connection, "dbms") == "impala") {
      for (colname in colnames(result)) {
        if (grepl("DATE", colname)) {
          result[[colname]] <- as.Date(result[[colname]], "%Y-%m-%d")
        }
      }
    }
    return(result)
  }, error = function(err) {
    .createErrorReport(attr(connection, "dbms"), err$message, sql)
  })
}

#' Retrieves data to an ffdf object
#'
#' @description
#' This function sends SQL to the server, and returns the results in an ffdf object.
#'
#' @param connection   The connection to the database server.
#' @param sql          The SQL to be send.
#'
#' @details
#' Retrieves data from the database server and stores it in an ffdf object. This allows very large
#' data sets to be retrieved without running out of memory. If an error occurs during SQL execution,
#' this error is written to a file to facilitate debugging.
#'
#' @return
#' A ffdf object containing the data. If there are 0 rows, a regular data frame is returned instead
#' (ffdf cannot have 0 rows)
#'
#' @examples
#' \dontrun{
#' library(ffbase)
#' connectionDetails <- createConnectionDetails(dbms = "mysql",
#'                                              server = "localhost",
#'                                              user = "root",
#'                                              password = "blah",
#'                                              schema = "cdm_v4")
#' conn <- connect(connectionDetails)
#' count <- querySql.ffdf(conn, "SELECT COUNT(*) FROM person")
#' disconnect(conn)
#' }
#' @export
querySql.ffdf <- function(connection, sql) {
  if (rJava::is.jnull(connection$jConnection))
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
    if (attr(connection, "dbms") == "impala") {
      for (colname in colnames(result)) {
        if (grepl("DATE", colname)) {
          result[[colname]] <- as.Date(result[[colname]], "%Y-%m-%d")
        }
      }
    }
    return(result)
  }, error = function(err) {
    .createErrorReport(attr(connection, "dbms"), err$message, sql)
  })
}
