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

.getBatch <- function(resultSet, batchSize, datesAsString) {
  batch <- RJDBC::fetch(resultSet, batchSize)
  if (!datesAsString) {
    cols <- rJava::.jcall(resultSet@md, "I", "getColumnCount")
    for (i in 1:cols) {
      type <- rJava::.jcall(resultSet@md, "I", "getColumnType", i)
      if (type == 91)
        batch[, i] <- as.Date(batch[, i])
    }
  } 
  return(batch)
}

#' Low level function for retrieving data to an ffdf object
#'
#' @description
#' This is the equivalent of the \code{\link{querySql.ffdf}} function, except no error report is
#' written when an error occurs.
#'
#' @param connection      The connection to the database server.
#' @param query           The SQL statement to retrieve the data
#' @param batchSize       The number of rows that will be retrieved at a time from the server. A larger
#'                        batchSize means less calls to the server so better performance, but too large
#'                        a batchSize could lead to out-of-memory errors. The default is "auto", meaning
#'                        heuristics will determine the appropriate batch size.
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
lowLevelQuerySql.ffdf <- function(connection,
                                  query = "",
                                  batchSize = "auto",
                                  datesAsString = FALSE) {
  # Create resultset:
  rJava::.jcall("java/lang/System", , "gc")

  # Have to set autocommit to FALSE for PostgreSQL, or else it will ignore setFetchSize (Note: reason
  # for this is that PostgreSQL doesn't want the data set you're getting to change during fetch)
  autoCommit <- rJava::.jcall(connection@jc, "Z", "getAutoCommit")
  if (autoCommit) {
    rJava::.jcall(connection@jc, "V", "setAutoCommit", FALSE)
    on.exit(rJava::.jcall(connection@jc, "V", "setAutoCommit", TRUE))
  }

  type_forward_only <- rJava::.jfield("java/sql/ResultSet", "I", "TYPE_FORWARD_ONLY")
  concur_read_only <- rJava::.jfield("java/sql/ResultSet", "I", "CONCUR_READ_ONLY")
  s <- rJava::.jcall(connection@jc,
                     "Ljava/sql/Statement;",
                     "createStatement",
                     type_forward_only,
                     concur_read_only)

  # Have to call setFetchSize on Statement object for PostgreSQL (RJDBC only calls it on ResultSet)
  rJava::.jcall(s, "V", method = "setFetchSize", as.integer(2048))

  r <- rJava::.jcall(s, "Ljava/sql/ResultSet;", "executeQuery", as.character(query)[1])
  md <- rJava::.jcall(r, "Ljava/sql/ResultSetMetaData;", "getMetaData", check = FALSE)
  resultSet <- new("JDBCResult", jr = r, md = md, stat = s, pull = rJava::.jnull())

  on.exit(RJDBC::dbClearResult(resultSet), add = TRUE)
  
  # Fetch first 100 rows to estimate required memory per batch:
  batch <- .getBatch(resultSet, 100, datesAsString)
  if (batchSize == "auto") {
    batchSize <- floor(5e8 / as.numeric(object.size(batch)))
  }
  n <- nrow(batch)
  
  # Convert to ffdf object:
  charCols <- sapply(batch, class)
  charCols <- names(charCols[charCols == "character"])
  for (charCol in charCols) {
    batch[[charCol]] <- factor(batch[[charCol]])
  }
  if (n == 0) {
    data <- batch  #ffdf cannot contain 0 rows, so return data.frame instead
    warning("Data has zero rows, returning an empty data frame")
  } else {
    data <- ff::as.ffdf(batch)
  }
  
  if (n == 100) {
    # Fetch remaining data in batches:
    n <- batchSize
    while (n == batchSize) {
      batch <- .getBatch(resultSet, batchSize, datesAsString)
      
      n <- nrow(batch)
      if (n != 0) {
        for (charCol in charCols) batch[[charCol]] <- factor(batch[[charCol]])
        data <- ffbase::ffdfappend(data, batch)
      }
    }
  }
  return(data)
}

#' Low level function for retrieving data to an ffdf object
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
  # Create resultset:
  rJava::.jcall("java/lang/System", , "gc")

  # Have to set autocommit to FALSE for PostgreSQL, or else it will ignore setFetchSize (Note: reason
  # for this is that PostgreSQL doesn't want the data set you're getting to change during fetch)
  autoCommit <- rJava::.jcall(connection@jc, "Z", "getAutoCommit")
  if (autoCommit) {
    rJava::.jcall(connection@jc, "V", "setAutoCommit", FALSE)
    on.exit(rJava::.jcall(connection@jc, "V", "setAutoCommit", TRUE))
  }

  type_forward_only <- rJava::.jfield("java/sql/ResultSet", "I", "TYPE_FORWARD_ONLY")
  concur_read_only <- rJava::.jfield("java/sql/ResultSet", "I", "CONCUR_READ_ONLY")
  s <- rJava::.jcall(connection@jc,
                     "Ljava/sql/Statement;",
                     "createStatement",
                     type_forward_only,
                     concur_read_only)

  # Have to call setFetchSize on Statement object for PostgreSQL (RJDBC only calls it on ResultSet)
  rJava::.jcall(s, "V", method = "setFetchSize", as.integer(2048))

  r <- rJava::.jcall(s, "Ljava/sql/ResultSet;", "executeQuery", as.character(query)[1])
  md <- rJava::.jcall(r, "Ljava/sql/ResultSetMetaData;", "getMetaData", check = FALSE)
  resultSet <- new("JDBCResult", jr = r, md = md, stat = s, pull = rJava::.jnull())

  on.exit(RJDBC::dbClearResult(resultSet), add = TRUE)

  data <- RJDBC::fetch(resultSet, -1)

  if (!datesAsString) {
    cols <- rJava::.jcall(resultSet@md, "I", "getColumnCount")
    for (i in 1:cols) {
      type <- rJava::.jcall(resultSet@md, "I", "getColumnType", i)
      if (type == 91)
        data[, i] <- as.Date(data[, i])
    }
  }

  return(data)
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
#' dbDisconnect(conn)
#' }
#' @export
executeSql <- function(connection,
                       sql,
                       profile = FALSE,
                       progressBar = TRUE,
                       reportOverallTime = TRUE) {
  if (profile)
    progressBar <- FALSE
  sqlStatements <- SqlRender::splitSql(sql)
  if (progressBar)
    pb <- txtProgressBar(style = 3)
  start <- Sys.time()
  for (i in 1:length(sqlStatements)) {
    sqlStatement <- sqlStatements[i]
    if (profile) {
      sink(paste("statement_", i, ".sql", sep = ""))
      cat(sqlStatement)
      sink()
    }
    tryCatch({
      startQuery <- Sys.time()
      RJDBC::dbSendUpdate(connection, sqlStatement)
      if (profile) {
        delta <- Sys.time() - startQuery
        writeLines(paste("Statement ", i, "took", delta, attr(delta, "units")))
      }
    }, error = function(err) {
      writeLines(paste("Error executing SQL:", err))

      # Write error report:
      filename <- paste(getwd(), "/errorReport.txt", sep = "")
      sink(filename)
      cat("DBMS:\n")
      cat(attr(connection, "dbms"))
      cat("\n\n")
      cat("Error:\n")
      cat(err$message)
      cat("\n\n")
      cat("SQL:\n")
      cat(sqlStatement)
      cat("\n\n")
      cat(.systemInfo())
      sink()

      writeLines(paste("An error report has been created at ", filename))
      break
    })
    if (progressBar)
      setTxtProgressBar(pb, i/length(sqlStatements))
  }
  if (progressBar)
    close(pb)
  if (reportOverallTime) {
    delta <- Sys.time() - start
    writeLines(paste("Executing SQL took", signif(delta, 3), attr(delta, "units")))
  }
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
  for (pkg in si$otherPkgs) lines <- c(lines,
                                       paste("- ", pkg$Package, " (", pkg$Version, ")", sep = ""))
  return(paste(lines, collapse = "\n"))
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
#' dbDisconnect(conn)
#' }
#' @export
querySql <- function(connection, sql) {
  # Calling splitSql, because this will also strip trailing semicolons (which cause Oracle to crash).
  sqlStatements <- SqlRender::splitSql(sql)
  if (length(sqlStatements) > 1)
    stop(paste("A query that returns a result can only consist of one SQL statement, but", length(sqlStatements), "statements were found"))
  tryCatch({
    rJava::.jcall("java/lang/System", , "gc")  #Calling garbage collection prevents crashes
    result <- lowLevelQuerySql(connection, sqlStatements[1])
    colnames(result) <- toupper(colnames(result))
    return(result)
  }, error = function(err) {
    writeLines(paste("Error executing SQL:", err))

    # Write error report:
    filename <- paste(getwd(), "/errorReport.txt", sep = "")
    sink(filename)
    cat("DBMS:\n")
    cat(attr(connection, "dbms"))
    cat("\n\n")
    cat("Error:\n")
    cat(err$message)
    cat("\n\n")
    cat("SQL:\n")
    cat(sql)
    cat("\n\n")
    cat(.systemInfo())
    sink()

    writeLines(paste("An error report has been created at ", filename))
    break
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
#' dbDisconnect(conn)
#' }
#' @export
querySql.ffdf <- function(connection, sql) {
  tryCatch({
    result <- lowLevelQuerySql.ffdf(connection, sql)
    colnames(result) <- toupper(colnames(result))
    return(result)
  }, error = function(err) {
    writeLines(paste("Error executing SQL:", err))

    # Write error report:
    filename <- paste(getwd(), "/errorReport.txt", sep = "")
    sink(filename)
    cat("DBMS:\n")
    cat(attr(connection, "dbms"))
    cat("\n\n")
    cat("Error:\n")
    cat(err$message)
    cat("\n\n")
    cat("SQL:\n")
    cat(sql)
    cat("\n\n")
    cat(.systemInfo())
    sink()

    writeLines(paste("An error report has been created at ", filename))
    break
  })
}
