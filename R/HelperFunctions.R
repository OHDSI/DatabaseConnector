# @file HelperFunctions.R
#
# Copyright 2016 Observational Health Data Sciences and Informatics
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
#'                        a batchSize could lead to out-of-memory errors.
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
                                  batchSize = 5e+05,
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
  
  # Fetch data in batches:
  data <- NULL
  n <- batchSize
  while (n == batchSize) {
    batch <- RJDBC::fetch(resultSet, batchSize)
    if (!datesAsString) {
      cols <- rJava::.jcall(resultSet@md, "I", "getColumnCount")
      for (i in 1:cols) {
        type <- rJava::.jcall(resultSet@md, "I", "getColumnType", i)
        if (type == 91)
          batch[, i] <- as.Date(batch[, i])
      }
    }
    
    n <- nrow(batch)
    if (is.null(data)) {
      charCols <- sapply(batch, class)
      charCols <- names(charCols[charCols == "character"])
      
      for (charCol in charCols) batch[[charCol]] <- factor(batch[[charCol]])
      
      if (n == 0) {
        data <- batch  #ffdf cannot contain 0 rows, so return data.frame instead
        warning("Data has zero rows, returning an empty data frame")
      } else data <- ff::as.ffdf(batch)
    } else if (n != 0) {
      for (charCol in charCols) batch[[charCol]] <- factor(batch[[charCol]])
      
      data <- ffbase::ffdfappend(data, batch)
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
    writeLines(paste("Analysis took", signif(delta, 3), attr(delta, "units")))
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
  tryCatch({
    rJava::.jcall("java/lang/System", , "gc")  #Calling garbage collection prevents crashes
    
    result <- lowLevelQuerySql(connection, sql)
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
#'                                              schema = "cdm_v4")  #'   conn <- connect(connectionDetails)
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

.sql.qescape <- function(s, identifier = FALSE, quote = "\"") {
  s <- as.character(s)
  if (identifier) {
    vid <- grep("^[A-Za-z]+([A-Za-z0-9_]*)$", s)
    if (length(s[-vid])) {
      if (is.na(quote))
        stop("The JDBC connection doesn't support quoted identifiers, but table/column name contains characters that must be quoted (",
             paste(s[-vid], collapse = ","),
             ")")
      s[-vid] <- .sql.qescape(s[-vid], FALSE, quote)
    }
    return(s)
  }
  if (is.na(quote))
    quote <- ""
  s <- gsub("\\\\", "\\\\\\\\", s)
  if (nchar(quote))
    s <- gsub(paste("\\", quote, sep = ""), paste("\\\\\\", quote, sep = ""), s, perl = TRUE)
  paste(quote, s, quote, sep = "")
}


recursiveMerge <- function(connection, tableName, varNames, tempNames, location, distribution) {
  maxSelects <- 300
  if (length(tempNames) > maxSelects) {
    batchSize <- ceiling(length(tempNames)/ceiling(length(tempNames)/maxSelects))
    newTempNames <- c()
    for (start in seq(1, length(tempNames), by = batchSize)) {
      end <- min(start + batchSize - 1, length(tempNames))
      tempName <- paste("#", paste(sample(letters, 24, replace = TRUE), collapse = ""), sep = "")
      recursiveMerge(connection,
                     tempName,
                     varNames,
                     tempNames[start:end],
                     "LOCATION = USER_DB, ",
                     distribution)
      newTempNames <- c(newTempNames, tempName)
    }
    tempNames <- newTempNames
  }
  unionString <- paste("\nUNION ALL\nSELECT ", varNames, " FROM ", sep = "")
  valueString <- paste(tempNames, collapse = unionString)
  sql <- paste("IF XACT_STATE() = 1 COMMIT; CREATE TABLE ",
               tableName,
               " (",
               varNames,
               " ) WITH (",
               location,
               "DISTRIBUTION=",
               distribution,
               ") AS SELECT ",
               varNames,
               " FROM ",
               valueString,
               sep = "")
  executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  
  # Drop temp tables:
  for (tempName in tempNames) {
    sql <- paste("DROP TABLE", tempName)
    executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  }
}

ctasHack <- function(connection, qname, tempTable, varNames, fts, data) {
  batchSize <- 1000
  if (any(tolower(names(data)) == "subject_Id")) {
    distribution <- "HASH(SUBJECT_ID)"
  } else if (any(tolower(names(data)) == "person_id")) {
    distribution <- "HASH(PERSON_ID)"
  } else {
    distribution <- "REPLICATE"
  }
  if (tempTable) {
    location <- "LOCATION = USER_DB, "
  } else {
    location <- ""
  }
  esc <- function(str) {
    result <- paste("'", gsub("'", "''", str), "'", sep = "")
    result[is.na(str)] <- "NULL"
    return(result)
  }
  
  # Insert data in batches in temp tables using CTAS:
  tempNames <- c()
  for (start in seq(1, nrow(data), by = batchSize)) {
    end <- min(start + batchSize - 1, nrow(data))
    tempName <- paste("#", paste(sample(letters, 24, replace = TRUE), collapse = ""), sep = "")
    tempNames <- c(tempNames, tempName)
    # First line gets type information
    valueString <- paste(paste("CAST(",
                               sapply(data[start, , drop = FALSE], esc),
                               " AS ",
                               fts,
                               ")",
                               sep = ""), collapse = ",")
    if (end > start) {
      valueString <- paste(c(valueString, apply(sapply(data[(start + 1):end, , drop = FALSE], esc),
                                                MARGIN = 1,
                                                FUN = paste,
                                                collapse = ",")), collapse = "\nUNION ALL\nSELECT ")
    }
    sql <- paste("IF XACT_STATE() = 1 COMMIT; CREATE TABLE ",
                 tempName,
                 " (",
                 varNames,
                 " ) WITH (LOCATION = USER_DB, DISTRIBUTION=",
                 distribution,
                 ") AS SELECT ",
                 valueString,
                 sep = "")
    executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  }
  
  recursiveMerge(connection, qname, varNames, tempNames, location, distribution)
}

#' Insert a table on the server
#'
#' @description
#' This function sends the data in a data frame or ffdf to a table on the server. Either a new table
#' is created, or the data is appended to an existing table.
#'
#' @param connection          The connection to the database server.
#' @param tableName           The name of the table where the data should be inserted.
#' @param data                The data frame or ffdf containing the data to be inserted.
#' @param dropTableIfExists   Drop the table if the table already exists before writing?
#' @param createTable         Create a new table? If false, will append to existing table.
#' @param tempTable           Should the table created as a temp table?
#' @param oracleTempSchema    Specifically for Oracle, a schema with write priviliges where temp tables
#'                            can be created.
#'
#' @details
#' This function sends the data in a data frame to a table on the server. Either a new table is
#' created, or the data is appended to an existing table.
#'
#' @examples
#' \dontrun{
#' connectionDetails <- createConnectionDetails(dbms = "mysql",
#'                                              server = "localhost",
#'                                              user = "root",
#'                                              password = "blah",
#'                                              schema = "cdm_v4")
#' conn <- connect(connectionDetails)
#' data <- data.frame(x = c(1, 2, 3), y = c("a", "b", "c"))
#' insertTable(conn, "my_table", data)
#' dbDisconnect(conn)
#' }
#' @export
insertTable <- function(connection,
                        tableName,
                        data,
                        dropTableIfExists = TRUE,
                        createTable = TRUE,
                        tempTable = FALSE,
                        oracleTempSchema = NULL) {
  if (dropTableIfExists)
    createTable <- TRUE
  if (tempTable & substr(tableName, 1, 1) != "#")
    tableName <- paste("#", tableName, sep = "")
  if (!tempTable & substr(tableName, 1, 1) == "#") {
    tempTable <- TRUE
    warning("Temp table name detected, setting tempTable parameter to TRUE")
  }
  if (is.vector(data) && !is.list(data))
    data <- data.frame(x = data)
  if (length(data) < 1)
    stop("data must have at least one column")
  if (is.null(names(data)))
    names(data) <- paste("V", 1:length(data), sep = "")
  if (length(data[[1]]) > 0) {
    if (!is.data.frame(data))
      data <- as.data.frame(data, row.names = 1:length(data[[1]]))
  } else {
    if (!is.data.frame(data))
      data <- as.data.frame(data)
  }
  
  def <- function(obj) {
    if (is.integer(obj))
      "INTEGER" else if (is.numeric(obj))
        "FLOAT" else if (class(obj) == "Date")
          "DATE" else "VARCHAR(255)"
  }
  fts <- sapply(data[1, ], def)
  isDate <- (fts == "DATE")
  fdef <- paste(.sql.qescape(names(data), TRUE, connection@identifier.quote), fts, collapse = ",")
  qname <- .sql.qescape(tableName, TRUE, connection@identifier.quote)
  esc <- function(str) {
    paste("'", gsub("'", "''", str), "'", sep = "")
  }
  varNames <- paste(.sql.qescape(names(data), TRUE, connection@identifier.quote), collapse = ",")
  
  if (dropTableIfExists) {
    if (tempTable) {
      sql <- "IF OBJECT_ID('tempdb..@tableName', 'U') IS NOT NULL DROP TABLE @tableName;"
    } else {
      sql <- "IF OBJECT_ID('@tableName', 'U') IS NOT NULL DROP TABLE @tableName;"
    }
    sql <- SqlRender::renderSql(sql, tableName = tableName)$sql
    sql <- SqlRender::translateSql(sql,
                                   targetDialect = attr(connection, "dbms"),
                                   oracleTempSchema = oracleTempSchema)$sql
    executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  }
  
  if (attr(connection, "dbms") == "pdw" && createTable) {
    ctasHack(connection, qname, tempTable, varNames, fts, data)
  } else {
    
    if (createTable) {
      sql <- paste("CREATE TABLE ", qname, " (", fdef, ");", sep = "")
      sql <- SqlRender::translateSql(sql,
                                     targetDialect = attr(connection, "dbms"),
                                     oracleTempSchema = oracleTempSchema)$sql
      executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
    }
    
    insertSql <- paste("INSERT INTO ",
                       qname,
                       " (",
                       varNames,
                       ") VALUES(",
                       paste(rep("?", length(fts)), collapse = ","),
                       ")",
                       sep = "")
    insertSql <- SqlRender::translateSql(insertSql,
                                         targetDialect = attr(connection, "dbms"),
                                         oracleTempSchema = oracleTempSchema)$sql
    
    batchSize <- 10000
    
    autoCommit <- rJava::.jcall(connection@jc, "Z", "getAutoCommit")
    if (autoCommit) {
      rJava::.jcall(connection@jc, "V", "setAutoCommit", FALSE)
      on.exit(rJava::.jcall(connection@jc, "V", "setAutoCommit", TRUE))
    }
    
    insertRow <- function(row, statement) {
      for (i in 1:length(row)) rJava::.jcall(statement, "V", "setString", i, as.character(row[i]))
      rJava::.jcall(statement, "V", "addBatch")
    }
    insertRowPostgreSql <- function(row, statement) {
      other <- rJava::.jfield("java/sql/Types", "I", "OTHER")
      for (i in 1:length(row)) {
        value <- rJava::.jnew("java/lang/String", as.character(row[i]))
        rJava::.jcall(statement,
                      "V",
                      "setObject",
                      i,
                      rJava::.jcast(value, "java/lang/Object"),
                      other)
      }
      rJava::.jcall(statement, "V", "addBatch")
    }
    insertRowOracle <- function(row, statement, isDate) {
      for (i in 1:length(row)) {
        if (isDate[i]) {
          date <- rJava::.jcall("java/sql/Date", "Ljava/sql/Date;", "valueOf", as.character(row[i]))
          rJava::.jcall(statement, "V", "setDate", i, date)
        } else rJava::.jcall(statement, "V", "setString", i, as.character(row[i]))
      }
      rJava::.jcall(statement, "V", "addBatch")
    }
    
    for (start in seq(1, nrow(data), by = batchSize)) {
      end <- min(start + batchSize - 1, nrow(data))
      statement <- rJava::.jcall(connection@jc,
                                 "Ljava/sql/PreparedStatement;",
                                 "prepareStatement",
                                 insertSql,
                                 check = FALSE)
      if (attr(connection, "dbms") == "postgresql" | attr(connection, "dbms") == "redshift")
        apply(data[start:end,
                   ,
                   drop = FALSE],
              statement = statement,
              MARGIN = 1,
              FUN = insertRowPostgreSql) else if (attr(connection, "dbms") == "oracle")
                apply(data[start:end,
                           ,
                           drop = FALSE],
                      statement = statement,
                      isDate = isDate,
                      MARGIN = 1,
                      FUN = insertRowOracle) else apply(data[start:end,
                                                             ,
                                                             drop = FALSE],
                                                        statement = statement,
                                                        MARGIN = 1,
                                                        FUN = insertRow)
      rJava::.jcall(statement, "[I", "executeBatch")
    }
  }
}
