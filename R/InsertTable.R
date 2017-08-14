# @file InsertTable.R
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

mergeTempTables <- function(connection, tableName, varNames, sourceNames, location, distribution) {
  unionString <- paste("\nUNION ALL\nSELECT ", varNames, " FROM ", sep = "")
  valueString <- paste(sourceNames, collapse = unionString)
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

  # Drop source tables:
  for (sourceName in sourceNames) {
    sql <- paste("DROP TABLE", sourceName)
    executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  }
}

ctasHack <- function(connection, qname, tempTable, varNames, fts, data) {
  batchSize <- 1000
  mergeSize <- 300
  if (any(tolower(names(data)) == "subject_id")) {
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
    if (length(tempNames) == mergeSize) {
      mergedName <- paste("#", paste(sample(letters, 24, replace = TRUE), collapse = ""), sep = "")
      mergeTempTables(connection, mergedName, varNames, tempNames, location, distribution)
      tempNames <- c(mergedName)
    }
    # First line gets type information
    valueString <- paste(paste("CAST(",
                               sapply(data[start, , drop = FALSE], esc),
                               " AS ",
                               fts,
                               ")",
                               sep = ""), collapse = ",")
    end <- min(start + batchSize - 1, nrow(data))
    if (end == start + 1) {
      valueString <- paste(c(valueString, paste(sapply(data[start + 1, , drop = FALSE], esc),
                                                collapse = ",")), collapse = "\nUNION ALL\nSELECT ")
    } else if (end > start + 1) {
      valueString <- paste(c(valueString, apply(sapply(data[(start + 1):end, , drop = FALSE], esc),
                                                MARGIN = 1,
                                                FUN = paste,
                                                collapse = ",")), collapse = "\nUNION ALL\nSELECT ")
    }
    tempName <- paste("#", paste(sample(letters, 24, replace = TRUE), collapse = ""), sep = "")
    tempNames <- c(tempNames, tempName)
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
  mergeTempTables(connection, qname, varNames, tempNames, location, distribution)
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
#' disconnect(conn)
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
  fdef <- paste(.sql.qescape(names(data), TRUE, connection$identifierQuote), fts, collapse = ",")
  qname <- .sql.qescape(tableName, TRUE, connection$identifierQuote)
  esc <- function(str) {
    paste("'", gsub("'", "''", str), "'", sep = "")
  }
  varNames <- paste(.sql.qescape(names(data), TRUE, connection$identifierQuote), collapse = ",")

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

    autoCommit <- rJava::.jcall(connection$jConnection, "Z", "getAutoCommit")
    if (autoCommit) {
      rJava::.jcall(connection$jConnection, "V", "setAutoCommit", FALSE)
      on.exit(rJava::.jcall(connection$jConnection, "V", "setAutoCommit", TRUE))
    }

    insertRow <- function(row, statement) {
      for (i in 1:length(row)) {
        if (is.na(row[i])) {
          rJava::.jcall(statement, "V", "setString", i, rJava::.jnull(class = "java/lang/String"))
        } else {
          rJava::.jcall(statement, "V", "setString", i, as.character(row[i]))
        }
      }
      rJava::.jcall(statement, "V", "addBatch")
    }
    insertRowPostgreSql <- function(row, statement) {
      other <- rJava::.jfield("java/sql/Types", "I", "OTHER")
      for (i in 1:length(row)) {
        if (is.na(row[i])) {
          rJava::.jcall(statement, "V", "setObject", i, rJava::.jnull(), other)
        } else {
          value <- rJava::.jnew("java/lang/String", as.character(row[i]))
          rJava::.jcall(statement,
                        "V",
                        "setObject",
                        i,
                        rJava::.jcast(value, "java/lang/Object"),
                        other)
        }
      }
      rJava::.jcall(statement, "V", "addBatch")
    }
    insertRowOracle <- function(row, statement, isDate) {
      for (i in 1:length(row)) {
        if (is.na(row[i])) {
          rJava::.jcall(statement, "V", "setString", i, rJava::.jnull(class = "java/lang/String"))
        } else if (isDate[i]) {
          date <- rJava::.jcall("java/sql/Date", "Ljava/sql/Date;", "valueOf", as.character(row[i]))
          rJava::.jcall(statement, "V", "setDate", i, date)
        } else rJava::.jcall(statement, "V", "setString", i, as.character(row[i]))
      }
      rJava::.jcall(statement, "V", "addBatch")
    }
    insertRowImpala <- function(row, statement) {
      for (i in 1:length(row)) {
        if (is.na(row[i])) {
          rJava::.jcall(statement, "V", "setString", i, rJava::.jnull(class = "java/lang/String"))
        } else if (is.integer(row[i])) {
          rJava::.jcall(statement, "V", "setInt", i, as.integer(row[i]))
        } else {
          rJava::.jcall(statement, "V", "setString", i, as.character(row[i]))
        }
      }
      rJava::.jcall(statement, "V", "addBatch")
    }

    for (start in seq(1, nrow(data), by = batchSize)) {
      end <- min(start + batchSize - 1, nrow(data))
      statement <- rJava::.jcall(connection$jConnection,
                                 "Ljava/sql/PreparedStatement;",
                                 "prepareStatement",
                                 insertSql,
                                 check = FALSE)
      if (attr(connection, "dbms") == "postgresql") {
        apply(data[start:end,
              ,
              drop = FALSE],
              statement = statement,
              MARGIN = 1,
              FUN = insertRowPostgreSql)
      } else if (attr(connection, "dbms") == "oracle" | attr(connection, "dbms") == "redshift") {
        apply(data[start:end,
              ,
              drop = FALSE],
              statement = statement,
              isDate = isDate,
              MARGIN = 1,
              FUN = insertRowOracle)
      } else if (attr(connection, "dbms") == "impala") {
        apply(data[start:end,
              ,
              drop = FALSE],
              statement = statement,
              MARGIN = 1,
              FUN = insertRowImpala)
      } else {
        apply(data[start:end, , drop = FALSE], statement = statement, MARGIN = 1, FUN = insertRow)
      }
      rJava::.jcall(statement, "[I", "executeBatch")
    }
  }
}
