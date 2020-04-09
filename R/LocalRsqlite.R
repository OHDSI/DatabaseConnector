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

#' Low level function for retrieving data to a local SQLite database
#'
#' @description
#' This is the equivalent of the \code{\link{querySql.sqlite}} function, except no error report is
#' written when an error occurs.
#'
#' @param connection      The connection to the database server.
#' @param query           The SQL statement to retrieve the data
#' @param datesAsString   Should dates be imported as character vectors, our should they be converted
#'                        to R's date format?
#' @param sqliteConnection An open connection to a SQLite database, for example as created using \code{RSQLite::dbConnect()}.
#' @param sqliteTableName  The name of the table in the local SQLite database where the results of the query will be stored.
#'
#' @details
#' Retrieves data from the database server and stores it in a local SQLite database This allows very large
#' data sets to be retrieved without running out of memory. Null values in the database are converted
#' to NA values in R. If a table with the same name already exists in the local SQLite database it is replaced.
#'
#' @return
#' Invisibly returns the sqliteConnection. The SQLite database will have a table added with the query results.
#'
#' @export
lowLevelQuerySql.sqlite <- function(connection, query, sqliteConnection, sqliteTableName, datesAsString = FALSE) {
  UseMethod("lowLevelQuerySql.sqlite", connection)
}

#' @export
lowLevelQuerySql.sqlite.default <- function(connection, query, sqliteConnection, sqliteTableName, datesAsString = FALSE) {
  if (rJava::is.jnull(connection@jConnection))
    stop("Connection is closed")
  batchedQuery <- rJava::.jnew("org.ohdsi.databaseConnector.BatchedQuery",
                               connection@jConnection,
                               query)
  
  on.exit(rJava::.jcall(batchedQuery, "V", "clear"))
  
  columnTypes <- rJava::.jcall(batchedQuery, "[I", "getColumnTypes")
  if (length(columnTypes) == 0)
    stop("No columns found")
  first <- TRUE
  while (!rJava::.jcall(batchedQuery, "Z", "isDone")) {
    rJava::.jcall(batchedQuery, "V", "fetchBatch")
    batch <- vector("list", length(columnTypes))
    for (i in seq.int(length(columnTypes))) {
      if (columnTypes[i] == 1) {
        column <- rJava::.jcall(batchedQuery,
                                "[D",
                                "getNumeric",
                                as.integer(i))
        # rJava doesn't appear to be able to return NAs, so converting NaNs to NAs:
        column[is.nan(column)] <- NA
        batch[[i]] <- column
      } else  {
        batch[[i]] <- rJava::.jcall(batchedQuery,
                                    "[Ljava/lang/String;",
                                    "getString",
                                    i)
        if (!datesAsString) {
          if (columnTypes[i] == 3) {
            batch[[i]] <- as.Date(batch[[i]])
          } else  if (columnTypes[i] == 4) {
            batch[[i]] <- as.POSIXct(batch[[i]])
          }
        }
      }
      
    }
    names(batch) <- rJava::.jcall(batchedQuery, "[Ljava/lang/String;", "getColumnNames")
    
    # More efficient than as.data.frame, as it avoids converting row.names to character:
    batch <- structure(batch, class = "data.frame", row.names = seq_len(length(batch[[1]])))
    RSQLite::dbWriteTable(conn = sqliteConnection,
                          name = sqliteTableName,
                          value = batch,
                          overwrite = first,
                          append = !first)
    first <- FALSE
  }
  invisible(sqliteConnection)
}

#' @export
lowLevelQuerySql.sqlite.DatabaseConnectorDbiConnection <- function(connection, query, sqliteConnection, sqliteTableName, datesAsString = FALSE) {
  results <- lowLevelQuerySql(connection, query)
  
  RSQLite::dbWriteTable(conn = sqliteConnection,
                        name = sqliteTableName,
                        value = results,
                        overwrite = TRUE,
                        append = FALSE)
  invisible(sqliteConnection)
}

#' Retrieves data to a local SQLite database
#'
#' @description
#' This function sends SQL to the server, and returns the results in a local SQLite database.
#'
#' @param connection           The connection to the database server.
#' @param sql                  The SQL to be sent.
#' @param sqliteConnection An open connection to a SQLite database, for example as created using \code{RSQLite::dbConnect()}.
#' @param sqliteTableName  The name of the table in the local SQLite database where the results of the query will be stored.
#' @param errorReportFile      The file where an error report will be written if an error occurs. Defaults to
#'                             'errorReport.txt' in the current working directory.
#' @param snakeCaseToCamelCase If true, field names are assumed to use snake_case, and are converted to camelCase.
#'
#' @details
#' Retrieves data from the database server and stores it in a local SQLite database. This allows very large
#' data sets to be retrieved without running out of memory. If an error occurs during SQL execution,
#' this error is written to a file to facilitate debugging. Null values in the database are converted
#' to NA values in R.If a table with the same name already exists in the local SQLite database it is replaced.
#'
#' @return
#' @return
#' Invisibly returns the sqliteConnection. The SQLite database will have a table added with the query results.
#' 
#' @examples
#' \dontrun{
#' sqliteConnection <- RSQLite::dbConnect(RSQLite::SQLite(), "myLocalDb.sqlite")
#' connectionDetails <- createConnectionDetails(dbms = "postgresql",
#'                                              server = "localhost",
#'                                              user = "root",
#'                                              password = "blah",
#'                                              schema = "cdm_v4")
#' conn <- connect(connectionDetails)
#' querySql.sqlite(connection = conn, 
#'                 sql = "SELECT * FROM person;", 
#'                 sqliteConnection = sqliteConnection,
#'                 sqliteTableName = "foo")
#' disconnect(conn)
#' 
#' RSQLite::dbGetQuery(sqliteConnection, "SELECT COUNT(*) FROM foo;")
#' #   COUNT(*)
#' # 1     2694
#' }
#' @export
querySql.sqlite <- function(connection, 
                            sql, 
                            sqliteConnection, 
                            sqliteTableName, 
                            errorReportFile = file.path(getwd(), "errorReport.txt"), 
                            snakeCaseToCamelCase = FALSE) {
  if (inherits(connection, "DatabaseConnectorJdbcConnection") && rJava::is.jnull(connection@jConnection))
    stop("Connection is closed")
  # Calling splitSql, because this will also strip trailing semicolons (which cause Oracle to crash).
  sqlStatements <- SqlRender::splitSql(sql)
  if (length(sqlStatements) > 1)
    stop(paste("A query that returns a result can only consist of one SQL statement, but",
               length(sqlStatements),
               "statements were found"))
  tryCatch({
    lowLevelQuerySql.sqlite(connection = connection,
                            query = sqlStatements[1], 
                            sqliteConnection = sqliteConnection, 
                            sqliteTableName = sqliteTableName)
    columnNames <- RSQLite::dbListFields(sqliteConnection, sqliteTableName)
    newColumnNames <- toupper(columnNames)
    if (snakeCaseToCamelCase) { 
      newColumnNames <- SqlRender::snakeCaseToCamelCase(newColumnNames)
    }
    idx <- columnNames != newColumnNames
    if (any(idx)) {
      sql <- sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s;", sqliteTableName, columnNames[idx], newColumnNames[idx]) 
      lapply(sql, function(x) RSQLite::dbExecute(sqliteConnection, x)) 
    }
    invisible(sqliteConnection)
  }, error = function(err) {
    .createErrorReport(connection@dbms, err$message, sql, errorReportFile)
  })
}