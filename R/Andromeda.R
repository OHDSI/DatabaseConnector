# Copyright 2021 Observational Health Data Sciences and Informatics
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

#' Low level function for retrieving data to a local Andromeda database
#'
#' @description
#' This is the equivalent of the \code{\link{querySqlToAndromeda}} function, except no error report is
#' written when an error occurs.
#'
#' @param connection      The connection to the database server.
#' @param query           The SQL statement to retrieve the data
#' @param datesAsString   Should dates be imported as character vectors, our should they be converted
#'                        to R's date format?
#' @param andromeda An open connection to a Andromeda database, for example as created using \code{\link[Andromeda]{andromeda}}.
#' @param andromedaTableName  The name of the table in the local Andromeda database where the results of the query will be stored.
#'
#' @details
#' Retrieves data from the database server and stores it in a local Andromeda database This allows very large
#' data sets to be retrieved without running out of memory. Null values in the database are converted
#' to NA values in R. If a table with the same name already exists in the local Andromeda database it is replaced.
#'
#' @return
#' Invisibly returns the andromeda. The Andromeda database will have a table added with the query results.
#'
#' @export
lowLevelQuerySqlToAndromeda <- function(connection, query, andromeda, andromedaTableName, datesAsString = FALSE) {
  UseMethod("lowLevelQuerySqlToAndromeda", connection)
}

#' @export
lowLevelQuerySqlToAndromeda.default <- function(connection, query, andromeda, andromedaTableName, datesAsString = FALSE) {
  if (rJava::is.jnull(connection@jConnection))
    stop("Connection is closed")
  batchedQuery <- rJava::.jnew("org.ohdsi.databaseConnector.BatchedQuery",
                               connection@jConnection,
                               query,
                               connection@dbms)
  
  on.exit(rJava::.jcall(batchedQuery, "V", "clear"))
  
  columnTypes <- rJava::.jcall(batchedQuery, "[I", "getColumnTypes")
  if (length(columnTypes) == 0)
    stop("No columns found")
  if (any(columnTypes == 5)) {
    validateInt64Query()
  }
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
      } else if (columnTypes[i] == 5) {
        column <- rJava::.jcall(batchedQuery,
                                "[D",
                                "getInteger64",
                                as.integer(i)) 
        oldClass(column) <- "integer64"
        batch[[i]] <- column
      } else if (columnTypes[i] == 6) {
        column <- rJava::.jcall(batchedQuery,
                                "[I",
                                "getInteger",
                                as.integer(i))
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
    RSQLite::dbWriteTable(conn = andromeda,
                          name = andromedaTableName,
                          value = batch,
                          overwrite = first,
                          append = !first)
    first <- FALSE
  }
  invisible(andromeda)
}

#' @export
lowLevelQuerySqlToAndromeda.DatabaseConnectorDbiConnection <- function(connection, query, andromeda, andromedaTableName, datesAsString = FALSE) {
  results <- lowLevelQuerySql(connection, query)
  
  RSQLite::dbWriteTable(conn = andromeda,
                        name = andromedaTableName,
                        value = results,
                        overwrite = TRUE,
                        append = FALSE)
  invisible(andromeda)
}

#' Retrieves data to a local Andromeda database
#'
#' @description
#' This function sends SQL to the server, and returns the results in a local Andromeda database.
#'
#' @param connection           The connection to the database server.
#' @param sql                  The SQL to be sent.
#' @param andromeda An open connection to a Andromeda database, for example as created using \code{\link[Andromeda]{andromeda}}.
#' @param andromedaTableName  The name of the table in the local Andromeda database where the results of the query will be stored.
#' @param errorReportFile      The file where an error report will be written if an error occurs. Defaults to
#'                             'errorReportSql.txt' in the current working directory.
#' @param snakeCaseToCamelCase If true, field names are assumed to use snake_case, and are converted to camelCase.
#'
#' @details
#' Retrieves data from the database server and stores it in a local Andromeda database. This allows very large
#' data sets to be retrieved without running out of memory. If an error occurs during SQL execution,
#' this error is written to a file to facilitate debugging. Null values in the database are converted
#' to NA values in R.If a table with the same name already exists in the local Andromeda database it is replaced.
#'
#' @return
#' @return
#' Invisibly returns the andromeda. The Andromeda database will have a table added with the query results.
#' 
#' @examples
#' \dontrun{
#' andromeda <- Andromeda::andromeda()
#' connectionDetails <- createConnectionDetails(dbms = "postgresql",
#'                                              server = "localhost",
#'                                              user = "root",
#'                                              password = "blah",
#'                                              schema = "cdm_v4")
#' conn <- connect(connectionDetails)
#' querySqlToAndromeda(connection = conn, 
#'                     sql = "SELECT * FROM person;", 
#'                     andromeda = andromeda,
#'                     andromedaTableName = "foo")
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
                                snakeCaseToCamelCase = FALSE) {
  if (inherits(connection, "DatabaseConnectorJdbcConnection") && rJava::is.jnull(connection@jConnection))
    stop("Connection is closed")
  if (!inherits(andromeda, "SQLiteConnection"))
    stop("The andromeda argument must be an Andromeda object (or SQLiteConnection objecT).")
  
  # Calling splitSql, because this will also strip trailing semicolons (which cause Oracle to crash).
  sqlStatements <- SqlRender::splitSql(sql)
  if (length(sqlStatements) > 1)
    stop(paste("A query that returns a result can only consist of one SQL statement, but",
               length(sqlStatements),
               "statements were found"))
  tryCatch({
    lowLevelQuerySqlToAndromeda(connection = connection,
                                query = sqlStatements[1], 
                                andromeda = andromeda, 
                                andromedaTableName = andromedaTableName)
    columnNames <- RSQLite::dbListFields(andromeda, andromedaTableName)
    newColumnNames <- toupper(columnNames)
    if (snakeCaseToCamelCase) { 
      newColumnNames <- SqlRender::snakeCaseToCamelCase(newColumnNames)
    }
    idx <- columnNames != newColumnNames
    if (any(idx)) {
      sql <- sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s;", andromedaTableName, columnNames[idx], newColumnNames[idx]) 
      lapply(sql, function(x) RSQLite::dbExecute(andromeda, x)) 
    }
    invisible(andromeda)
  }, error = function(err) {
    .createErrorReport(connection@dbms, err$message, sql, errorReportFile)
  })
}

#' Render, translate, and query to local Andromeda
#'
#' @description
#' This function renders, and translates SQL, sends it to the server, and returns the results as an ffdf object
#'
#' @param connection           The connection to the database server.
#' @param sql                  The SQL to be send.
#' @param andromeda An open connection to a Andromeda database, for example as created using \code{\link[Andromeda]{andromeda}}.
#' @param andromedaTableName  The name of the table in the local Andromeda database where the results of the query will be stored.
#' @param errorReportFile      The file where an error report will be written if an error occurs. Defaults to
#'                             'errorReportSql.txt' in the current working directory.
#' @param snakeCaseToCamelCase If true, field names are assumed to use snake_case, and are converted to camelCase.  
#' @param oracleTempSchema    DEPRECATED: use \code{tempEmulationSchema} instead.
#' @param tempEmulationSchema Some database platforms like Oracle and Impala do not truly support temp tables. To
#'                            emulate temp tables, provide a schema with write privileges where temp tables
#'                            can be created.
#' @param ...                  Parameters that will be used to render the SQL.
#'
#' @details
#' This function calls the \code{render} and \code{translate} functions in the SqlRender package before 
#' calling \code{\link{querySqlToAndromeda}}.
#'
#' @return
#' Invisibly returns the andromeda. The Andromeda database will have a table added with the query results.
#'
#' @examples
#' \dontrun{
#' connectionDetails <- createConnectionDetails(dbms = "postgresql",
#'                                              server = "localhost",
#'                                              user = "root",
#'                                              password = "blah",
#'                                              schema = "cdm_v4")
#' conn <- connect(connectionDetails)
#' renderTranslatequerySqlToAndromeda(conn, 
#'                                    sql = "SELECT * FROM @@schema.person",
#'                                    schema = "cdm_synpuf",
#'                                    andromeda = andromeda,
#'                                    andromedaTableName = "foo")
#' disconnect(conn)
#' 
#' andromeda$foo
#' }
#' @export
renderTranslateQuerySqlToAndromeda <- function(connection, 
                                               sql, 
                                               andromeda,
                                               andromedaTableName,
                                               errorReportFile = file.path(getwd(), "errorReportSql.txt"), 
                                               snakeCaseToCamelCase = FALSE,
                                               oracleTempSchema = NULL,
                                               tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                               ...) {
  if (!is.null(oracleTempSchema) && oracleTempSchema != "") {
    warn("The 'oracleTempSchema' argument is deprecated. Use 'tempEmulationSchema' instead.",
         .frequency = "regularly",
         .frequency_id = "oracleTempSchema")
    tempEmulationSchema <- oracleTempSchema
  }
  sql <- SqlRender::render(sql, ...)
  sql <- SqlRender::translate(sql, targetDialect = connection@dbms, oracleTempSchema = tempEmulationSchema)
  return(querySqlToAndromeda(connection = connection,
                             sql = sql,
                             andromeda = andromeda,
                             andromedaTableName = andromedaTableName,
                             errorReportFile = errorReportFile,
                             snakeCaseToCamelCase = snakeCaseToCamelCase))
}
