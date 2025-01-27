# @file ListTables.R
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

#' @inherit DBI::dbListTables title description params details references return seealso
#' 
#' @template DatabaseSchema
#' @param ... Not used
#'
#' @export
setMethod(
  "dbListTables",
  "DatabaseConnectorConnection",
  function(conn, databaseSchema = NULL, ...) {
    errorMessages <- checkmate::makeAssertCollection()
    checkmate::assertCharacter(databaseSchema, len = 1, null.ok = TRUE, add = errorMessages)
    checkmate::reportAssertions(collection = errorMessages)
    
    if (is(conn, "DatabaseConnectorDbiConnection")) {
      if (!is.null(databaseSchema) && dbms(conn) == "spark" && grepl("\\.", databaseSchema)) {
        databaseSchema <- strsplit(databaseSchema, "\\.")[[1]]
        tables <- DBI::dbListTables(
          conn@dbiConnection, 
          catalog = cleanSchemaName(databaseSchema[1]),
          schema = cleanSchemaName(databaseSchema[2])
        )
      } else if (!is.null(databaseSchema) && dbms(conn) == "duckdb") {
        if (grepl("\\.", databaseSchema)) {
          databaseSchema <- strsplit(databaseSchema, "\\.")[[1]][2]
        }
        sql <- sprintf("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s';", databaseSchema)
        tables <- querySql(conn, sql)[[1]]
      } else {
        tables <- DBI::dbListTables(conn@dbiConnection, schema = databaseSchema)
      }
      return(tolower(tables))
    }
    
    if (is.null(databaseSchema)) {
      database <- rJava::.jnull("java/lang/String")
      schema <- rJava::.jnull("java/lang/String")
    } else {
      if (dbms(conn) %in% c("oracle", "snowflake")) {
        databaseSchema <- toupper(databaseSchema)
      }
      if (dbms(conn) == "redshift") {
        databaseSchema <- tolower(databaseSchema)
      }
      databaseSchema <- strsplit(databaseSchema, "\\.")[[1]]
      if (length(databaseSchema) == 1) {
        if (dbms(conn) %in% c("sql server", "pdw")) {
          database <- cleanDatabaseName(databaseSchema)
          schema <- "dbo"
        } else {
          database <- rJava::.jnull("java/lang/String")
          schema <- cleanSchemaName(databaseSchema)
        }
      } else {
        database <- cleanDatabaseName(databaseSchema[1])
        schema <- cleanSchemaName(databaseSchema[2])
      }
    }
    metaData <- rJava::.jcall(conn@jConnection, "Ljava/sql/DatabaseMetaData;", "getMetaData")
    types <- rJava::.jarray(c("TABLE", "VIEW", "EXTERNAL TABLE"))
    resultSet <- rJava::.jcall(metaData,
                               "Ljava/sql/ResultSet;",
                               "getTables",
                               database,
                               schema,
                               rJava::.jnull("java/lang/String"),
                               types,
                               check = FALSE
    )
    tables <- character()
    while (rJava::.jcall(resultSet, "Z", "next")) {
      tables <- c(tables, rJava::.jcall(resultSet, "S", "getString", "TABLE_NAME"))
    }
    return(tolower(tables))
  })

#' List all tables in a database schema.
#'
#' @description
#' This function returns a list of all tables in a database schema.
#'
#' @template Connection
#' @template DatabaseSchema
#' @param cast Should the table names be cast to uppercase or lowercase before being returned? 
#' Valid options are "upper" , "lower" (default), "none" (no casting is done)
#'
#' @return A character vector of table names. 
#'
#' @export
getTableNames <- function(connection, databaseSchema = NULL, cast = "lower") {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertTRUE(DBI::dbIsValid(connection))
  checkmate::assertCharacter(databaseSchema, len = 1, null.ok = TRUE, add = errorMessages)
  checkmate::assertChoice(cast, c("upper", "lower", "none"), add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)
  
  if (is.null(databaseSchema)) {
    tableNames <- DBI::dbListTables(connection)
  } else if (is(connection, "DatabaseConnectorConnection")) {
    tableNames <- DBI::dbListTables(conn = connection, databaseSchema = databaseSchema)
  } else if (is(connection, "PqConnection") || is(connection, "RedshiftConnection") || is(connection, "duckdb_connection")) {
    stopifnot(length(databaseSchemaSplit) == 1)
    sql <- paste0("SELECT table_name FROM information_schema.tables WHERE table_schema = '", databaseSchema, "';")
    tableNames <- DBI::dbGetQuery(connection, sql)[["table_name"]]
  } else if (is(connection, "Microsoft SQL Server")) {
    databaseSchemaSplit <- strsplit(databaseSchema, "\\.")[[1]]
    if (!(length(databaseSchemaSplit) %in% 1:2)) rlang::abort("databaseSchema can contain at most one dot (.)")
    
    if (length(databaseSchemaSplit) == 1) {
      tableNames <- DBI::dbListTables(connection, schema_name = databaseSchemaSplit)
    } else {
      tableNames <- DBI::dbListTables(connection, catalog_name = databaseSchemaSplit[[1]], schema_name = databaseSchemaSplit[[2]])
    }
  } else if (is(connection, "SQLiteConnection")) {
    if (databaseSchema != "main") rlang::abort("The only schema supported on SQLite is 'main'")
    tableNames <- DBI::dbListTables(connection)
  } else {
    rlang::abort(paste(paste(class(connection), collapse = ", "), "connection not supported"))
  }
  
  switch(cast,
         "upper" = toupper(tableNames),
         "lower" = tolower(tableNames),
         "none" = tableNames
  )
}

#' Does the table exist?
#'
#' @description
#' Checks whether a table exists. Accounts for surrounding escape characters. 
#' Case insensitive.
#'
#' @template Connection
#' @template DatabaseSchema
#' @param tableName        The name of the table to check.
#'
#' @return
#' A logical value indicating whether the table exits.
#'
#' @export
existsTable <- function(connection, databaseSchema, tableName) {
  tables <- getTableNames(connection, databaseSchema)
  tableName <- tolower(cleanTableName(tableName))
  return(tableName %in% tables)
}

cleanDatabaseName <- function(name) {
  if (grepl("^\\[.*\\]$", name) || grepl("^\".*\"$", name)) {
    name <- substr(name, 2, nchar(name) - 1)
  }
  return(name)
}

cleanSchemaName <- function(name) {
  # JDBC interprets schema as a regular expression, so make valid expression
  name <- cleanDatabaseName(name)
  name <- gsub("\\\\", "\\\\\\\\", name)
  return(name)
}

cleanTableName <- function(name) {
  return(cleanDatabaseName(name))
}
