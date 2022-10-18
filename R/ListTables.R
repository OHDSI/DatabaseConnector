# @file ListTables.R
#
# Copyright 2022 Observational Health Data Sciences and Informatics
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

#' List all tables in a database schema.
#'
#' @description
#' This function returns a list of all tables in a database schema.
#'
#' @param connection       The connection to the database server.
#' @template DatabaseSchema
#'
#' @return
#' A character vector of table names. To ensure consistency across platforms, these table names are in
#' upper case.
#'
#' @export
getTableNames <- function(connection, databaseSchema) {
  if (dbms(connection) %in% c("sqlite", "sqlite extended")) {
    tables <- dbListTables(connection@dbiConnection, schema = databaseSchema)
    return(toupper(tables))
  }
  
  if (dbms(connection) == "duckdb") {
    tables <- dbListTables(connection@dbiConnection)
    return(toupper(tables))
  }

  if (is.null(databaseSchema)) {
    database <- rJava::.jnull("java/lang/String")
    schema <- rJava::.jnull("java/lang/String")
  } else {
    if (dbms(connection) == "oracle") {
      databaseSchema <- toupper(databaseSchema)
    }
    if (dbms(connection) == "redshift") {
      databaseSchema <- tolower(databaseSchema)
    }
    databaseSchema <- strsplit(databaseSchema, "\\.")[[1]]
    if (length(databaseSchema) == 1) {
      if (dbms(connection) %in% c("sql server", "pdw")) {
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
  metaData <- rJava::.jcall(connection@jConnection, "Ljava/sql/DatabaseMetaData;", "getMetaData")
  types <- rJava::.jarray(c("TABLE", "VIEW"))
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
  return(toupper(tables))
}

#' Does the table exist?
#'
#' @description
#' Checks whether a table exists. Accounts for surrounding escape characters. 
#' Case insensitive.
#'
#' @param connection       The connection to the database server.
#' @template DatabaseSchema
#' @param tableName        The name of the table to check.
#'
#' @return
#' A logical value indicating whether the table exits.
#'
#' @export
existsTable <- function(connection, databaseSchema, tableName) {
  tables <- getTableNames(connection, databaseSchema)
  tableName <- toupper(cleanTableName(tableName))
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
