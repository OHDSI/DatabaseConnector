# @file ListTables.R
#
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

#' List all tables in a database schema.
#'
#' @description
#' This function returns a list of all tables in a database schema.
#'
#' @param connection       The connection to the database server.
#' @param databaseSchema   The name of the database schema. See details for platform-specific details.
#'
#' @details
#' The \code{databaseSchema} argument is interpreted differently according to the different platforms:
#' SQL Server and PDW: The databaseSchema schema should specify both the database and the schema, e.g.
#' 'my_database.dbo'. PostgreSQL and Redshift: The databaseSchema should specify the schema. Oracle:
#' The databaseSchema should specify the Oracle 'user'. MySql and Impala: The databaseSchema should
#' specify the database.
#'
#' @return
#' A character vector of table names. To ensure consistency across platforms, these table names are in
#' upper case.
#'
#' @export
getTableNames <- function(connection, databaseSchema) {
  if (connection@dbms %in% c("sqlite", "sqlite extended")) {
    tables <- dbListTables(connection@dbiConnection, schema = databaseSchema)
    return(toupper(tables))
  }

  if (is.null(databaseSchema)) {
    database <- rJava::.jnull("java/lang/String")
    schema <- rJava::.jnull("java/lang/String")
  } else {
    if (connection@dbms == "oracle") {
      databaseSchema <- toupper(databaseSchema)
    }
    if (connection@dbms == "redshift") {
      databaseSchema <- tolower(databaseSchema)
    }
    databaseSchema <- strsplit(databaseSchema, "\\.")[[1]]
    if (length(databaseSchema) == 1) {
      if (connection@dbms %in% c("sql server", "pdw")) {
        database <- cleanDatabaseOrSchemaName(databaseSchema)
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
