# @file ListTables.R
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
  dbms <- attr(connection, "dbms")
  if (dbms == "mysql" || dbms == "impala") {
    query <- paste("SHOW TABLES IN", databaseSchema)
  } else if (dbms == "sql server" || dbms == "pdw") {
    databaseSchema <- strsplit(databaseSchema, "\\.")[[1]]
    if (length(databaseSchema) == 1) {
      database <- databaseSchema
      schema <- "dbo"
    } else {
      database <- databaseSchema[1]
      schema <- databaseSchema[2]
    }
    query <- paste0("SELECT table_name FROM ",
                    database,
                    ".information_schema.tables WHERE table_schema = '",
                    schema,
                    "' ORDER BY table_name")
  } else if (dbms == "oracle") {
    query <- paste0("SELECT table_name FROM all_tables WHERE owner='",
                    toupper(databaseSchema),
                    "' ORDER BY table_name")
  } else if (dbms == "postgresql" || dbms == "redshift") {
    query <- paste0("SELECT table_name FROM information_schema.tables WHERE table_schema = '",
                    tolower(databaseSchema),
                    "' ORDER BY table_name")
  } else if (dbms == "bigquery") {
    query <- paste0("SELECT table_id as table_name FROM ",
                    databaseSchema,
                    ".__TABLES__ ORDER BY table_name")
  }
  tables <- querySql(connection, query)
  return(toupper(tables[, 1]))
}
