# Copyright 2023 Observational Health Data Sciences and Informatics
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

#' Compute hash of data
#'
#' @description
#' Compute a hash of the data in the database schema. If the data changes, this
#' should produce a different hash code. Specifically, the hash is based on the
#' field names, field types, and table row counts.
#'
#' @param connection      The connection to the database server.
#' @template DatabaseSchema
#' @param tables          (Optional) A list of tables to restrict to.
#' @param progressBar     When true, a progress bar is shown based on the number of tables
#'                        in the database schema.
#'
#' @return
#' A string representing the MD5 hash code.
#'
#' @export
computeDataHash <- function(connection, databaseSchema, tables = NULL, progressBar = TRUE) {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertClass(connection, "DatabaseConnectorConnection", add = errorMessages)
  checkmate::assertCharacter(databaseSchema, len = 1, add = errorMessages)
  checkmate::assertCharacter(tables, min.len = 1, null.ok = TRUE, add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)

  startQuery <- Sys.time()
  tableNames <- DatabaseConnector::getTableNames(connection, databaseSchema)
  if (!is.null(tables)) {
    tableNames <- tableNames[tableNames %in% tables]
  }
  if (progressBar) {
    pb <- txtProgressBar(style = 3)
  }

  strings <- vector(mode = "character", length = length(tableNames))
  for (i in seq_along(tableNames)) {
    strings[i] <- getTableString(
      tableName = tableNames[i],
      connection = connection,
      databaseSchema = databaseSchema
    )
    if (progressBar) {
      setTxtProgressBar(pb, i / length(tableNames))
    }
  }
  if (progressBar) {
    close(pb)
  }

  hash <- digest::digest(strings, "md5")
  delta <- Sys.time() - startQuery
  writeLines(paste("Computing hash took", delta, attr(delta, "units")))
  return(hash)
}

# tableName = tableNames[1]
getTableString <- function(tableName, connection, databaseSchema) {
  row <- renderTranslateQuerySql(
    connection = connection,
    sql = "SELECT TOP 1 * FROM @database_schema.@table_name;",
    database_schema = databaseSchema,
    table_name = tableName
  )
  fieldNames <- names(row)
  fieldClasses <- sapply(row, class)
  if (dbms(connection) == "postgresql") {
    # COUNT(*) is too slow on PostgreSQL. Using pg_catalog to get count from latest
    # VACUUM / ANALYSE, which is probably accurate enough:
    sql <- "SELECT (CASE WHEN c.reltuples < 0 THEN NULL       -- never vacuumed
             WHEN c.relpages = 0 THEN float8 '0'  -- empty table
             ELSE c.reltuples / c.relpages END
     * (pg_catalog.pg_relation_size(c.oid)
      / pg_catalog.current_setting('block_size')::int)
       )::bigint
FROM   pg_catalog.pg_class c
WHERE  c.oid = '@database_schema.@table_name'::regclass;      -- schema-qualified table here"
    count <- renderTranslateQuerySql(
      connection = connection,
      sql = sql,
      database_schema = databaseSchema,
      table_name = tableName
    )[, 1]
  } else {
    count <- renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT COUNT(*) FROM @database_schema.@table_name;",
      database_schema = databaseSchema,
      table_name = tableName
    )[, 1]
  }
  return(paste(paste(paste(fieldNames, fieldClasses, sep = "="), collapse = ", "), sprintf("nrow=%d", count)))
}
