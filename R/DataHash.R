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

#' Compute hash of data
#'
#' @description
#' Compute a hash of the data in the database schema. If the data changes, this
#' should produce a different hash code. Specifically, the hash is based on the
#' field names, field types, and table row counts.
#'
#' @template Connection
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
  if (dbms(connection) == "sql server") {
    strings <- bulkHashSqlServer(connection, databaseSchema, tables)
  } else if (dbms(connection) == "postgresql") {
    strings <- bulkHashPostgreSql(connection, databaseSchema, tables)
  } else if (dbms(connection) == "redshift") {
    strings <- bulkHashRedShift(connection, databaseSchema, tables)
  } else {
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
  
  count <- renderTranslateQuerySql(
    connection = connection,
    sql = "SELECT COUNT(*) FROM @database_schema.@table_name;",
    database_schema = databaseSchema,
    table_name = tableName
  )[, 1]
  return(paste(tableName, paste(paste(fieldNames, fieldClasses, sep = "="), collapse = ", "), sprintf("nrow=%d", count)))
}

bulkHashSqlServer <- function(connection, databaseSchema, tables) {
  databaseSchema <- strsplit(databaseSchema, "\\.")[[1]]
  if (length(databaseSchema) == 1) {
    database <- cleanDatabaseName(databaseSchema)
    schema <- "dbo"
  } else {
    database <- cleanDatabaseName(databaseSchema[1])
    schema <- cleanSchemaName(databaseSchema[2])
  }
  tryCatch(
    lowLevelExecuteSql(
      connection = connection,
      sql = SqlRender::render("USE [@database];", database = database)
    ),
    error = function(e) {
      # Do nothing (assuming this is Azure, where you can't use USE)
    }
  )
  sql <- "
    SELECT LOWER(objects.name) AS table_name,
      SUM(partitions.Rows) AS row_count
    FROM sys.objects
    INNER JOIN sys.partitions
      ON objects.object_id = partitions.object_id
    WHERE objects.type = 'U'
      AND objects.is_ms_shipped = 0x0
      AND index_id < 2 -- 0:Heap, 1:Clustered
      AND LOWER(SCHEMA_NAME(schema_id)) = LOWER('@schema')
    GROUP BY objects.name;"
  
  rowCounts <- renderTranslateQuerySql(
    connection = connection,
    sql = sql,
    schema = schema,
    snakeCaseToCamelCase = TRUE
  )
  
  sql <- "
    SELECT LOWER(tables.name) AS table_name, 
      LOWER(columns.name) as column_name, 
      types.name AS data_type
    FROM sys.tables 
    INNER JOIN sys.columns
      ON tables.object_id = columns.object_id
    LEFT JOIN sys.types
      ON columns.user_type_id = types.user_type_id
    WHERE LOWER(SCHEMA_NAME(tables.schema_id)) = LOWER('@schema');"
  columnTypes <- renderTranslateQuerySql(
    connection = connection,
    sql = sql,
    schema = schema,
    snakeCaseToCamelCase = TRUE
  )
  if (!is.null(tables)) {
    rowCounts <- rowCounts[rowCounts$tableName %in% tolower(tables), ]
    columnTypes <- columnTypes[rowCounts$tableName %in% tolower(tables), ]
  }
  # subset = subsets[[1]]
  createStringPerTable <- function(subset) {
    tableName <- subset$tableName[1]
    rowCount <- rowCounts$rowCount[rowCounts$tableName == tableName]
    return(paste(tableName, paste(paste(subset$columnName, subset$dataType, sep = "="), collapse = ", "), sprintf("nrow=%s", rowCount)))
  }
  subsets <- split(columnTypes, columnTypes$tableName)
  strings <- sapply(subsets, createStringPerTable)
  names(strings) <- NULL
  return(strings)
}

bulkHashPostgreSql <- function(connection, databaseSchema, tables) {
  sql <- "
    SELECT relname AS table_name,
      (CASE 
        WHEN c.reltuples < 0 THEN NULL       -- never vacuumed
        WHEN c.relpages = 0 THEN float8 '0'  -- empty table
        ELSE c.reltuples / c.relpages 
       END
      * (pg_catalog.pg_relation_size(c.oid)
      / pg_catalog.current_setting('block_size')::int)
      )::bigint AS row_count
    FROM   pg_catalog.pg_class c
    INNER JOIN pg_catalog.pg_namespace ns
      ON c.relnamespace = ns.oid
    WHERE  LOWER(nspname) = '@schema'
      AND c.relkind IN('r', 't', 'f', 'p');"
  rowCounts <- renderTranslateQuerySql(
    connection = connection,
    sql = sql,
    schema = tolower(databaseSchema),
    snakeCaseToCamelCase = TRUE
  )
  # Check:
  # renderTranslateQuerySql(connection, "SELECT COUnT(*) FROM @schema.care_site;", schema = databaseSchema)
  # rowCounts[rowCounts$tableName == "care_site", ]
  
  sql <- "
    SELECT table_name,
      column_name,
      data_type
    FROM information_schema.columns 
    WHERE table_schema = '@schema';"
  columnTypes <- renderTranslateQuerySql(
    connection = connection,
    sql = sql,
    schema = tolower(databaseSchema),
    snakeCaseToCamelCase = TRUE
  )
  if (!is.null(tables)) {
    rowCounts <- rowCounts[rowCounts$tableName %in% tolower(tables), ]
    columnTypes <- columnTypes[rowCounts$tableName %in% tolower(tables), ]
  }
  # subset = subsets[[1]]
  createStringPerTable <- function(subset) {
    tableName <- subset$tableName[1]
    rowCount <- rowCounts$rowCount[rowCounts$tableName == tableName]
    return(paste(tableName, paste(paste(subset$columnName, subset$dataType, sep = "="), collapse = ", "), sprintf("nrow=%s", rowCount)))
  }
  subsets <- split(columnTypes, columnTypes$tableName)
  strings <- sapply(subsets, createStringPerTable)
  names(strings) <- NULL
  return(strings)
}

bulkHashRedShift <- function(connection, databaseSchema, tables) {
  sql <- "
    SELECT relname AS table_name,
      c.reltuples AS row_count
    FROM pg_catalog.pg_class c
    INNER JOIN pg_catalog.pg_namespace ns
      ON c.relnamespace = ns.oid
    WHERE  LOWER(nspname) = '@schema'
      AND c.relkind IN('r', 't', 'f', 'p');"
  rowCounts <- renderTranslateQuerySql(
    connection = connection,
    sql = sql,
    schema = tolower(databaseSchema),
    snakeCaseToCamelCase = TRUE
  )
  # Check:
  # renderTranslateQuerySql(connection, "SELECT COUnT(*) FROM @schema.care_site;", schema = databaseSchema)
  # rowCounts[rowCounts$tableName == "care_site", ]
  
  sql <- "
    SELECT table_name,
      column_name,
      data_type
    FROM information_schema.columns 
    WHERE table_schema = '@schema';"
  columnTypes <- renderTranslateQuerySql(
    connection = connection,
    sql = sql,
    schema = tolower(databaseSchema),
    snakeCaseToCamelCase = TRUE
  )
  if (!is.null(tables)) {
    rowCounts <- rowCounts[rowCounts$tableName %in% tolower(tables), ]
    columnTypes <- columnTypes[rowCounts$tableName %in% tolower(tables), ]
  }
  # subset = subsets[[1]]
  createStringPerTable <- function(subset) {
    tableName <- subset$tableName[1]
    rowCount <- rowCounts$rowCount[rowCounts$tableName == tableName]
    return(paste(tableName, paste(paste(subset$columnName, subset$dataType, sep = "="), collapse = ", "), sprintf("nrow=%s", rowCount)))
  }
  subsets <- split(columnTypes, columnTypes$tableName)
  strings <- sapply(subsets, createStringPerTable)
  names(strings) <- NULL
  return(strings)
}
