# @file RStudio.R
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

registerWithRStudio <- function(connection) {
  observer <- getOption("connectionObserver")
  if (!is.null(observer)) {
    registeredDisplayNames <- getOption("registeredDisplayNames")
    if (is.null(registeredDisplayNames)) {
      registeredDisplayNames <- data.frame()
    }
    server <- getServer(connection)
    displayName <- server
    i <- 1
    while (displayName %in% registeredDisplayNames$displayName) {
      i <- i + 1
      displayName <- paste0(server, " (", i, ")")
    }
    registeredDisplayNames <- rbind(
      registeredDisplayNames,
      data.frame(
        uuid = connection@uuid,
        displayName = displayName,
        stringsAsFactors = FALSE
      )
    )
    options(registeredDisplayNames = registeredDisplayNames)
    observer$connectionOpened(
      type = compileTypeLabel(connection),
      displayName = displayName,
      host = displayName,
      connectCode = compileReconnectCode(connection),
      icon = "",
      disconnect = function() {
        disconnect(connection)
      }, listObjectTypes = function() {
        listDatabaseConnectorObjectTypes(connection)
      }, listObjects = function(...) {
        listDatabaseConnectorObjects(connection, ...)
      }, listColumns = function(...) {
        listDatabaseConnectorColumns(connection, ...)
      }, previewObject = function(rowLimit, ...) {
        previewObject(connection, rowLimit, ...)
      }, actions = connectionActions(connection), connectionObject = connection
    )
  }
}

compileTypeLabel <- function(connection) {
  return(paste0("DatabaseConnector (", dbms(connection), ")"))
}

unregisterWithRStudio <- function(connection) {
  observer <- getOption("connectionObserver")
  if (!is.null(observer)) {
    registeredDisplayNames <- getOption("registeredDisplayNames")
    displayName <- registeredDisplayNames$displayName[registeredDisplayNames$uuid == connection@uuid]
    registeredDisplayNames <- registeredDisplayNames[registeredDisplayNames$uuid != connection@uuid, ]
    options(registeredDisplayNames = registeredDisplayNames)
    observer$connectionClosed(compileTypeLabel(connection), displayName)
  }
}

hasCatalogs <- function(connection) {
  return(dbms(connection) %in% c("pdw", "postgresql", "sql server", "synapse", "redshift", "snowflake", "spark", "bigquery", "duckdb"))
}

listDatabaseConnectorColumns <- function(connection,
                                         catalog = NULL,
                                         schema = NULL,
                                         table = NULL,
                                         ...) {
  UseMethod("listDatabaseConnectorColumns", connection)
}

#' @exportS3Method
listDatabaseConnectorColumns.default <- function(connection,
                                                 catalog = NULL,
                                                 schema = NULL,
                                                 table = NULL,
                                                 ...) {
  if (dbms(connection) == "oracle") {
    table <- toupper(table)
    if (!is.null(catalog)) {
      catalog <- toupper(catalog)
    }
    if (!is.null(schema)) {
      schema <- toupper(schema)
    }
  } else {
    table <- tolower(table)
    if (!is.null(catalog)) {
      catalog <- tolower(catalog)
    }
    if (!is.null(schema)) {
      schema <- tolower(schema)
    }
  }
  if (is.null(catalog)) {
    catalog <- rJava::.jnull("java/lang/String")
  }
  if (is.null(schema)) {
    schema <- rJava::.jnull("java/lang/String")
  }

  metaData <- rJava::.jcall(connection@jConnection, "Ljava/sql/DatabaseMetaData;", "getMetaData")
  resultSet <- rJava::.jcall(
    metaData,
    "Ljava/sql/ResultSet;",
    "getColumns",
    catalog,
    schema,
    table,
    rJava::.jnull("java/lang/String")
  )
  on.exit(rJava::.jcall(resultSet, "V", "close"))
  fields <- character()
  types <- character()
  while (rJava::.jcall(resultSet, "Z", "next")) {
    fields <- c(fields, rJava::.jcall(resultSet, "S", "getString", "COLUMN_NAME"))
    types <- c(types, rJava::.jcall(resultSet, "S", "getString", "TYPE_NAME"))
  }
  return(data.frame(name = fields, type = types, stringsAsFactors = FALSE))
}

#' @exportS3Method
listDatabaseConnectorColumns.DatabaseConnectorDbiConnection <- function(connection,
                                                                        catalog = NULL,
                                                                        schema = NULL,
                                                                        table = NULL,
                                                                        ...) {
  if (!is.null(schema)) {
    table <- paste(schema, table, sep = ".")
  }
  if (!is.null(catalog)) {
    table <- paste(catalog, table, sep = ".")
  }
  res <- DBI::dbSendQuery(connection@dbiConnection, sprintf("SELECT * FROM %s LIMIT 0;", table))
  info <- dbColumnInfo(res)
  dbClearResult(res)
  if (dbms(connection) == "sqlite") {
    info$type[grepl("DATE$", info$name)] <- "date"
    info$type[grepl("DATETIME$", info$name)] <- "datetime"
  }
  return(info)
}

listDatabaseConnectorObjects <- function(connection, catalog = NULL, schema = NULL, ...) {
  if (is.null(catalog) && hasCatalogs(connection)) {
    catalogs <- getCatalogs(connection)
    return(data.frame(
      name = catalogs,
      type = rep("catalog", times = length(catalogs)),
      stringsAsFactors = FALSE
    ))
  }
  if (is.null(schema)) {
    schemas <- getSchemaNames(connection, catalog)
    return(data.frame(
      name = schemas,
      type = rep("schema", times = length(schemas)),
      stringsAsFactors = FALSE
    ))
  }
  if (!hasCatalogs(connection) || dbms(connection) %in% c("postgresql", "redshift", "sqlite", "sqlite extended", "bigquery")) {
    databaseSchema <- schema
  } else {
    databaseSchema <- paste(catalog, schema, sep = ".")
  }
  tables <- getTableNames(connection, databaseSchema)
  return(data.frame(
    name = tables,
    type = rep("table", times = length(tables)),
    stringsAsFactors = FALSE
  ))
}

listDatabaseConnectorObjectTypes <- function(connection) {
  types <- list(schema = list(contains = c(
    list(table = list(contains = "data")),
    list(view = list(contains = "data"))
  )))
  if (hasCatalogs(connection)) {
    types <- list(catalog = list(contains = types))
  }
  return(types)
}

previewObject <- function(connection, rowLimit, catalog = NULL, table = NULL, schema = NULL) {
  if (!hasCatalogs(connection) || dbms(connection) %in% c("postgresql", "redshift", "sqlite", "sqlite extended", "bigquery")) {
    databaseSchema <- schema
  } else {
    databaseSchema <- paste(catalog, schema, sep = ".")
  }
  sql <- "SELECT TOP 1000 * FROM @databaseSchema.@table;"
  sql <- SqlRender::render(sql = sql, databaseSchema = databaseSchema, table = table)
  sql <- SqlRender::translate(sql = sql, targetDialect = dbms(connection))
  querySql(connection, sql)
}

connectionActions <- function(connection) {
  list(Help = list(icon = "", callback = function() {
    utils::browseURL("http://ohdsi.github.io/DatabaseConnector/")
  }))
}

getServer <- function(connection) {
  UseMethod("getServer", connection)
}

#' @exportS3Method
getServer.default <- function(connection) {
  if (dbms(connection) == "hive") {
    url <- connection@url
  } else {
    databaseMetaData <- rJava::.jcall(
      connection@jConnection,
      "Ljava/sql/DatabaseMetaData;",
      "getMetaData"
    )
    url <- rJava::.jcall(databaseMetaData, "Ljava/lang/String;", "getURL")
  }
  server <- urltools::url_parse(url)$domain
  return(server)
}

#' @exportS3Method
getServer.DatabaseConnectorDbiConnection <- function(connection) {
  return(connection@server)
}

compileReconnectCode <- function(connection) {
  UseMethod("compileReconnectCode", connection)
}

#' @exportS3Method
compileReconnectCode.default <- function(connection) {
  databaseMetaData <- rJava::.jcall(
    connection@jConnection,
    "Ljava/sql/DatabaseMetaData;",
    "getMetaData"
  )
  if (dbms(connection) == "hive") {
    url <- connection@url
    user <- connection@user
  } else {
    url <- rJava::.jcall(databaseMetaData, "Ljava/lang/String;", "getURL")
    user <- rJava::.jcall(databaseMetaData, "Ljava/lang/String;", "getUserName")
  }
  code <- sprintf(
    "library(DatabaseConnector)\ncon <- connect(dbms = \"%s\", connectionString = \"%s\", user = \"%s\", password = password)",
    dbms(connection),
    url,
    user
  )
  return(code)
}

#' @exportS3Method
compileReconnectCode.DatabaseConnectorDbiConnection <- function(connection) {
  code <- sprintf(
    "library(DatabaseConnector)\ncon <- connect(dbms = \"%s\", server = \"%s\")",
    dbms(connection),
    connection@server
  )
  return(code)
}

getSchemaNames <- function(conn, catalog = NULL) {
  UseMethod("getSchemaNames", conn)
}

#' @exportS3Method
getSchemaNames.default <- function(conn, catalog = NULL) {
  if (is.null(catalog)) {
    catalog <- rJava::.jnull("java/lang/String")
  }
  metaData <- rJava::.jcall(conn@jConnection, "Ljava/sql/DatabaseMetaData;", "getMetaData")
  resultSet <- rJava::.jcall(
    metaData,
    "Ljava/sql/ResultSet;",
    "getSchemas",
    catalog,
    rJava::.jnull("java/lang/String")
  )
  on.exit(rJava::.jcall(resultSet, "V", "close"))
  schemas <- character()
  while (rJava::.jcall(resultSet, "Z", "next")) {
    thisCatalog <- rJava::.jcall(resultSet, "S", "getString", "TABLE_CATALOG")
    if (rJava::is.jnull(thisCatalog) || (!rJava::is.jnull(catalog) && thisCatalog == catalog)) {
      schemas <- c(schemas, rJava::.jcall(resultSet, "S", "getString", "TABLE_SCHEM"))
    }
  }
  return(schemas)
}

#' @exportS3Method
getSchemaNames.DatabaseConnectorDbiConnection <- function(conn, catalog = NULL) {
  if (dbms(conn) %in% c("sqlite", "sqlite extended")) {
    return("main")
  } else if (dbms(conn) == "spark") {
    schemas <- DBI::dbGetQuery(conn@dbiConnection, "SHOW DATABASES")
    return(schemas[, 1])
  } else if (conn@dbms == "duckdb") {
    return(dbGetQuery(conn, sprintf("SELECT schema_name FROM information_schema.schemata WHERE catalog_name = '%s'", catalog))$schema_name)
  } else {
    schemas <- DBI::dbGetQuery(conn@dbiConnection, "SELECT schema_name FROM information_schema.schemata;")
    return(schemas[, 1])
  }
}

getCatalogs <- function(connection) {
  UseMethod("getCatalogs", connection)
}

#' @exportS3Method
getCatalogs.default <- function(connection) {
  metaData <- rJava::.jcall(connection@jConnection, "Ljava/sql/DatabaseMetaData;", "getMetaData")
  resultSet <- rJava::.jcall(metaData, "Ljava/sql/ResultSet;", "getCatalogs")
  on.exit(rJava::.jcall(resultSet, "V", "close"))
  schemas <- character()
  catalogs <- character()
  while (rJava::.jcall(resultSet, "Z", "next")) {
    catalogs <- c(catalogs, rJava::.jcall(resultSet, "S", "getString", "TABLE_CAT"))
  }
  return(catalogs)
}

#' @exportS3Method
getCatalogs.DatabaseConnectorDbiConnection <- function(connection) {
  if (connection@dbms == "duckdb") {
    sql <- "
    SELECT DISTINCT catalog_name 
    FROM information_schema.schemata
    WHERE catalog_name NOT IN ('system', 'temp');"
    return(dbGetQuery(connection, sql)$catalog_name)
  } else {
    return(DBI::dbGetInfo(connection@dbiConnection)$dbname)
  }
}
