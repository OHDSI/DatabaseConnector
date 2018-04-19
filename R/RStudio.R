# @file RStudio.R
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


registerWithRStudio <- function(connection) {
  observer <- getOption("connectionObserver")
  if (!is.null(observer)) {
    server <- getServer(connection)
    observer$connectionOpened(type = compileTypeLabel(connection),
                              displayName = server,
                              host = server,
                              connectCode = compileReconnectCode(connection),
                              icon = "",
                              disconnect = function() {
                                disconnect(connection)
                              },
                              listObjectTypes = function () {
                                listDatabaseConnectorObjectTypes(connection)
                              },
                              listObjects = function(...) {
                                listDatabaseConnectorObjects(connection, ...)
                              },
                              listColumns = function(...) {
                                listDatabaseConnectorColumns(connection, ...)
                              },
                              previewObject = function(rowLimit, ...) {
                                previewObject(connection, rowLimit, ...)
                              },
                              actions = connectionActions(connection),
                              connectionObject = connection
    )
  }
}

compileTypeLabel <- function(connection) {
  return(paste0("DatabaseConnector (", connection@dbms, ")"))
}

unregisterWithRStudio <- function(connection) {
  observer <- getOption("connectionObserver")
  if (!is.null(observer)) {
    observer$connectionClosed(compileTypeLabel(connection), getServer(connection))
  } 
}

hasCatalogs <- function(connection) {
  return (connection@dbms %in% c("pdw", "sql server", "postgresql", "redshift"))
}

listDatabaseConnectorColumns <- function(connection, catalog = NULL, schema = NULL, table = NULL, ...) {
  if (is.null(catalog))
    catalog <- .jnull("java/lang/String")
  if (is.null(schema))
    schema <- .jnull("java/lang/String")
  if (connection@dbms == "oracle") {
    table <- toupper(table)
  } else {
    table <- tolower(table)
  }
  metaData <- .jcall(connection@jConnection, "Ljava/sql/DatabaseMetaData;", "getMetaData")
  resultSet <- .jcall(metaData, 
                      "Ljava/sql/ResultSet;", 
                      "getColumns", 
                      catalog,
                      schema, 
                      table, 
                      .jnull("java/lang/String"))
  on.exit(.jcall(resultSet, "V", "close"))
  fields <- character()
  types <- character()
  while (.jcall(resultSet, "Z", "next")) {
    fields <- c(fields, .jcall(resultSet, "S", "getString", "COLUMN_NAME"))
    types <- c(types, .jcall(resultSet, "S", "getString", "TYPE_NAME"))
  }
  return(data.frame(name = fields,
                    type = types,
                    stringsAsFactors = FALSE))
}
  


listDatabaseConnectorObjects <- function(connection, catalog = NULL, schema = NULL, ...) {
  if (is.null(catalog) && hasCatalogs(connection)) {
    catalogs <- getCatalogs(connection)
    return(
      data.frame(
        name = catalogs,
        type = rep("catalog", times = length(catalogs)),
        stringsAsFactors = FALSE
      ))
  } 
  if (is.null(schema)) {
    schemas <- getSchemaNames(connection, catalog)
    return(
      data.frame(
        name = schemas,
        type = rep("schema", times = length(schemas)),
        stringsAsFactors = FALSE
      ))
  } 
  if (!hasCatalogs(connection) || connection@dbms %in% c("postgresql", "redshift")) {
    databaseSchema <- schema
  } else {
    databaseSchema <- paste(catalog, schema, sep = ".")
  }
  tables <- getTableNames(connection, databaseSchema)
  return(
    data.frame(
      name = tables,
      type = rep("table", times = length(tables)),
      stringsAsFactors = FALSE
    ))
  
}

listDatabaseConnectorObjectTypes <- function(connection) {
  types <- list(schema = list(contains = c(list(table = list(contains = "data")), list(view = list(contains = "data")))))
  if (hasCatalogs(connection)) {
    types <-  list(catalog = list(contains = types))
  }
  return(types)
}

previewObject <- function(connection, rowLimit, catalog = NULL, table = NULL, schema = NULL) {
  if (!hasCatalogs(connection)) {
    databaseSchema <- schema
  } else {
    databaseSchema <- paste(catalog, schema, sep = ".")
  }
  sql <- "SELECT TOP 1000 * FROM @databaseSchema.@table;"
  sql <- SqlRender::renderSql(sql = sql,
                              databaseSchema = databaseSchema,
                              table = table)$sql
  sql <- SqlRender::translateSql(sql = sql,
                                 targetDialect = connection@dbms)$sql
  querySql(connection, sql)
}

connectionActions <- function(connection) {
  list(
    Help = list(
      # show README for this package as the help; we will update to a more
      # helpful (and/or more driver-specific) website once one exists
      icon = "",
      callback = function() {
        utils::browseURL("https://github.com/OHDSI/DatabaseConnector/blob/master/README.md")
      }
    )
  )
}

getServer <- function(connection) {
  databaseMetaData <- rJava::.jcall(connection@jConnection, "Ljava/sql/DatabaseMetaData;", "getMetaData")
  url <- rJava::.jcall(databaseMetaData, "Ljava/lang/String;", "getURL")
  server <- urltools::url_parse(url)$domain 
  return(server)
}

compileReconnectCode <- function(connection) {
  databaseMetaData <- rJava::.jcall(connection@jConnection, "Ljava/sql/DatabaseMetaData;", "getMetaData")
  url <- rJava::.jcall(databaseMetaData, "Ljava/lang/String;", "getURL")
  user <- rJava::.jcall(databaseMetaData, "Ljava/lang/String;", "getUserName")
  code <- sprintf("con <- library(DatabaseConnector)\nconnect(dbms = \"%s\", connectionString = \"%s\", user = \"%s\", password = password)", connection@dbms, url, user)
  return(code)
}

getSchemaNames <- function(conn, catalog = NULL) {
  if (is.null(catalog))
    catalog <- .jnull("java/lang/String")
  metaData <- .jcall(conn@jConnection, "Ljava/sql/DatabaseMetaData;", "getMetaData")
  resultSet <- .jcall(metaData, 
                      "Ljava/sql/ResultSet;", 
                      "getSchemas",
                      catalog,
                      .jnull("java/lang/String"))
  on.exit(.jcall(resultSet, "V", "close"))
  schemas <- character()
  while (.jcall(resultSet, "Z", "next")) {
    thisCatalog <- .jcall(resultSet, "S", "getString", "TABLE_CATALOG")
    if (is.null(thisCatalog) || (!is.null(catalog) && thisCatalog == catalog)) {
      schemas <- c(schemas, .jcall(resultSet, "S", "getString", "TABLE_SCHEM"))
    }
  }
  return(schemas)
}

getCatalogs <- function(conn) {
  metaData <- .jcall(conn@jConnection, "Ljava/sql/DatabaseMetaData;", "getMetaData")
  resultSet <- .jcall(metaData, 
                      "Ljava/sql/ResultSet;", 
                      "getCatalogs")
  on.exit(.jcall(resultSet, "V", "close"))
  schemas <- character()
  catalogs <- character()
  while (.jcall(resultSet, "Z", "next")) {
    catalogs <- c(catalogs, .jcall(resultSet, "S", "getString", "TABLE_CAT"))
  }
  return(catalogs)
}