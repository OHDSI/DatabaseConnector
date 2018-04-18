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


registerWithRStudio <- function(connection, 
                                dbms,
                                server,
                                connectionString) {
  observer <- getOption("connectionObserver")
  if (!is.null(observer)) {
    server <- getServer(connection)
    observer$connectionOpened(type = dbms,
                              displayName = server,
                              host = server,
                              connectCode = "NA",
                              disconnect = function() {
                                disconnect(connection)
                              },
                              listObjectTypes = function () {
                                listDatabaseConnectorObjectTypes()
                              },
                              listObjects = function(...) {
                                listDatabaseConnectorObjects(connection, ...)
                              },
                              listColumns = function(...) {
                                dbListFields(connection, ...)
                              },
                              previewObject = function(rowLimit, ...) {
                                previewObject(connection, rowLimit, ...)
                              },
                              actions = connectionActions(connection),
                              connectionObject = connection
    )
  }
}

unregisterWithRStudio <- function(connection) {
  observer <- getOption("connectionObserver")
  if (!is.null(observer)) {
    observer$connectionClosed(connection@dbms, getServer(connection))
  } 
}

#' @export
listDatabaseConnectorObjects <- function(connection, databaseSchema = NULL) {
  # print("Check")
  # print(names(list(...)))
  if (is.null(databaseSchema)) {
    databaseSchemas <- c("ohdsi", "public")
    return(
      data.frame(
        name = databaseSchemas,
        type = rep("databaseSchema", times = length(databaseSchemas)),
        stringsAsFactors = FALSE
      ))
  } else {
    tables <- getTableNames(connection, databaseSchema)
    return(
      data.frame(
        name = tables,
        type = rep("table", times = length(tables)),
        stringsAsFactors = FALSE
      ))
  }
}

#' @export
listDatabaseConnectorObjectTypes <- function(connection) {
  return(list(
    databaseSchema = list(
      contains = list(
        table = list(
          contains = "data")))))
  
  
  # return(list(databaseSchema = list(contains = list(table = list(contains = "data")))))
}

#' @export
previewObject <- function(connection, rowLimit, table = NULL, databaseSchema = NULL) {
  if (!is.null(databaseSchema)) {
    name <- paste(databaseSchema, table, sep = ".")
  }
  
  dbGetQuery(connection, paste("SELECT * FROM", name, "LIMIT 10"), n = rowLimit)
}

#' @export
connectionActions <- function(connection) {
  list(
    Help = list(
      # show README for this package as the help; we will update to a more
      # helpful (and/or more driver-specific) website once one exists
      icon = "",
      callback = function() {
        utils::browseURL("https://github.com/rstats-db/odbc/blob/master/README.md")
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