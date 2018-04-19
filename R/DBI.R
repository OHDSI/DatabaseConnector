# @file DBI.R
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

# Driver -----------------------------------------------------------------------------------------

#' DatabaseConnectorDriver class.
#' 
#' @keywords internal
#' @export
#' @import DBI
#' @import methods
setClass("DatabaseConnectorDriver", contains = "DBIDriver")

#' @export
#' @rdname DatabaseConnectorDriver-class
setMethod("dbUnloadDriver", "DatabaseConnectorDriver", function(drv, ...) {
  TRUE
})

setMethod("show", "DatabaseConnectorDriver", function(object) {
  cat("<DatabaseConnectorDriver>\n")
})

#' Create a DatabaseConnectorDriver object
#' 
#' @export
DatabaseConnectorDriver <- function() {
  new("DatabaseConnectorDriver")
}


# Connection  -----------------------------------------------------------------------------------------

#' DatabaseConnectorConnection class.
#' 
#' @export
#' @keywords internal
setClass("DatabaseConnectorConnection", 
         contains = "DBIConnection", 
         slots = list(
           jConnection = "jobjRef",
           identifierQuote = "character",
           dbms = "character"
         )
)

#' @param drv An object created by \code{DatabaseConnectorDriver()} 
#' @rdname DatabaseConnectorDriver
#' @export
setMethod("dbConnect", "DatabaseConnectorDriver", function(drv, ...) {
   return(connect(...))
})

#' @param conn An object created by \code{connect()} 
#' @rdname DatabaseConnectorDriver
#' @export
setMethod("dbDisconnect", "DatabaseConnectorConnection", function(conn) {
  disconnect(conn)  
})

#' @param object An object created by \code{connect()} 
#' @rdname DatabaseConnectorDriver
#' @export
setMethod("show", "DatabaseConnectorConnection", function(object) {
  cat("<DatabaseConnectorConnection>", getServer(object))
})

# Results -----------------------------------------------------------------------------------------

#' DatabaseConnector results class.
#' 
#' @keywords internal
#' @export
setClass("DatabaseConnectorResult", 
         contains = "DBIResult",
         slots = list(ptr = "externalptr")
)

#' Send a query to DatabaseConnector
#' 
#' @export
setMethod("dbSendQuery", "DatabaseConnectorConnection", function(conn, statement, ...) {
  querySql(conn, statement)
})

#' Send a query to DatabaseConnector
#' 
#' @export
setMethod("dbGetQuery", "DatabaseConnectorConnection", function(conn, statement, ...) {
  querySql(conn, statement)
})

#' @export
setMethod("dbClearResult", "DatabaseConnectorResult", function(res, ...) {
  return(TRUE)
})

#' Retrieve records from Kazam query
#' @export
setMethod("dbFetch", "DatabaseConnectorResult", function(res, n = -1, ...) {
  return(res)
})

#' @export
setMethod("dbHasCompleted", "DatabaseConnectorResult", function(res, ...) { 
  return(TRUE)
})

#' @export
setMethod("dbListFields", "DatabaseConnectorConnection", def=function(conn, name) {
  metaData <- .jcall(conn@jConnection, "Ljava/sql/DatabaseMetaData;", "getMetaData")
  resultSet <- .jcall(metaData, 
                      "Ljava/sql/ResultSet;", 
                      "getColumns", 
                      .jnull("java/lang/String"),
                      .jnull("java/lang/String"), 
                      name, 
                      .jnull("java/lang/String"))
  on.exit(.jcall(resultSet, "V", "close"))
  fields <- character()
  while (.jcall(resultSet, "Z", "next"))
    fields <- c(fields, .jcall(resultSet, "S", "getString", "COLUMN_NAME"))
  return(fields)
})

#' @export
setMethod("dbListTables", "DatabaseConnectorConnection", def=function(conn, catalog = NULL, schema = NULL, ...) {
  if (is.null(catalog)) {
    databaseSchema <- schema
  } else {
    databaseSchema <- paste(catalog, schema, sep = ".")
  }
  return(getTableNames(conn, databaseSchema))
})


