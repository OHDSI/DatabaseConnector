# @file DBI.R
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


# Connection classes------------------------------------------------------------

#' DatabaseConnectorConnection class.
#'
#' @keywords internal
#' @export
#' @import DBI
setClass("DatabaseConnectorConnection",
  contains = "DBIConnection",
  slots = list(
    identifierQuote = "character",
    stringQuote = "character", 
    dbms = "character", 
    uuid = "character"
  )
)

## Internal driver connection classes ------------------------------------------

#' DatabaseConnectorJdbcConnection class.
#'
#' @keywords internal
#' @export
#' @import rJava
setClass("DatabaseConnectorJdbcConnection",
  contains = "DatabaseConnectorConnection",
  slots = list(jConnection = "jobjRef")
)

#' @keywords internal
#' @export
setClass("Oracle", contains = "DatabaseConnectorJdbcConnection")

#' @keywords internal
#' @export
setClass("PostgreSQL", contains = "DatabaseConnectorJdbcConnection")

#' @keywords internal
#' @export
setClass("Redshift", contains = "DatabaseConnectorJdbcConnection")

#' @keywords internal
#' @export
setClass("Microsoft SQL Server", contains = "DatabaseConnectorJdbcConnection")

#' @keywords internal
#' @export
setClass("PDW", contains = "DatabaseConnectorJdbcConnection")

#' @keywords internal
#' @export
setClass("Netezza", contains = "DatabaseConnectorJdbcConnection")

#' @keywords internal
#' @export
setClass("BigQuery", contains = "DatabaseConnectorJdbcConnection")

#' @keywords internal
#' @export
setClass("Spark", contains = "DatabaseConnectorJdbcConnection")

## External driver connection classes ------------------------------------------

#' DatabaseConnectorRSQLiteConnection class.
#'
#' RSQLite connections using databaseConnector are different from other dbms
#' in that DatabaseConnector does not provider the driver component for RSQLite.
#' Instead it uses the driver from the RSQLite package. DatabaseConnector defines a
#' new class that inherits from both DatabaseConnectorConnection and RSQLite::SQLiteConnection
#' which allows DatabaseConnector to add new slots and methods for SQLite connections while still 
#' using the methods defined for SQLiteConnection objects in the RSQLite and dbplyr packages.
#' 
#' @keywords internal
#' @export
#' @import DBI
setClass("DatabaseConnectorRSQLiteConnection",
  contains = c("DatabaseConnectorConnection", "SQLiteConnection"),
  slots = list(
    # TODO use the dbname slot from  RSQLiteConnection instead of a new server slot.
    server = "character"
  )
)


# setMethod("dbConnect", "OracleDriver", function(drv, ) {
#   jarPath <- findPathToJar("^ojdbc.*\\.jar$", pathToDriver)
#   driver <- getJbcDriverSingleton("oracle.jdbc.driver.OracleDriver", jarPath)
#   if (missing(connectionString) || is.null(connectionString)) {
#     # Build connection string from parts
#     if (oracleDriver == "thin") {
#       inform("- using THIN to connect")
#       if (missing(port) || is.null(port)) {
#         port <- "1521"
#       }
#       host <- "127.0.0.1"
#       sid <- server
#       if (grepl("/", server)) {
#         parts <- unlist(strsplit(server, "/"))
#         host <- parts[1]
#         sid <- parts[2]
#       }
#       connectionString <- paste0("jdbc:oracle:thin:@", host, ":", port, ":", sid)
#       if (!missing(extraSettings) && !is.null(extraSettings)) {
#         connectionString <- paste0(connectionString, extraSettings)
#       }
#       result <- class(try(connection <- connectUsingJdbcDriver(driver,
#                                                                connectionString,
#                                                                user = user,
#                                                                password = password,
#                                                                oracle.jdbc.mapDateToTimestamp = "false",
#                                                                dbms = dbms
#       ), silent = FALSE))[1]
# 
#       # Try using TNSName instead:
#       if (result == "try-error") {
#         inform("- Trying using TNSName")
#         connectionString <- paste0("jdbc:oracle:thin:@", server)
#         connection <- connectUsingJdbcDriver(driver,
#                                              connectionString,
#                                              user = user,
#                                              password = password,
#                                              oracle.jdbc.mapDateToTimestamp = "false",
#                                              dbms = dbms
#         )
#       }
#     }
#     if (oracleDriver == "oci") {
#       inform("- using OCI to connect")
#       connectionString <- paste0("jdbc:oracle:oci8:@", server)
#       connection <- connectUsingJdbcDriver(driver,
#                                            connectionString,
#                                            user = user,
#                                            password = password,
#                                            oracle.jdbc.mapDateToTimestamp = "false",
#                                            dbms = dbms
#       )
#     }
#   } else {
#     # User has provided the connection string:
#     if (missing(user) || is.null(user)) {
#       connection <- connectUsingJdbcDriver(driver,
#                                            connectionString,
#                                            oracle.jdbc.mapDateToTimestamp = "false",
#                                            dbms = dbms
#       )
#     } else {
#       connection <- connectUsingJdbcDriver(driver,
#                                            connectionString,
#                                            user = user,
#                                            password = password,
#                                            oracle.jdbc.mapDateToTimestamp = "false",
#                                            dbms = dbms
#       )
#     }
#   }
#   attr(connection, "dbms") <- dbms
#   return(connection)
# })


#' @exportMethod dbCanConnect
NULL

#' @exportMethod dbIsReadOnly
NULL

#' @inherit
#' methods::show title description params details references return seealso
#' @export
setMethod("show", "DatabaseConnectorConnection", function(object) {
  cat("<DatabaseConnectorConnection>", getServer(object))
})

#' @inherit
#' DBI::dbIsValid title description params details references return seealso
#' @export
setMethod("dbIsValid", "DatabaseConnectorJdbcConnection", function(dbObj, ...) {
  return(!rJava::is.jnull(dbObj@jConnection))
})

#' @inherit
#' DBI::dbIsValid title description params details references return seealso
#' @export
setMethod("dbIsValid", "DatabaseConnectorDbiConnection", function(dbObj, ...) {
  return(DBI::dbIsValid(dbObj@dbiConnection))
})

#' @inherit
#' DBI::dbQuoteIdentifier title description params details references return seealso
#' @export
setMethod("dbQuoteIdentifier", signature("DatabaseConnectorConnection", "character"), function(conn, x, ...) {
  if (length(x) == 0L) {
    return(DBI::SQL(character()))
  }
  if (any(is.na(x))) {
    abort("Cannot pass NA to dbQuoteIdentifier()")
  }
  if (nzchar(conn@identifierQuote)) {
    x <- gsub(conn@identifierQuote, paste0(
      conn@identifierQuote,
      conn@identifierQuote
    ), x, fixed = TRUE)
  }
  return(DBI::SQL(paste0(conn@identifierQuote, encodeString(x), conn@identifierQuote)))
})

#' @inherit
#' DBI::dbQuoteString title description params details references return seealso
#' @export
setMethod(
  "dbQuoteString",
  signature("DatabaseConnectorConnection", "character"),
  function(conn, x, ...) {
    if (length(x) == 0L) {
      return(DBI::SQL(character()))
    }
    if (any(is.na(x))) {
      abort("Cannot pass NA to dbQuoteString()")
    }
    if (nzchar(conn@stringQuote)) {
      x <- gsub(conn@stringQuote, paste0(conn@stringQuote, conn@stringQuote), x, fixed = TRUE)
    }
    return(DBI::SQL(paste0(conn@stringQuote, encodeString(x), conn@stringQuote)))
  }
)

# Results -----------------------------------------------------------------------------------------

#' DatabaseConnector results class.
#'
#' @keywords internal
#' @import rJava
#' @export
setClass("DatabaseConnectorResult",
  contains = "DBIResult",
  slots = list(
    content = "jobjRef", type = "character",
    statement = "character"
  )
)

#' @inherit
#' DBI::dbSendQuery title description params details references return seealso
#' @export
setMethod(
  "dbSendQuery",
  signature("DatabaseConnectorJdbcConnection", "character"),
  function(conn, statement,
           ...) {
    if (rJava::is.jnull(conn@jConnection)) {
      abort("Connection is closed")
    }
    batchedQuery <- rJava::.jnew(
      "org.ohdsi.databaseConnector.BatchedQuery",
      conn@jConnection,
      statement,
      conn@dbms
    )
    result <- new("DatabaseConnectorResult",
      content = batchedQuery,
      type = "batchedQuery",
      statement = statement
    )
    return(result)
  }
)

#' @inherit
#' DBI::dbSendQuery title description params details references return seealso
#' @export
setMethod(
  "dbSendQuery",
  signature("DatabaseConnectorDbiConnection", "character"),
  function(conn, statement,
           ...) {
    return(DBI::dbSendQuery(conn@dbiConnection, statement, ...))
  }
)

#' @inherit
#' DBI::dbHasCompleted title description params details references return seealso
#' @export
setMethod("dbHasCompleted", "DatabaseConnectorResult", function(res, ...) {
  if (res@type == "rowsAffected") {
    return(TRUE)
  } else {
    return(rJava::.jcall(res@content, "Z", "isDone"))
  }
})

#' @inherit
#' DBI::dbColumnInfo title description params details references return seealso
#' @export
setMethod("dbColumnInfo", "DatabaseConnectorResult", function(res, ...) {
  columnTypeIds <- rJava::.jcall(res@content, "[I", "getColumnTypes")
  columnTypes <- rep("", length(columnTypeIds))
  columnTypes[columnTypeIds == 1] <- "numeric"
  columnTypes[columnTypeIds == 2] <- "Character"
  columnTypes[columnTypeIds == 3] <- "Date"
  columnNames <- rJava::.jcall(res@content, "[Ljava/lang/String;", "getColumnNames")
  columnSqlTypes <- rJava::.jcall(res@content, "[Ljava/lang/String;", "getColumnSqlTypes")
  return(data.frame(name = columnNames, field.type = columnSqlTypes, data.type = columnTypes))
})

#' @inherit
#' DBI::dbGetStatement title description params details references return seealso
#' @export
setMethod("dbGetStatement", "DatabaseConnectorResult", function(res, ...) {
  return(res@statement)
})

#' @inherit
#' DBI::dbGetRowCount title description params details references return seealso
#' @export
setMethod("dbGetRowCount", "DatabaseConnectorResult", function(res, ...) {
  return(rJava::.jcall(res@content, "I", "getTotalRowCount"))
})

#' @inherit
#' DBI::dbFetch title description params details references return seealso
#' @export
setMethod("dbFetch", "DatabaseConnectorResult", function(res, ...) {
  rJava::.jcall(res@content, "V", "fetchBatch")
  columns <- parseJdbcColumnData(res@content, ...)
  return(columns)
})

#' @inherit
#' DBI::dbClearResult title description params details references return seealso
#' @export
setMethod("dbClearResult", "DatabaseConnectorResult", function(res, ...) {
  if (res@type == "batchedQuery") {
    rJava::.jcall(res@content, "V", "clear")
  }
  return(TRUE)
})

#' @inherit
#' DBI::dbGetQuery title description params details references return seealso
#' @export
setMethod(
  "dbGetQuery",
  signature("DatabaseConnectorConnection", "character"),
  function(conn, statement,
           ...) {
    lowLevelQuerySql(conn, statement)
  }
)

#' @inherit
#' DBI::dbSendStatement title description params details references return seealso
#' @export
setMethod(
  "dbSendStatement",
  signature("DatabaseConnectorConnection", "character"),
  function(conn, statement,
           ...) {
    rowsAffected <- lowLevelExecuteSql(connection = conn, sql = statement)
    rowsAffected <- rJava::.jnew("java/lang/Integer", as.integer(rowsAffected))
    result <- new("DatabaseConnectorResult",
      content = rowsAffected,
      type = "rowsAffected",
      statement = statement
    )
  }
)

#' @inherit
#' DBI::dbGetRowsAffected title description params details references return seealso
#' @export
setMethod("dbGetRowsAffected", "DatabaseConnectorResult", function(res, ...) {
  if (res@type != "rowsAffected") {
    abort("Object not result of dbSendStatement")
  }
  return(rJava::.jsimplify(res@content))
})

#' @inherit
#' DBI::dbExecute title description params details references return seealso
#' @export
setMethod(
  "dbExecute",
  signature("DatabaseConnectorConnection", "character"),
  function(conn, statement,
           ...) {
    rowsAffected <- lowLevelExecuteSql(connection = conn, sql = statement)
    return(rowsAffected)
  }
)

# Misc ----------------------------------------------------------------------

#' @inherit
#' DBI::dbListFields title description params details references return seealso
#' @param database   Name of the database.
#' @param schema     Name of the schema.
#'
#' @export
setMethod(
  "dbListFields",
  signature("DatabaseConnectorConnection", "character"),
  function(conn, name,
           database = NULL, schema = NULL, ...) {
    columns <- listDatabaseConnectorColumns(
      connection = conn,
      catalog = database,
      schema = schema,
      table = name
    )
    return(columns$name)
  }
)

#' @inherit
#' DBI::dbListTables title description params details references return seealso
#' @param database   Name of the database.
#' @param schema     Name of the schema.
#'
#' @export
setMethod(
  "dbListTables",
  "DatabaseConnectorConnection",
  function(conn, database = NULL, schema = NULL,
           ...) {
    if (is.null(database)) {
      databaseSchema <- schema
    } else {
      databaseSchema <- paste(database, schema, sep = ".")
    }
    return(getTableNames(conn, databaseSchema))
  }
)

#' @inherit
#' DBI::dbExistsTable title description params details references return seealso
#' @param database   Name of the database.
#' @param schema     Name of the schema.
#' @export
setMethod(
  "dbExistsTable",
  signature("DatabaseConnectorConnection", "character"),
  function(conn, name,
           database = NULL, schema = NULL, ...) {
    if (length(name) != 1) {
      abort("Name should be a single string")
    }
    if (is.null(database)) {
      databaseSchema <- schema
    } else {
      databaseSchema <- paste(database, schema, sep = ".")
    }
    return(existsTable(connection = conn,
                       databaseSchema = databaseSchema, 
                       tableName = name))
  }
)

#' @inherit
#' DBI::dbWriteTable title description params details references return seealso
#' @param overwrite          Overwrite an existing table (if exists)?
#' @param append             Append to existing table?
#' @param temporary          Should the table created as a temp table?
#' @template TempEmulationSchema
#
#' @export
setMethod(
  "dbWriteTable",
  signature("DatabaseConnectorConnection", "character", "data.frame"),
  function(conn,
           name, value, overwrite = FALSE, append = FALSE, temporary = FALSE, oracleTempSchema = NULL, tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"), ...) {
    if (!is.null(oracleTempSchema) && oracleTempSchema != "") {
      warn("The 'oracleTempSchema' argument is deprecated. Use 'tempEmulationSchema' instead.",
           .frequency = "regularly",
           .frequency_id = "oracleTempSchema"
      )
      tempEmulationSchema <- oracleTempSchema
    }
    if (overwrite) {
      append <- FALSE
    }
    insertTable(
      connection = conn,
      tableName = name,
      data = value,
      dropTableIfExists = overwrite,
      createTable = !append,
      tempTable = temporary, 
      tempEmulationSchema = tempEmulationSchema
    )
    invisible(TRUE)
  }
)

#' @inherit
#' DBI::dbAppendTable title description params details references return seealso
#' @param temporary          Should the table created as a temp table?
#' @template TempEmulationSchema
#
#' @export
setMethod(
  "dbAppendTable",
  signature("DatabaseConnectorConnection", "character", "data.frame"),
  function(conn,
           name, value, temporary = FALSE, oracleTempSchema = NULL, tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"), ..., row.names = NULL) {
    if (!is.null(oracleTempSchema) && oracleTempSchema != "") {
      warn("The 'oracleTempSchema' argument is deprecated. Use 'tempEmulationSchema' instead.",
           .frequency = "regularly",
           .frequency_id = "oracleTempSchema"
      )
      tempEmulationSchema <- oracleTempSchema
    }
    insertTable(
      connection = conn,
      tableName = name,
      data = value,
      dropTableIfExists = FALSE,
      createTable = FALSE,
      tempTable = temporary, 
      tempEmulationSchema = tempEmulationSchema
    )
    invisible(TRUE)
  }
)

#' @inherit
#' DBI::dbCreateTable title description params details references return seealso
#' @param temporary          Should the table created as a temp table?
#' @template TempEmulationSchema
#
#' @export
setMethod(
  "dbCreateTable",
  signature("DatabaseConnectorConnection", "character", "data.frame"),
  function(conn,
           name, fields, oracleTempSchema = NULL, tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"), ..., row.names = NULL, temporary = FALSE) {
    if (!is.null(oracleTempSchema) && oracleTempSchema != "") {
      warn("The 'oracleTempSchema' argument is deprecated. Use 'tempEmulationSchema' instead.",
           .frequency = "regularly",
           .frequency_id = "oracleTempSchema"
      )
      tempEmulationSchema <- oracleTempSchema
    }
    insertTable(
      connection = conn, tableName = name, data = fields[FALSE, ], dropTableIfExists = TRUE,
      createTable = TRUE, tempTable = temporary, tempEmulationSchema = tempEmulationSchema
    )
    invisible(TRUE)
  }
)

#' @inherit
#' DBI::dbReadTable title description params details references return seealso
#' @param database              Name of the database.
#' @param schema                Name of the schema.
#' @template TempEmulationSchema
#'
#' @export
setMethod("dbReadTable", signature("DatabaseConnectorConnection", "character"), function(conn, 
                                                                                         name,
                                                                                         database = NULL, 
                                                                                         schema = NULL, 
                                                                                         oracleTempSchema = NULL, 
                                                                                         tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                                                                         ...) {
  if (!is.null(oracleTempSchema) && oracleTempSchema != "") {
    warn("The 'oracleTempSchema' argument is deprecated. Use 'tempEmulationSchema' instead.",
      .frequency = "regularly",
      .frequency_id = "oracleTempSchema"
    )
    tempEmulationSchema <- oracleTempSchema
  }
  if (!is.null(schema)) {
    name <- paste(schema, name, sep = ".")
  }
  if (!is.null(database)) {
    name <- paste(database, name, sep = ".")
  }
  sql <- "SELECT * FROM @table;"
  sql <- SqlRender::render(sql = sql, table = name)
  sql <- SqlRender::translate(
    sql = sql,
    targetDialect = conn@dbms,
    tempEmulationSchema = tempEmulationSchema
  )
  return(lowLevelQuerySql(conn, sql))
})

#' @inherit
#' DBI::dbRemoveTable title description params details references return seealso
#' @param database           Name of the database.
#' @param schema             Name of the schema.
#' @template TempEmulationSchema
#'
#' @export
setMethod(
  "dbRemoveTable",
  signature("DatabaseConnectorConnection", "character"),
  function(conn, name,
           database = NULL, schema = NULL, oracleTempSchema = NULL, tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"), ...) {
    if (!is.null(oracleTempSchema) && oracleTempSchema != "") {
      warn("The 'oracleTempSchema' argument is deprecated. Use 'tempEmulationSchema' instead.",
           .frequency = "regularly",
           .frequency_id = "oracleTempSchema"
      )
      tempEmulationSchema <- oracleTempSchema
    }
    if (!is.null(schema)) {
      name <- paste(schema, name, sep = ".")
    }
    if (!is.null(database)) {
      name <- paste(database, name, sep = ".")
    }
    sql <- "TRUNCATE TABLE @table; DROP TABLE @table;"
    sql <- SqlRender::render(sql = sql, table = name)
    sql <- SqlRender::translate(
      sql = sql,
      targetDialect = conn@dbms,
      tempEmulationSchema = tempEmulationSchema
    )
    for (statement in SqlRender::splitSql(sql)) {
      lowLevelExecuteSql(conn, statement)
    }
    return(TRUE)
  }
)

#' @importFrom dbplyr dbplyr_edition
#' @export
dbplyr_edition.DatabaseConnectorJdbcConnection <- function(con) 2L
