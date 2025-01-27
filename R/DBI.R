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

# Driver -----------------------------------------------------------------------------------------

#' DatabaseConnectorDriver class.
#'
#' @keywords internal
#' @export
#' @import DBI
#' @import methods
setClass("DatabaseConnectorDriver", contains = "DBIDriver")

#' @inherit
#' DBI::dbUnloadDriver title description params details references return seealso
#' @export
setMethod("dbUnloadDriver", "DatabaseConnectorDriver", function(drv, ...) {
  TRUE
})

setMethod("show", "DatabaseConnectorDriver", function(object) {
  cat("<DatabaseConnectorDriver>\n")
})

#' @inherit DBI::dbGetInfo title description params details references return seealso 
#' 
#' @export
setMethod("dbGetInfo", "DatabaseConnectorDriver", function(dbObj, ...) {
  return(list(
    driver.version = utils::packageVersion(utils::packageName()),
    client.version = 1,
    max.connections = 999
  ))
})

#' Create a DatabaseConnectorDriver object
#'
#' @export
DatabaseConnectorDriver <- function() {
  new("DatabaseConnectorDriver")
}


# Connection
# -----------------------------------------------------------------------------------------

# Borrowed from the odbc package:
class_cache <- new.env(parent = emptyenv())

# Simple class prototype to avoid messages about unknown classes from setMethod
setClass("Microsoft SQL Server", where = class_cache)

setClass("DatabaseConnectorConnection", where = class_cache)

setClass("DatabaseConnectorJdbcConnection", where = class_cache)

setClass("DatabaseConnectorDbiConnection", where = class_cache)

#' Create a connection to a DBMS
#'
#' @description
#' Connect to a database. This function is synonymous with the [connect()] function. except
#' a dummy driver needs to be specified
#'
#' @param drv   The result of the [DatabaseConnectorDriver()] function
#' @param ...   Other parameters. These are the same as expected by the [connect()] function.
#'
#' @return
#' Returns a DatabaseConnectorConnection object that can be used with most of the other functions in
#' this package.
#'
#' @examples
#' \dontrun{
#' conn <- dbConnect(DatabaseConnectorDriver(),
#'   dbms = "postgresql",
#'   server = "localhost/ohdsi",
#'   user = "joe",
#'   password = "secret"
#' )
#' querySql(conn, "SELECT * FROM cdm_synpuf.person;")
#' dbDisconnect(conn)
#' }
#'
#' @export
setMethod("dbConnect", "DatabaseConnectorDriver", function(drv, ...) {
  return(connect(...))
})

#' @exportMethod dbCanConnect
NULL

#' @exportMethod dbIsReadOnly
NULL

#' @inherit DBI::dbDisconnect title description params details references return seealso
#' 
#' @export
setMethod("dbDisconnect", "DatabaseConnectorConnection", function(conn) {
  disconnect(conn)
})

setMethod("show", "DatabaseConnectorConnection", function(object) {
  cat("<DatabaseConnectorConnection>", getServer(object))
})

#' @inherit DBI::dbIsValid title description params details references return seealso
#' 
#' @export
setMethod("dbIsValid", "DatabaseConnectorJdbcConnection", function(dbObj, ...) {
  return(!rJava::is.jnull(dbObj@jConnection))
})

#' @inherit DBI::dbIsValid title description params details references return seealso
#' 
#' @export
setMethod("dbIsValid", "DatabaseConnectorDbiConnection", function(dbObj, ...) {
  return(DBI::dbIsValid(dbObj@dbiConnection))
})

#' @inherit DBI::dbGetInfo title description params details references return seealso
#' 
#' @export
setMethod("dbGetInfo", "DatabaseConnectorConnection", function(dbObj, ...) {
  return(list(db.version = "15.0"))
})

setMethod("dbQuoteIdentifier", signature("DatabaseConnectorConnection", "character"), function(conn,
                                                                                               x, ...) {
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

#' DatabaseConnector JDBC results class.
#'
#' @keywords internal
#' @import rJava
#' @export
setClass("DatabaseConnectorJdbcResult",
         contains = "DBIResult",
         slots = list(
           content = "jobjRef", 
           type = "character",
           statement = "character",
           dbms = "character"
         )
)

#' DatabaseConnector DBI results class.
#'
#' @keywords internal
#' @export
setClass("DatabaseConnectorDbiResult",
         contains = "DBIResult",
         slots = list(
           resultSet = "DBIResult",
           dbms = "character"
         )
)

#' @inherit DBI::dbSendQuery title description params details references return seealso
#' 
#' @param translate Translate the query using SqlRender?
#' 
#' @export
setMethod(
  "dbSendQuery",
  signature("DatabaseConnectorJdbcConnection", "character"),
  function(conn, statement, translate = TRUE, ...) {
    if (rJava::is.jnull(conn@jConnection)) {
      abort("Connection is closed")
    }
    logTrace(paste("Sending SQL:", truncateSql(statement)))
    startTime <- Sys.time()
    
    dbms <- dbms(conn)
    if (translate) {
      statement <- translateStatement(
        sql = statement,
        targetDialect = dbms
      )
    }
    # For Oracle, remove trailing semicolon:
    statement <- gsub(";\\s*$", "", statement)
    tryCatch(
      batchedQuery <- rJava::.jnew(
        "org.ohdsi.databaseConnector.BatchedQuery",
        conn@jConnection,
        statement,
        dbms
      ),
      error = function(error) {
        # Rethrowing error to avoid 'no field, method or inner class called 'use_cli_format''
        # error by rlang (see https://github.com/OHDSI/DatabaseConnector/issues/235)
        rlang::abort(error$message)
      }
    )
    
    result <- new("DatabaseConnectorJdbcResult",
                  content = batchedQuery,
                  type = "batchedQuery",
                  statement = statement,
                  dbms = dbms
    )
    delta <- Sys.time() - startTime
    logTrace(paste("Querying SQL took", delta, attr(delta, "units")))
    return(result)
  }
)

#' @inherit DBI::dbSendQuery title description params details references return seealso
#' 
#' @param translate Translate the query using SqlRender?
#' 
#' @export
setMethod(
  "dbSendQuery",
  signature("DatabaseConnectorDbiConnection", "character"),
  function(conn, statement, translate = TRUE, ...) {
    if (translate) {
      statement <- translateStatement(
        sql = statement,
        targetDialect = dbms(conn)
      )
    }
    logTrace(paste("Sending SQL:", truncateSql(statement)))
    startTime <- Sys.time()
    
    resultSet <- DBI::dbSendQuery(conn@dbiConnection, statement, ...)
    result <- new("DatabaseConnectorDbiResult",
                  resultSet = resultSet,
                  dbms = dbms(conn)
    )
    
    delta <- Sys.time() - startTime
    logTrace(paste("Querying SQL took", delta, attr(delta, "units")))
    return(result)
  }
)

#' @inherit DBI::dbHasCompleted title description params details references return seealso
#' @export
setMethod("dbHasCompleted", "DatabaseConnectorJdbcResult", function(res, ...) {
  if (res@type == "rowsAffected") {
    return(TRUE)
  } else {
    return(rJava::.jcall(res@content, "Z", "isDone"))
  }
})

#' @inherit DBI::dbHasCompleted title description params details references return seealso
#' @export
setMethod("dbHasCompleted", "DatabaseConnectorDbiResult", function(res, ...) {
  return(DBI::dbHasCompleted(res@resultSet, ...))
})

#' @inherit DBI::dbColumnInfo title description params details references return seealso
#' @export
setMethod("dbColumnInfo", "DatabaseConnectorJdbcResult", function(res, ...) {
  columnTypeIds <- rJava::.jcall(res@content, "[I", "getColumnTypes")
  columnTypes <- rep("", length(columnTypeIds))
  columnTypes[columnTypeIds == 1] <- "numeric"
  columnTypes[columnTypeIds == 2] <- "Character"
  columnTypes[columnTypeIds == 3] <- "Date"
  columnNames <- rJava::.jcall(res@content, "[Ljava/lang/String;", "getColumnNames")
  columnSqlTypes <- rJava::.jcall(res@content, "[Ljava/lang/String;", "getColumnSqlTypes")
  return(data.frame(name = columnNames, field.type = columnSqlTypes, data.type = columnTypes))
})

#' @inherit DBI::dbColumnInfo title description params details references return seealso
#' @export
setMethod("dbColumnInfo", "DatabaseConnectorDbiResult", function(res, ...) {
  return(DBI::dbColumnInfo(res@resultSet, ...))
})

#' @inherit DBI::dbGetStatement title description params details references return seealso
#' @export
setMethod("dbGetStatement", "DatabaseConnectorJdbcResult", function(res, ...) {
  return(res@statement)
})

#' @inherit DBI::dbGetStatement title description params details references return seealso
#' @export
setMethod("dbGetStatement", "DatabaseConnectorDbiResult", function(res, ...) {
  return(DBI::dbGetStatement(res@resultSet, ...))
})

#' @inherit DBI::dbGetRowCount title description params details references return seealso
#' @export
setMethod("dbGetRowCount", "DatabaseConnectorJdbcResult", function(res, ...) {
  return(rJava::.jcall(res@content, "I", "getTotalRowCount"))
})

#' @inherit DBI::dbGetRowCount title description params details references return seealso
#' @export
setMethod("dbGetRowCount", "DatabaseConnectorDbiResult", function(res, ...) {
  return(DBI::dbGetRowCount(res@resultSet, ...))
})

#' @inherit DBI::dbFetch title description params details references return seealso
#' @export
setMethod("dbFetch", "DatabaseConnectorJdbcResult", function(res, n = -1, ...) {
  tryCatch({
    if (n == -1 | is.infinite(n)) {
      columns <- getAllBatches(batchedQuery = res@content,
                               datesAsString = FALSE,
                               integer64AsNumeric = getOption("databaseConnectorInteger64AsNumeric",
                                                              default = TRUE),
                               integerAsNumeric = getOption("databaseConnectorIntegerAsNumeric",
                                                            default = TRUE))
    } else {
      warn("The 'n' argument is set to something other than -1 or Inf, and will be ignored. Fetching as many rows as fits in the Java VM.",
           .frequency = "regularly",
           .frequency_id = "dbFetchN"
      )
      rJava::.jcall(res@content, "V", "fetchBatch")
      columns <- parseJdbcColumnData(batchedQuery = res@content,
                                     datesAsString = FALSE,
                                     integer64AsNumeric = getOption("databaseConnectorInteger64AsNumeric",
                                                                    default = TRUE),
                                     integerAsNumeric = getOption("databaseConnectorIntegerAsNumeric",
                                                                  default = TRUE))
    }
    columns <- convertFields(res@dbms, columns)
    colnames(columns) <- tolower(colnames(columns))
    return(columns)
  },
  error = function(error) {
    # Rethrowing error to avoid 'no field, method or inner class called 'use_cli_format'  
    # error by rlang (see https://github.com/OHDSI/DatabaseConnector/issues/235)
    rlang::abort(error$message)
  })
})

#' @inherit DBI::dbFetch title description params details references return seealso
#' @export
setMethod("dbFetch", "DatabaseConnectorDbiResult", function(res, n = -1, ...) {
  columns <- DBI::dbFetch(res@resultSet, n, ...)
  columns <- convertFields(res@dbms, columns)
  columns <- dbFetchIntegerToNumeric(columns)
  colnames(columns) <- tolower(colnames(columns))
  return(columns)
})

#' @inherit DBI::dbClearResult title description params details references return seealso
#' 
#' @export
setMethod("dbClearResult", "DatabaseConnectorJdbcResult", function(res, ...) {
  if (res@type == "batchedQuery") {
    rJava::.jcall(res@content, "V", "clear")
  }
  return(TRUE)
})

#' @inherit DBI::dbClearResult title description params details references return seealso
#' 
#' @export
setMethod("dbClearResult", "DatabaseConnectorDbiResult", function(res, ...) {
  return(DBI::dbClearResult(res@resultSet, ...))
})

#' @inherit DBI::dbGetRowsAffected title description params details references return seealso
#' 
#' @export
setMethod("dbGetRowsAffected", "DatabaseConnectorJdbcResult", function(res, ...) {
  if (res@type != "rowsAffected") {
    abort("Object not result of dbSendStatement")
  }
  return(rJava::.jsimplify(res@content))
})

#' @inherit DBI::dbGetRowsAffected title description params details references return seealso
#' 
#' @export
setMethod("dbGetRowsAffected", "DatabaseConnectorDbiResult", function(res, ...) {
  return(DBI::dbGetRowsAffected(res@resultSet, ...))
})

#' @inherit DBI::dbGetQuery title description params details references return seealso
#' 
#' @param translate Translate the query using SqlRender?
#' 
#' @export
setMethod(
  "dbGetQuery",
  signature("DatabaseConnectorConnection", "character"),
  function(conn, statement, translate = TRUE, ...) {
    if (translate) {
      statement <- translateStatement(
        sql = statement,
        targetDialect = dbms(conn)
      )
    }
    result <- querySql(conn, statement)
    colnames(result) <- tolower(colnames(result))
    return(result)
  }
)

#' @inherit DBI::dbSendStatement title description params details references return seealso
#' 
#' @param translate Translate the query using SqlRender?
#' 
#' @export
setMethod(
  "dbSendStatement",
  signature("DatabaseConnectorConnection", "character"),
  function(conn, statement, translate = TRUE, ...) {
    if (translate) {
      statement <- translateStatement(
        sql = statement,
        targetDialect = dbms(conn)
      )
    }
    rowsAffected <- executeSql(connection = conn, sql = statement)
    rowsAffected <- rJava::.jnew("java/lang/Double", as.double(sum(rowsAffected)))
    result <- new("DatabaseConnectorJdbcResult",
                  content = rowsAffected,
                  type = "rowsAffected",
                  statement = statement
    )
  }
)

#' @inherit DBI::dbExecute title description params details references return seealso
#' 
#' @param translate Translate the query using SqlRender?
#' 
#' @export
setMethod(
  "dbExecute",
  signature("DatabaseConnectorConnection", "character"),
  function(conn, statement, translate = TRUE, ...) {
    if (isDbplyrSql(statement) && dbms(conn) %in% c("oracle", "bigquery", "spark", "hive") && grepl("^UPDATE STATISTICS", statement)) {
      # These platforms don't support this, so SqlRender translates to an empty string, which causes errors down the line.
      return(0)
    }
    if (translate) {
      statement <- translateStatement(
        sql = statement,
        targetDialect = dbms(conn)
      )
    }
    rowsAffected <- 0
    for (sql in SqlRender::splitSql(statement)) {
      rowsAffected <- rowsAffected + executeSql(conn, sql)
    }
    return(rowsAffected)
  }
)

# Misc ----------------------------------------------------------------------

#' @inherit DBI::dbListFields title description params details references return seealso
#' 
#' @template DatabaseSchema
#'
#' @export
setMethod(
  "dbListFields",
  signature("DatabaseConnectorConnection", "character"),
  function(conn, name,
           databaseSchema = NULL, ...) {
    databaseSchemaSplit <- splitDatabaseSchema(databaseSchema, dbms(conn))
    columns <- listDatabaseConnectorColumns(
      connection = conn,
      catalog = databaseSchemaSplit[[1]],
      schema = databaseSchemaSplit[[2]],
      table = name
    )
    return(tolower(columns$name))
  }
)

#' @inherit DBI::dbExistsTable title description params details references return seealso
#' 
#' @template DatabaseSchema
#' 
#' @export
setMethod(
  "dbExistsTable",
  signature("DatabaseConnectorConnection", "character"),
  function(conn, name,
           databaseSchema = NULL, ...) {
    if (length(name) != 1) {
      abort("Name should be a single string")
    }
    return(existsTable(connection = conn,
                       databaseSchema = databaseSchema, 
                       tableName = name))
  }
)

#' @inherit DBI::dbWriteTable title description params details references return seealso
#' 
#' @template DatabaseSchema
#' @param overwrite          Overwrite an existing table (if exists)?
#' @param append             Append to existing table?
#' @param temporary          Should the table created as a temp table?
#' @template TempEmulationSchema
#
#' @export
setMethod(
  "dbWriteTable",
  "DatabaseConnectorConnection",
  function(conn,
           name, value, databaseSchema = NULL, overwrite = FALSE, append = FALSE, temporary = FALSE, oracleTempSchema = NULL, tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"), ...) {
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
      databaseSchema = databaseSchema,
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

#' @inherit DBI::dbAppendTable title description params details references return seealso
#' 
#' @param temporary          Should the table created as a temp table?
#' @template TempEmulationSchema
#' @template DatabaseSchema
#'
#' @export
setMethod(
  "dbAppendTable",
  signature("DatabaseConnectorConnection", "character"),
  function(conn,
           name, value, databaseSchema = NULL, temporary = FALSE, oracleTempSchema = NULL, tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"), ..., row.names = NULL) {
    if (!is.null(oracleTempSchema) && oracleTempSchema != "") {
      warn("The 'oracleTempSchema' argument is deprecated. Use 'tempEmulationSchema' instead.",
           .frequency = "regularly",
           .frequency_id = "oracleTempSchema"
      )
      tempEmulationSchema <- oracleTempSchema
    }
    insertTable(
      connection = conn,
      databaseSchema = databaseSchema,
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

#' @inherit DBI::dbCreateTable title description params details references return seealso
#' 
#' @param temporary          Should the table created as a temp table?
#' @template TempEmulationSchema
#' @template DatabaseSchema
#' 
#' @export
setMethod(
  "dbCreateTable",
  "DatabaseConnectorConnection",
  function(conn,
           name, fields, databaseSchema = NULL, oracleTempSchema = NULL, tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"), ..., row.names = NULL, temporary = FALSE) {
    if (!is.null(oracleTempSchema) && oracleTempSchema != "") {
      warn("The 'oracleTempSchema' argument is deprecated. Use 'tempEmulationSchema' instead.",
           .frequency = "regularly",
           .frequency_id = "oracleTempSchema"
      )
      tempEmulationSchema <- oracleTempSchema
    }
    insertTable(
      connection = conn, 
      databaseSchema = databaseSchema,
      tableName = name, 
      data = fields[FALSE, ], 
      dropTableIfExists = TRUE,
      createTable = TRUE, 
      tempTable = temporary, 
      tempEmulationSchema = tempEmulationSchema
    )
    invisible(TRUE)
  }
)

#' @inherit DBI::dbReadTable title description params details references return seealso
#' 
#' @template DatabaseSchema
#' @template TempEmulationSchema
#'
#' @export
setMethod("dbReadTable", 
          signature("DatabaseConnectorConnection", "character"), 
          function(conn, 
                   name,
                   databaseSchema = NULL, 
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
            if (!is.null(databaseSchema)) {
              name <- paste(databaseSchema, name, sep = ".")
            }
            sql <- "SELECT * FROM @table;"
            sql <- SqlRender::render(sql = sql, table = name)
            sql <- translateStatement(
              sql = sql,
              targetDialect = dbms(conn),
              tempEmulationSchema = tempEmulationSchema
            )
            return(querySql(conn, sql))
          })

#' @inherit DBI::dbRemoveTable title description params details references return seealso
#' 
#' @template DatabaseSchema
#' @template TempEmulationSchema
#'
#' @export
setMethod(
  "dbRemoveTable",
  "DatabaseConnectorConnection",
  function(conn, name,
           databaseSchema = NULL, oracleTempSchema = NULL, tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"), ...) {
    if (!is.null(oracleTempSchema) && oracleTempSchema != "") {
      warn("The 'oracleTempSchema' argument is deprecated. Use 'tempEmulationSchema' instead.",
           .frequency = "regularly",
           .frequency_id = "oracleTempSchema"
      )
      tempEmulationSchema <- oracleTempSchema
    }
    if (!is.null(databaseSchema)) {
      name <- paste(databaseSchema, name, sep = ".")
    }
    sql <- "TRUNCATE TABLE @table; DROP TABLE @table;"
    sql <- SqlRender::render(sql = sql, table = name)
    sql <- translateStatement(
      sql = sql,
      targetDialect = dbms(conn),
      tempEmulationSchema = tempEmulationSchema
    )
    for (statement in SqlRender::splitSql(sql)) {
      executeSql(conn, statement)
    }
    return(TRUE)
  }
)

setMethod("dbBegin",
          signature(conn="DatabaseConnectorConnection"),
          def = function(conn,  ...){
            # Do nothing
          }
)
setMethod("dbCommit",
          signature(conn="DatabaseConnectorConnection"),
          def = function(conn,  ...){
            # Do nothing
          }
)
setMethod("dbRollback",
          signature(conn="DatabaseConnectorConnection"),
          def = function(conn,  ...){
            # Do nothing
          }
)

#' Refer to a table in a database schema
#' 
#' @description 
#' Can be used with [dplyr::tbl()] to indicate a table in a specific database schema.
#'
#' @template DatabaseSchema
#' @param table          The name of the table in the database schema.
#'
#' @return
#' An object representing the table and database schema.
#' 
#' @export
inDatabaseSchema <- function(databaseSchema, table) {
  databaseSchema <- strsplit(databaseSchema, "\\.")[[1]]
  if (length(databaseSchema) == 1) {
    return(dbplyr::in_schema(databaseSchema[1], table))
  } else {
    return(dbplyr::in_catalog(databaseSchema[1], databaseSchema[2], table))
  }
}

isDbplyrSql <- function(sql) {
  return(is(sql, "sql"))
}

translateStatement <- function(sql, targetDialect, tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")) {
  debug <- isTRUE(getOption("DEBUG_DATABASECONNECTOR_DBPLYR"))
  if (debug) {
    message(paste("SQL in:", sql))
  }
  if (isDbplyrSql(sql)) {
    if (!grepl(";\\s*$", sql)) {
      # SqlRender requires statements to end with semicolon, but dbplyr does not generate these:
      sql <- paste0(sql, ";")
    }
    sql <- translateDateFunctions(sql)
  }
  sql <- SqlRender::translate(sql = sql, targetDialect = targetDialect, tempEmulationSchema = tempEmulationSchema)
  if (debug) {
    message(paste("SQL out:", sql))
  }
  return(sql)
}

splitDatabaseSchema <- function(databaseSchema, dbms) {
  if (is.null(databaseSchema)) {
    return(list(NULL, NULL))
  }
  if (dbms %in% c("sql server", "pdw")) {
    databaseSchemaSplit <- strsplit(databaseSchema, "\\.")[[1]]
    if (length(databaseSchemaSplit) == 1) {
      return(list(NULL, databaseSchema))
    } else if (length(databaseSchemaSplit) == 2) {
      return(list(databaseSchemaSplit[1], databaseSchemaSplit[2]))
    } else {
      rlang::abort("databaseSchema can contain at most one dot (.)")
    }
  } else {
    return(list(NULL, databaseSchema))
  }
}

dbFetchIntegerToNumeric <- function(columns) {
  if (getOption("databaseConnectorIntegerAsNumeric", default = TRUE)) {
    for (i in seq_len(ncol(columns))) {
      if (is(columns[[i]], "integer")) {
        columns[[i]] <- as.numeric(columns[[i]])
      }
    }
  }
  if (getOption("databaseConnectorInteger64AsNumeric", default = TRUE)) {
    for (i in seq_len(ncol(columns))) {
      if (is(columns[[i]], "integer64")) {
        columns[[i]] <- convertInteger64ToNumeric(columns[[i]])
      }
    }
  }
  return(columns)
}
