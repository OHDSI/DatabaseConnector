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

#' @inherit
#' methods::show title description params details references return seealso
#' @export
setMethod("show", "DatabaseConnectorDriver", function(object) {
  cat("<DatabaseConnectorDriver>\n")
})

#' Create a DatabaseConnectorDriver object
#'
#' @export
DatabaseConnectorDriver <- function() {
  new("DatabaseConnectorDriver")
}


# Connection
# -----------------------------------------------------------------------------------------

#' Microsoft SQL Server class.
#'
#' @keywords internal
#' @export
#' @import DBI
setClass("Microsoft SQL Server",
         contains = "DBIConnection"
)


#' DatabaseConnectorConnection class.
#'
#' @keywords internal
#' @export
#' @import DBI
setClass("DatabaseConnectorConnection",
         contains = "Microsoft SQL Server",
         slots = list(
           identifierQuote = "character",
           stringQuote = "character", dbms = "character", uuid = "character"
         )
)

#' DatabaseConnectorJdbcConnection class.
#'
#' @keywords internal
#' @export
#' @import rJava
setClass("DatabaseConnectorJdbcConnection",
         contains = "DatabaseConnectorConnection",
         slots = list(jConnection = "jobjRef")
)

#' DatabaseConnectorDbiConnection class.
#'
#' @keywords internal
#' @export
#' @import DBI
setClass("DatabaseConnectorDbiConnection",
         contains = "DatabaseConnectorConnection",
         slots = list(
           dbiConnection = "DBIConnection",
           server = "character"
         )
)

#' Create a connection to a DBMS
#'
#' @description
#' Connect to a database. This function is synonymous with the \code{\link{connect}} function. except
#' a dummy driver needs to be specified
#'
#' @param drv   The result of the \code{link{DatabaseConnectorDriver}} function
#' @param ...   Other parameters. These are the same as expected by the \code{\link{connect}} function.
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

#' @rdname DatabaseConnectorConnection-class
#' @export
setMethod("dbDisconnect", "DatabaseConnectorConnection", function(conn) {
  disconnect(conn)
})

#' @rdname DatabaseConnectorConnection-class
#' @export
setMethod("show", "DatabaseConnectorConnection", function(object) {
  cat("<DatabaseConnectorConnection>", getServer(object))
})

#' @rdname DatabaseConnectorJdbcConnection-class
#' @export
setMethod("dbIsValid", "DatabaseConnectorJdbcConnection", function(dbObj, ...) {
  return(!rJava::is.jnull(dbObj@jConnection))
})

#' @rdname DatabaseConnectorDbiConnection-class
#' @export
setMethod("dbIsValid", "DatabaseConnectorDbiConnection", function(dbObj, ...) {
  return(DBI::dbIsValid(dbObj@dbiConnection))
})

#' @rdname DatabaseConnectorConnection-class
#' @export
setMethod("dbGetInfo", "DatabaseConnectorConnection", function(dbObj, ...) {
  return(list(db.version = "15.0"))
})

#' @rdname DatabaseConnectorConnection-class
#' @export
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

#' @rdname DatabaseConnectorConnection-class
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

#' @rdname DatabaseConnectorJdbcConnection-class
#' @export
setMethod(
  "dbSendQuery",
  signature("DatabaseConnectorJdbcConnection", "character"),
  function(conn, statement,
           ...) {
    if (rJava::is.jnull(conn@jConnection)) {
      abort("Connection is closed")
    }
    statement <- translateStatement(
      sql = statement,
      targetDialect = dbms(conn)
    )
    batchedQuery <- rJava::.jnew(
      "org.ohdsi.databaseConnector.BatchedQuery",
      conn@jConnection,
      statement,
      dbms(conn)
    )
    result <- new("DatabaseConnectorResult",
                  content = batchedQuery,
                  type = "batchedQuery",
                  statement = statement
    )
    return(result)
  }
)

#' @rdname DatabaseConnectorDbiConnection-class
#' @export
setMethod(
  "dbSendQuery",
  signature("DatabaseConnectorDbiConnection", "character"),
  function(conn, statement,
           ...) {
    statement <- translateStatement(
      sql = statement,
      targetDialect = dbms(conn)
    )
    return(DBI::dbSendQuery(conn@dbiConnection, statement, ...))
  }
)

#' @rdname DatabaseConnectorResult-class
#' @export
setMethod("dbHasCompleted", "DatabaseConnectorResult", function(res, ...) {
  if (res@type == "rowsAffected") {
    return(TRUE)
  } else {
    return(rJava::.jcall(res@content, "Z", "isDone"))
  }
})

#' @rdname DatabaseConnectorResult-class
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

#' @rdname DatabaseConnectorResult-class
#' @export
setMethod("dbGetStatement", "DatabaseConnectorResult", function(res, ...) {
  return(res@statement)
})

#' @rdname DatabaseConnectorResult-class
#' @export
setMethod("dbGetRowCount", "DatabaseConnectorResult", function(res, ...) {
  return(rJava::.jcall(res@content, "I", "getTotalRowCount"))
})

#' @rdname DatabaseConnectorResult-class
#' @export
setMethod("dbFetch", "DatabaseConnectorResult", function(res, ...) {
  rJava::.jcall(res@content, "V", "fetchBatch")
  columns <- parseJdbcColumnData(res@content, ...)
  return(columns)
})

#' @rdname DatabaseConnectorResult-class
#' @export
setMethod("dbClearResult", "DatabaseConnectorResult", function(res, ...) {
  if (res@type == "batchedQuery") {
    rJava::.jcall(res@content, "V", "clear")
  }
  return(TRUE)
})

#' @rdname DatabaseConnectorConnection-class
#' @export
setMethod(
  "dbGetQuery",
  signature("DatabaseConnectorConnection", "character"),
  function(conn, statement,
           ...) {
    statement <- translateStatement(
      sql = statement,
      targetDialect = dbms(conn)
    )
    lowLevelQuerySql(conn, statement)
  }
)

#' @rdname DatabaseConnectorConnection-class
#' @export
setMethod(
  "dbSendStatement",
  signature("DatabaseConnectorConnection", "character"),
  function(conn, statement,
           ...) {
    rowsAffected <- lowLevelExecuteSql(connection = conn, sql = statement)
    rowsAffected <- rJava::.jnew("java/lang/Integer", as.integer(rowsAffected))
    statement <- translateStatement(
      sql = statement,
      targetDialect = dbms(conn)
    )
    result <- new("DatabaseConnectorResult",
                  content = rowsAffected,
                  type = "rowsAffected",
                  statement = statement
    )
  }
)

#' @rdname DatabaseConnectorResult-class
#' @export
setMethod("dbGetRowsAffected", "DatabaseConnectorResult", function(res, ...) {
  if (res@type != "rowsAffected") {
    abort("Object not result of dbSendStatement")
  }
  return(rJava::.jsimplify(res@content))
})

#' @rdname DatabaseConnectorConnection-class
#' @export
setMethod(
  "dbExecute",
  signature("DatabaseConnectorConnection", "character"),
  function(conn, statement,
           ...) {
    if (isDbplyrSql(sql) && dbms(conn) == "oracle" && grepl("^UPDATE STATISTICS", statement)) {
      return(0)
    }
    statement <- translateStatement(
      sql = statement,
      targetDialect = dbms(conn)
    )
    rowsAffected <- lowLevelExecuteSql(connection = conn, sql = statement)
    return(rowsAffected)
  }
)

# Misc ----------------------------------------------------------------------

#' @rdname DatabaseConnectorConnection-class
#' 
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


#' @rdname DatabaseConnectorConnection-class
#' 
#' @param database   Name of the database.
#' @param schema     Name of the schema.
#' 
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

#' @rdname DatabaseConnectorConnection-class
#' 
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

#' @rdname DatabaseConnectorConnection-class
#' 
#' @param temporary          Should the table created as a temp table?
#' @template TempEmulationSchema
#
#' @export
setMethod(
  "dbAppendTable",
  signature("DatabaseConnectorConnection", "character"),
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

#' @rdname DatabaseConnectorConnection-class
#' @export
setMethod(
  "dbCreateTable",
  "DatabaseConnectorConnection",
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

#' @rdname DatabaseConnectorConnection-class
#' 
#' @param database              Name of the database.
#' @param schema                Name of the schema.
#' @template TempEmulationSchema
#'
#' @export
setMethod("dbReadTable", 
          signature("DatabaseConnectorConnection", "character"), 
          function(conn, 
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
            sql <- translateStatement(
              sql = sql,
              targetDialect = dbms(conn),
              tempEmulationSchema = tempEmulationSchema
            )
            return(lowLevelQuerySql(conn, sql))
          })

#' @rdname DatabaseConnectorConnection-class
#' 
#' @param database           Name of the database.
#' @param schema             Name of the schema.
#' @template TempEmulationSchema
#'
#' @export
setMethod(
  "dbRemoveTable",
  "DatabaseConnectorConnection",
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
    sql <- translateStatement(
      sql = sql,
      targetDialect = dbms(conn),
      tempEmulationSchema = tempEmulationSchema
    )
    for (statement in SqlRender::splitSql(sql)) {
      lowLevelExecuteSql(conn, statement)
    }
    return(TRUE)
  }
)

#' Refer to a table in a database schema
#' 
#' @description 
#' Can be used with \code{dplyr::tbl()} to indicate a table in a specific database schema.
#'
#' @param databaseSchema 
#' @param table 
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

translateStatement <- function(sql, targetDialect) {
  if (isDbplyrSql(sql) && !grepl(";\\s*$", sql)) {
    # SqlRender requires statements to end with semicolon, but dbplyr does not generate these:
    sql <- paste0(sql, ";")
  }
  sql <- SqlRender::translate(sql, targetDialect)
  # Remove trailing semicolons for Oracle: (alternatively could use querySql instead of lowLevelQuery)
  sql <- gsub(";\\s*$", "", sql)
  return(sql)
}

