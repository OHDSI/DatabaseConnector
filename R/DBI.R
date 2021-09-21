# @file DBI.R
#
# Copyright 2021 Observational Health Data Sciences and Informatics
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

#' DatabaseConnectorConnection class.
#'
#' @keywords internal
#' @export
#' @import DBI
setClass("DatabaseConnectorConnection",
         contains = "DBIConnection",
         slots = list(identifierQuote = "character",
                      stringQuote = "character",
                      dbms = "character",
                      uuid = "character"))

#' DatabaseConnectorJdbcConnection class.
#'
#' @keywords internal
#' @export
#' @import rJava
setClass("DatabaseConnectorJdbcConnection",
         contains = "DatabaseConnectorConnection",
         slots = list(jConnection = "jobjRef"))

#' DatabaseConnectorDbiConnection class.
#'
#' @keywords internal
#' @export
#' @import DBI
setClass("DatabaseConnectorDbiConnection",
         contains = "DatabaseConnectorConnection",
         slots = list(dbiConnection = "DBIConnection",
                      server = "character"))

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
#'                   dbms = "postgresql",
#'                   server = "localhost/ohdsi",
#'                   user = "joe",
#'                   password = "secret")
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

#' @inherit
#' DBI::dbDisconnect title description params details references return seealso
#' @export
setMethod("dbDisconnect", "DatabaseConnectorConnection", function(conn) {
  disconnect(conn)
})

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
setMethod("dbQuoteIdentifier",
          signature("DatabaseConnectorConnection", "character"),
          function(conn, x, ...) {
            if (length(x) == 0L) {
              return(DBI::SQL(character()))
            }
            if (any(is.na(x))) {
              abort("Cannot pass NA to dbQuoteIdentifier()")
            }
            if (nzchar(conn@identifierQuote)) {
              x <- gsub(conn@identifierQuote, paste0(conn@identifierQuote,
                                                     conn@identifierQuote), x, fixed = TRUE)
            }
            return(DBI::SQL(paste0(conn@identifierQuote, encodeString(x), conn@identifierQuote)))
          })

#' @inherit
#' DBI::dbQuoteString title description params details references return seealso
#' @export
setMethod("dbQuoteString",
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
          })

# Results -----------------------------------------------------------------------------------------

#' DatabaseConnector results class.
#'
#' @keywords internal
#' @import rJava
#' @export
setClass("DatabaseConnectorResult",
         contains = "DBIResult",
         slots = list(content = "jobjRef", type = "character", statement = "character"))

#' @inherit
#' DBI::dbSendQuery title description params details references return seealso
#' @export
setMethod("dbSendQuery",
          signature("DatabaseConnectorJdbcConnection", "character"),
          function(conn, statement, ...) {
            if (rJava::is.jnull(conn@jConnection))
              abort("Connection is closed")
            batchedQuery <- rJava::.jnew("org.ohdsi.databaseConnector.BatchedQuery",
                                         conn@jConnection,
                                         statement,
                                         conn@dbms)
            result <- new("DatabaseConnectorResult",
                          content = batchedQuery,
                          type = "batchedQuery",
                          statement = statement)
            return(result)
          })

#' @inherit
#' DBI::dbSendQuery title description params details references return seealso
#' @export
setMethod("dbSendQuery",
          signature("DatabaseConnectorDbiConnection", "character"),
          function(conn, statement, ...) {
            return(DBI::dbSendQuery(conn@dbiConnection, statement, ...))
          })

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


parseJdbcColumnData <- function(content,
                                columnTypes = NULL,
                                datesAsString = FALSE,
                                integerAsNumeric = getOption("databaseConnectorIntegerAsNumeric",
                                                             default = TRUE),
                                integer64AsNumeric = getOption("databaseConnectorInteger64AsNumeric",
                                                               default = TRUE)) {

  if (is.null(columnTypes))
    columnTypes <- rJava::.jcall(content, "[I", "getColumnTypes")

  columns <- vector("list", length(columnTypes))

  for (i in seq.int(length(columnTypes))) {
    if (columnTypes[i] == 1) {
      column <- rJava::.jcall(content,
                              "[D",
                              "getNumeric",
                              as.integer(i))
      # rJava doesn't appear to be able to return NAs, so converting NaNs to NAs:
      column[is.nan(column)] <- NA
      columns[[i]] <- c(columns[[i]], column)
    } else if (columnTypes[i] == 5) {
      column <- rJava::.jcall(content,
                              "[D",
                              "getInteger64",
                              as.integer(i))
      oldClass(column) <- "integer64"
      if (is.null(columns[[i]])) {
        columns[[i]] <- column
      } else {
        columns[[i]] <- c(columns[[i]], column)
      }

      if (integer64AsNumeric) {
        columns[[i]] <- convertInteger64ToNumeric(columns[[i]])
      }
    } else if (columnTypes[i] == 6) {
      columns[[i]] <- c(columns[[i]],
                        rJava::.jcall(content,
                                      "[I",
                                      "getInteger",
                                      as.integer(i)))
      if (integerAsNumeric) {
        columns[[i]] <- as.numeric(columns[[i]])
      }
    } else {
      columns[[i]] <- c(columns[[i]],
                        rJava::.jcall(content, "[Ljava/lang/String;", "getString", i))

      if (!datesAsString) {
        if (columnTypes[i] == 3) {
          columns[[i]] <- as.Date(columns[[i]])
        } else if (columnTypes[i] == 4) {
          columns[[i]] <- as.POSIXct(columns[[i]])
        }
      }
    }
  }

  names(columns) <- rJava::.jcall(content, "[Ljava/lang/String;", "getColumnNames")
  # More efficient than as.data.frame, as it avoids converting row.names to character:
  columns <- structure(columns, class = "data.frame", row.names = seq_len(length(columns[[1]])))
  return(columns)
}

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
setMethod("dbGetQuery",
          signature("DatabaseConnectorConnection", "character"),
          function(conn, statement, ...) {
            lowLevelQuerySql(conn, statement)
          })

#' @inherit
#' DBI::dbSendStatement title description params details references return seealso
#' @export
setMethod("dbSendStatement",
          signature("DatabaseConnectorConnection", "character"),
          function(conn, statement, ...) {
            rowsAffected <- lowLevelExecuteSql(connection = conn, sql = statement)
            rowsAffected <- rJava::.jnew("java/lang/Integer", as.integer(rowsAffected))
            result <- new("DatabaseConnectorResult",
                          content = rowsAffected,
                          type = "rowsAffected",
                          statement = statement)
          })

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
setMethod("dbExecute",
          signature("DatabaseConnectorConnection", "character"),
          function(conn, statement, ...) {
            rowsAffected <- lowLevelExecuteSql(connection = conn, sql = statement)
            return(rowsAffected)
          })

# Misc ----------------------------------------------------------------------

#' @inherit
#' DBI::dbListFields title description params details references return seealso
#' @param database   Name of the database.
#' @param schema     Name of the schema.
#'
#' @export
setMethod("dbListFields",
          signature("DatabaseConnectorConnection", "character"),
          function(conn, name, database = NULL, schema = NULL, ...) {
            columns <- listDatabaseConnectorColumns(connection = conn,
                                                    catalog = database,
                                                    schema = schema,
                                                    table = name)
            return(columns$name)
          })

#' @inherit
#' DBI::dbListTables title description params details references return seealso
#' @param database   Name of the database.
#' @param schema     Name of the schema.
#'
#' @export
setMethod("dbListTables",
          "DatabaseConnectorConnection",
          function(conn, database = NULL, schema = NULL, ...) {
            if (is.null(database)) {
              databaseSchema <- schema
            } else {
              databaseSchema <- paste(database, schema, sep = ".")
            }
            return(getTableNames(conn, databaseSchema))
          })

#' @inherit
#' DBI::dbExistsTable title description params details references return seealso
#' @param database   Name of the database.
#' @param schema     Name of the schema.
#' @export
setMethod("dbExistsTable",
          signature("DatabaseConnectorConnection", "character"),
          function(conn, name, database = NULL, schema = NULL, ...) {
            if (length(name) != 1) {
              abort("Name should be a single string")
            }
            tables <- dbListTables(conn, name = name, database = database, schema = schema)
            return(tolower(name) %in% tolower(tables))
          })

#' @inherit
#' DBI::dbWriteTable title description params details references return seealso
#' @param overwrite          Overwrite an existing table (if exists)?
#' @param append             Append to existing table?
#' @param temporary          Should the table created as a temp table?
#' @param oracleTempSchema   Specifically for Oracle, a schema with write privileges where temp tables
#'                           can be created.
#
#' @export
setMethod("dbWriteTable",
          signature("DatabaseConnectorConnection", "character", "data.frame"),
          function(conn,
                   name,
                   value,
                   overwrite = FALSE,
                   append = FALSE,
                   temporary = FALSE,
                   oracleTempSchema = NULL,
                   ...) {
            if (overwrite)
              append <- FALSE
            insertTable(connection = conn,
                        tableName = name,
                        data = value,
                        dropTableIfExists = overwrite,
                        createTable = !append,
                        tempTable = temporary,
                        oracleTempSchema = oracleTempSchema)
            invisible(TRUE)
          })

#' @inherit
#' DBI::dbAppendTable title description params details references return seealso
#' @param temporary          Should the table created as a temp table?
#' @param oracleTempSchema   Specifically for Oracle, a schema with write privileges where temp tables
#'                           can be created.
#
#' @export
setMethod("dbAppendTable",
          signature("DatabaseConnectorConnection", "character", "data.frame"),
          function(conn,
                   name,
                   value,
                   temporary = FALSE,
                   oracleTempSchema = NULL,
                   ...,
                   row.names = NULL) {
            insertTable(connection = conn,
                        tableName = name,
                        data = value,
                        dropTableIfExists = FALSE,
                        createTable = FALSE,
                        tempTable = temporary,
                        oracleTempSchema = oracleTempSchema)
            invisible(TRUE)
          })

#' @inherit
#' DBI::dbCreateTable title description params details references return seealso
#' @param temporary          Should the table created as a temp table?
#' @param oracleTempSchema   Specifically for Oracle, a schema with write privileges where temp tables
#'                           can be created.
#
#' @export
setMethod("dbCreateTable",
          signature("DatabaseConnectorConnection", "character", "data.frame"),
          function(conn,
                   name,
                   fields,
                   oracleTempSchema = NULL,
                   ...,
                   row.names = NULL,
                   temporary = FALSE) {
            insertTable(connection = conn,
                        tableName = name,
                        data = fields[FALSE,],
                        dropTableIfExists = TRUE,
                        createTable = TRUE,
                        tempTable = temporary,
                        oracleTempSchema = oracleTempSchema)
            invisible(TRUE)
          })

#' @inherit
#' DBI::dbReadTable title description params details references return seealso
#' @param database           Name of the database.
#' @param schema             Name of the schema.
#' @param oracleTempSchema    DEPRECATED: use \code{tempEmulationSchema} instead.
#' @param tempEmulationSchema Some database platforms like Oracle and Impala do not truly support temp tables. To
#'                            emulate temp tables, provide a schema with write privileges where temp tables
#'                            can be created.
#' @export
setMethod("dbReadTable",
          signature("DatabaseConnectorConnection", "character"),
          function(conn, name, database = NULL, schema = NULL, oracleTempSchema = NULL, tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"), ...) {
            if (!is.null(oracleTempSchema) && oracleTempSchema != "") {
              warn("The 'oracleTempSchema' argument is deprecated. Use 'tempEmulationSchema' instead.",
                   .frequency = "regularly",
                   .frequency_id = "oracleTempSchema")
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
            sql <- SqlRender::translate(sql = sql,
                                        targetDialect = conn@dbms,
                                        tempEmulationSchema = tempEmulationSchema)
            return(lowLevelQuerySql(conn, sql))
          })

#' @inherit
#' DBI::dbRemoveTable title description params details references return seealso
#' @param database           Name of the database.
#' @param schema             Name of the schema.
#' @param oracleTempSchema   Specifically for Oracle, a schema with write privileges where temp tables
#'                           can be created.
#' @export
setMethod("dbRemoveTable",
          signature("DatabaseConnectorConnection", "character"),
          function(conn, name, database = NULL, schema = NULL, oracleTempSchema = NULL, ...) {
            if (!is.null(schema)) {
              name <- paste(schema, name, sep = ".")
            }
            if (!is.null(database)) {
              name <- paste(database, name, sep = ".")
            }
            sql <- "TRUNCATE TABLE @table; DROP TABLE @table;"
            sql <- SqlRender::render(sql = sql, table = name)
            sql <- SqlRender::translate(sql = sql,
                                        targetDialect = conn@dbms,
                                        oracleTempSchema = oracleTempSchema)
            for (statement in SqlRender::splitSql(sql)) {
              lowLevelExecuteSql(conn, statement)
            }
            return(TRUE)
          })
