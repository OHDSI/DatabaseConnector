# Copyright 2026 Observational Health Data Sciences and Informatics
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

#' Get available Java heap space
#'
#' @description
#' For debugging purposes: get the available Java heap space.
#'
#' @return
#' The Java heap space (in bytes).
#'
#' @export
getAvailableJavaHeapSpace <- function() {
  availableSpace <- rJava::J("org.ohdsi.databaseConnector.BatchedQuery")$getAvailableHeapSpace()
  return(availableSpace)
}

validateInt64Query <- function() {
  # Validate that communication of 64-bit integers with Java is correct:
  array <- rJava::J("org.ohdsi.databaseConnector.BatchedQuery")$validateInteger64()
  oldClass(array) <- "integer64"
  if (!all.equal(array, bit64::as.integer64(c(1, -1, 8589934592, -8589934592)))) {
    abort("Error converting 64-bit integers between R and Java")
  }
}

parseJdbcColumnData <- function(batchedQuery,
                                columnTypes = NULL) {
  if (is.null(columnTypes)) {
    columnTypes <- rJava::.jcall(batchedQuery, "[I", "getColumnTypes")
  }
  columns <- vector("list", length(columnTypes))
  for (i in seq_along(columnTypes)) {
    if (columnTypes[i] == 1) {
      column <- rJava::.jcall(batchedQuery, "[D", "getNumeric", as.integer(i))
    } else if (columnTypes[i] == 5) {
      column <- rJava::.jcall(batchedQuery, "[D", "getInteger64", as.integer(i))
      oldClass(column) <- "integer64"
    } else if (columnTypes[i] == 6) {
      column <- rJava::.jcall(batchedQuery, "[I", "getInteger", as.integer(i))
    } else if (columnTypes[i] == 3) {
      column <- rJava::.jcall(batchedQuery, "[I", "getInteger", as.integer(i))
      column <- as.Date(column, origin = "1970-01-01")
    } else if (columnTypes[i] == 4) {
      column <- rJava::.jcall(batchedQuery, "[D", "getNumeric", as.integer(i))
      column <- as.POSIXct(column, origin = "1970-01-01")
    } else if (columnTypes[i] == 7) {
      column <- rJava::.jcall(batchedQuery, "[I", "getBoolean", as.integer(i))
      column <- vapply(column, FUN = function(x) ifelse(x == -1L, NA, as.logical(x)), FUN.VALUE = logical(1))
    } else {
      column <- rJava::.jcall(batchedQuery, "[Ljava/lang/String;", "getString", i)
    }
    columns[[i]] <- column
  }
  names(columns) <- rJava::.jcall(batchedQuery, "[Ljava/lang/String;", "getColumnNames")
  
  # More efficient than as.data.frame, as it avoids converting row.names to character:
  columns <- structure(columns, class = "data.frame", row.names = seq_len(length(columns[[1]])))
  return(columns)
}

ddlExecutionTimes <- new.env()
insertExecutionTimes <- new.env()

delayIfNecessary <- function(sql, regex, executionTimes, threshold) {
  regexGroups <- stringr::str_match(sql, stringr::regex(regex, ignore_case = TRUE))
  tableName <- regexGroups[3]
  if (!is.na(tableName) && !is.null(tableName)) {
    currentTime <- Sys.time()
    lastExecutedTime <- executionTimes[[tableName]]
    if (!is.na(lastExecutedTime) && !is.null(lastExecutedTime)) {
      delta <- difftime(currentTime, lastExecutedTime, units = "secs") 
      if (delta < threshold) {
        Sys.sleep(threshold - delta)
      }
    }
    executionTimes[[tableName]] <- currentTime
  }
}

delayIfNecessaryForDdl <- function(sql) {
  regexForDdl <- "(^CREATE\\s+TABLE\\s+IF\\s+EXISTS|^CREATE\\s+TABLE|^DROP\\s+TABLE\\s+IF\\s+EXISTS|^DROP\\s+TABLE)\\s+([a-zA-Z0-9_$#-]*\\.?\\s*(?:[a-zA-Z0-9_]+)*)"
  delayIfNecessary(sql, regexForDdl, ddlExecutionTimes, 5)
}

delayIfNecessaryForInsert <- function(sql) {
  regexForInsert <- "(^INSERT\\s+INTO)\\s+([a-zA-Z0-9_$#-]*\\.?\\s*(?:[a-zA-Z0-9_]+)*)"
  delayIfNecessary(sql, regexForInsert, insertExecutionTimes, 5)
}

# This helper function helps rlang handle rJava errors thrown by DatabaseConnector
# See https://github.com/tidyverse/dbplyr/issues/1186 and https://github.com/r-lib/rlang/issues/1619 for details
sanitizeJavaErrorForRlang <- function(expr) { tryCatch(expr, error = function(cnd) stop(conditionMessage(cnd))) }

lowLevelExecuteSql <- function(connection, sql) {
  statement <- rJava::.jcall(connection@jConnection, "Ljava/sql/Statement;", "createStatement")
  on.exit(sanitizeJavaErrorForRlang(rJava::.jcall(statement, "V", "close")))
  if ((dbms(connection) == "spark") || (dbms(connection) == "iris")) {
    # For some queries the DataBricks JDBC driver will throw an error saying no ROWCOUNT is returned
    # when using executeLargeUpdate, so using execute instead.
    # Also use this approach for IRIS JDBC driver, which does not support executeLargeUpdate() directly.
    sanitizeJavaErrorForRlang(rJava::.jcall(statement, "Z", "execute", as.character(sql), check = FALSE))
    rowsAffected <- rJava::.jcall(statement, "I", "getUpdateCount", check = FALSE)
    if (rowsAffected == -1) {
      rowsAffected <- 0
    }
  } else {
    rowsAffected <- sanitizeJavaErrorForRlang(rJava::.jcall(statement, "J", "executeLargeUpdate", as.character(sql), check = FALSE))
  }
  
  if (dbms(connection) == "bigquery") {
    delayIfNecessaryForDdl(sql)
    delayIfNecessaryForInsert(sql)
  }
  
  invisible(rowsAffected)
}

trySettingAutoCommit <- function(connection, value) {
  tryCatch(
    {
      rJava::.jcall(connection@jConnection, "V", "setAutoCommit", value)
    },
    error = function(cond) {
      # do nothing
    }
  )
}

lowLevelDbSendQuery <- function(conn, statement) {
  if (!DBI::dbIsValid(conn)) {
    abort("Connection is closed")
  }
  dbms <- dbms(conn)
  
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
  return(result)
}

getBatch <- function(batchedQuery) {
  rJava::.jcall(batchedQuery, "V", "fetchBatch")
  columns <- parseJdbcColumnData(batchedQuery)
}

getAllBatches <- function(batchedQuery) {
  columnTypes <- rJava::.jcall(batchedQuery, "[I", "getColumnTypes")
  if (any(columnTypes == 5)) {
    validateInt64Query()
  }
  data <- list()
  while (!rJava::.jcall(batchedQuery, "Z", "isDone")) {
    rJava::.jcall(batchedQuery, "V", "fetchBatch")
    batch <- parseJdbcColumnData(batchedQuery,
                                 columnTypes = columnTypes)
    data[[length(data) + 1]] <- batch
  }
  data <- do.call(rbind, data)
  return(data)
}
