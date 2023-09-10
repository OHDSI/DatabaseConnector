# Copyright 2023 Observational Health Data Sciences and Informatics
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

truncateSql <- function(sql, maxLength = 150) {
  sql <- paste(sql, collapse = "\n")
  if (nchar(sql) > maxLength) {
    sql <- paste0(substr(sql, 1, maxLength), "...")
  }
  sql <- gsub("\r", "", gsub("\n", "\\\\n", sql))
  return(sql)
}

#' Extract query times from a `ParallelLogger` log file
#' 
#' @description 
#' When using the `ParallelLogger` default file logger, and using `options(LOG_DATABASECONNECTOR_SQL = TRUE)`,
#' `DatabaseConnector` will log all SQL sent to the server, and the time to get a response. 
#' 
#' This function parses the log file, producing a data frame with time per query.
#'
#' @param logFileName Name of the `ParallelLogger` log file. Assumes the file was created using
#'                    the default file logger.              
#'
#' @return
#' A data frame with queries and their run times in milliseconds.
#' 
#' @examples 
#' 
#' connection <- connect(dbms = "sqlite", server = ":memory:")
#' logFile <- tempfile(fileext = ".log")
#' ParallelLogger::addDefaultFileLogger(fileName = logFile, name = "MY_LOGGER")
#' options(LOG_DATABASECONNECTOR_SQL = TRUE)
#' 
#' executeSql(connection, "CREATE TABLE test (x INT);")
#' querySql(connection, "SELECT * FROM test;")
#' 
#' extractQueryTimes(logFile)
#' 
#' ParallelLogger::unregisterLogger("MY_LOGGER")
#' unlink(logFile)
#' disconnect(connection)
#' 
#' @export
extractQueryTimes <- function(logFileName) {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertCharacter(logFileName, len = 1, add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)
  
  # Possible improvement: read log file in batches, filtering each batch to the relevant DatabaseConnector trace 
  # logs, to avoid running out of memory for very large log files.
  logLines <- readLines(logFileName)
  logLines <- logLines[grepl("\tTRACE\tDatabaseConnector\tlogTrace\t(Querying|Executing|Sending) SQL", logLines)]
  
  threads <- unique(stringr::str_extract(logLines, "(?<=\t\\[)(Main thread|Thread [0-9]+)(?=\\]\t)"))
  queryTimes <- lapply(threads, extractQueryTimesFromThread, logLines = logLines)
  queryTimes <- do.call(rbind, queryTimes)
  return(queryTimes)
}

extractQueryTimesFromThread <- function(thread, logLines) {
  logLines <- logLines[grepl(sprintf("\t\\[%s\\]\t", thread), logLines)]
  queryIdx <- which(grepl("\tTRACE\tDatabaseConnector\tlogTrace\t(Querying|Executing|Sending) SQL: ", logLines))
  if (length(queryIdx) == 0) {
    return(NULL)
  }
  timeIdx <- which(grepl("\tTRACE\tDatabaseConnector\tlogTrace\t(Querying|Executing) SQL took ", logLines))
  
  # Align query and time indexes in case we missed a query or time:
  alignedTimeIdx <- rep(as.numeric(NA), length(queryIdx))
  maxTimeIdx <- max(timeIdx)
  for (i in seq_along(queryIdx)) {
    if (queryIdx[i] < maxTimeIdx) {
      alignedTimeIdx[i] <- min(timeIdx[timeIdx > queryIdx[i]])
    }  
  }
  
  durationString <- sub("^.*\t(Querying|Executing) SQL took ", "", logLines[alignedTimeIdx])
  parts <- stringr::str_split(durationString, " ", simplify = TRUE)
  msPerUnit <- ifelse(parts[, 2] == "secs", 1000, 
                      ifelse(parts[, 2] == "mins", 60 * 1000, 
                             ifelse(parts[, 2] == "hours", 60 * 60 * 1000, 
                                    ifelse(parts[, 2] == "days", 24 * 60 * 60 * 1000, 
                                           ifelse(parts[, 2] == "weeks", 7 * 24 * 60 * 60 * 1000, NA)
                                    )
                             )
                      )
  )
  milliseconds <- round(as.numeric(parts[, 1]) * msPerUnit)
  result <- data.frame(
    query = gsub("^.*\t(Querying|Executing|Sending) SQL: ", "", logLines[queryIdx]),
    startTime = as.POSIXct(gsub("\t.*$", "", logLines[queryIdx])),
    milliseconds = milliseconds,
    thread = thread
  )
  return(result) 
}
