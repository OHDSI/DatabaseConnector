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

#' Compute difference between dates
#'
#' @param interval Unit for the interval. Can be "day", "week", "month", "year".
#' @param date1    The first date.
#' @param date2    The second date.
#'
#' @template DbiDateFunction
#'
#' @return
#' The numeric value of the difference.
#'
#' @examples
#' dateDiff("day", as.Date("2000-01-01"), as.Date("2000-03-01"))
#'
#' @export
dateDiff <- function(interval, date1, date2) {
  interval <- tolower(interval)
  if (interval %in% c("day", "dd", "d")) {
    return(as.numeric(difftime(time1 = date2, time2 = date1, units = "days")))
  } else {
    abort(sprintf("Translation of unit '%s' not supported", interval))
  }
}

#' Add an interval to a date
#'
#' @param interval Unit for the interval. Can be "day", "week", "month", "year".
#' @param number   The number of units to add to the date.
#' @param date     The date to add to.
#'
#' @template DbiDateFunction
#'
#' @return
#' A new date.
#'
#' @examples
#' dateAdd("day", 10, as.Date("2000-01-01"))
#' 
#' @export
dateAdd <- function(interval, number, date) {
  interval <- tolower(interval)
  if (interval %in% c("day", "dd", "d")) {
    return(as.Date(date) + number)
  } else {
    abort(sprintf("Translation of unit '%s' not supported", interval))
  }
}

#' Return the end of the month
#'
#' @param date A date in the month for which we need the end.
#'
#' @template DbiDateFunction
#'
#' @return
#' The date of the last day of the month.
#'
#' @examples
#' eoMonth(as.Date("2000-02-01"))
#' 
#' @export
eoMonth <- function(date) {
  return(.ifElse(month(date) != month(date + 1),
         date,
         .ifElse(month(date) == 12,
                dateFromParts(year(date) + 1, 1, 1) - 1,
                dateFromParts(year(date), month(date) + 1, 1) - 1)
  ))
}

# Note: base ifelse converts dates to integers for unknown reasons, so adding this:
.ifElse <- function (condition, true, false) {
  false[condition] <- true[condition]
  return(false)
}

#' Construct a date from parts
#'
#' @param year  The calendar year.
#' @param month The calendar month (1 = January).
#' @param day   The day of the month.
#'
#' @template DbiDateFunction
#'
#' @return
#' The date.
#'
#' @examples
#' dateFromParts(2000, 1, 5)
#' 
#' @export
dateFromParts <- function(year, month, day) {
  return(as.Date(paste(year, month, day, sep='-'), optional = TRUE))
}

#' Extract the year from a date
#'
#' @param date  The date.
#'
#' @template DbiDateFunction
#'
#' @return
#' The year
#'
#' @examples
#' year(as.Date("2000-02-01"))
#' 
#' @export
year <- function(date) {
  return(as.numeric(format(date, format = "%Y")))
}

#' Extract the month from a date
#'
#' @param date  The date.
#'
#' @template DbiDateFunction
#'
#' @return
#' The month
#'
#' @examples
#' month(as.Date("2000-02-01"))
#' 
#' @export
month <- function(date) {
  return(as.numeric(format(date, format = "%m")))
}

#' Extract the day from a date
#'
#' @param date  The date.
#'
#' @template DbiDateFunction
#'
#' @return
#' The day
#'
#' @examples
#' day(as.Date("2000-02-01"))
#' 
#' @export
day <- function(date) {
  return(as.numeric(format(date, format = "%d")))
}

translateDateFunctions <- function(sql) {
  # sql <- "SELECT DatabaseConnector::dateAdd(1 AS value, DatabaseConnector::dateAdd('day', 1, a_date) AS date, 'day' AS unit) AS start_date FROM my_table;"
  # sql <- "SELECT dateDiff('day', a_date, another_date) AS start_date FROM my_table;"
  translator <- rJava::new(rJava::J("org.ohdsi.databaseConnector.RtoSqlTranslator"), string = sql)
  
  fun <- rJava::new(rJava::J("org.ohdsi.databaseConnector.RFunctionToTranslate"), name = "dateDiff")  
  fun$addArgument(name = "interval", removeQuotes = TRUE)
  fun$addArgument(name = "date1") 
  fun$addArgument(name = "date2") 
  translator$translate(fun)
  
  fun <- rJava::new(rJava::J("org.ohdsi.databaseConnector.RFunctionToTranslate"), name = "dateAdd")  
  fun$addArgument(name = "interval", removeQuotes = TRUE)
  fun$addArgument(name = "number") 
  fun$addArgument(name = "date") 
  translator$translate(fun)
  
  fun <- rJava::new(rJava::J("org.ohdsi.databaseConnector.RFunctionToTranslate"), name = "eoMonth")  
  fun$addArgument(name = "date") 
  translator$translate(fun)
  
  fun <- rJava::new(rJava::J("org.ohdsi.databaseConnector.RFunctionToTranslate"), name = "dateFromParts")  
  fun$addArgument(name = "year")
  fun$addArgument(name = "month") 
  fun$addArgument(name = "day") 
  translator$translate(fun)
  
  fun <- rJava::new(rJava::J("org.ohdsi.databaseConnector.RFunctionToTranslate"), name = "year")  
  fun$addArgument(name = "date") 
  translator$translate(fun)
  
  fun <- rJava::new(rJava::J("org.ohdsi.databaseConnector.RFunctionToTranslate"), name = "month")  
  fun$addArgument(name = "date") 
  translator$translate(fun)
  
  fun <- rJava::new(rJava::J("org.ohdsi.databaseConnector.RFunctionToTranslate"), name = "day")  
  fun$addArgument(name = "date") 
  translator$translate(fun)
  
  sql <- translator$getSql()
  sql
  return(sql)
}

