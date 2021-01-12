# @file Drivers.R
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

jdbcDrivers <- new.env()

#' Download DatabaseConnector JDBC Jar files
#' 
#' Download the DatabaseConnector JDBC drivers from https://ohdsi.github.io/DatabaseConnectorJars/
#'
#' @param pathToDriver The full path to the folder where the JDBC driver .jar files should be downloaded to.
#'        By default the value of the environment variable "DATABASECONNECTOR_JAR_FOLDER" is used.
#' @param dbms The type of DBMS to download Jar files for.
#'      \itemize{
#'          \item{"postgresql" for PostgreSQL}
#'          \item{"redshift" for Amazon Redshift}
#'          \item{"sql server" or "pdw" for Microsoft SQL Server}
#'          \item{"oracle" for Oracle}
#'      }
#' @param method The method used for downloading files. See \code{?download.file} for details and options.
#' @param ... Further arguments passed on to \code{download.file} 
#' 
#' @return Invisibly returns the destination if the download was successful.
#' @export
#'
#' @examples
#' \dontrun{
#' downloadJdbcDrivers("redshift")
#' }
downloadJdbcDrivers <- function(dbms, pathToDriver = Sys.getenv("DATABASECONNECTOR_JAR_FOLDER"), method = "auto", ...){
  
  if (is.null(pathToDriver) || is.na(pathToDriver) || pathToDriver == "") 
    stop("The pathToDriver argument must be specified. Consider setting the DATABASECONNECTOR_JAR_FOLDER environment variable, for example in the .Renviron file.")
  
  if (pathToDriver != Sys.getenv("DATABASECONNECTOR_JAR_FOLDER")) {
    if (Sys.getenv("DATABASECONNECTOR_JAR_FOLDER") != pathToDriver) {
      inform(paste0("Consider adding `DATABASECONNECTOR_JAR_FOLDER='", 
                    pathToDriver,
                    "'` to ", 
                    path.expand("~/.Renviron"), " and restarting R."))
    }
  }
  
  pathToDriver <- path.expand(pathToDriver)
  if (!dir.exists(pathToDriver)) {
    warn(paste0("The folder location '", pathToDriver, "' does not exist. Attempting to create."))
    dir.create(pathToDriver, recursive = TRUE)
  }
  
  stopifnot(is.character(dbms), length(dbms) == 1, dbms %in% c("all", "postgresql", "redshift", "sql server", "oracle", "pdw"))
  
  if (dbms == "pdw") {
    dbms <- "sql server"
  }
  
  baseUrl <- "https://ohdsi.github.io/DatabaseConnectorJars/"
  
  jdbcDriverNames <- c("postgresql" = "postgresqlV42.2.18.zip",
                       "redshift" = "redShiftV1.2.27.1051.zip",
                       "sql server" = "sqlServerV8.4.1.zip",
                       "oracle" = "oracleV19.8.zip")
  
  driverName <- jdbcDriverNames[[dbms]]
  result <- download.file(url = paste0(baseUrl, driverName),
                          destfile = paste(pathToDriver, driverName, sep = "/"),
                          method = method)
  
  extractedFilename <- unzip(file.path(pathToDriver, driverName), exdir = pathToDriver)
  unzipSuccess <- is.character(extractedFilename)
  
  if (unzipSuccess) {
    file.remove(file.path(pathToDriver, driverName))
  }
  if (unzipSuccess && result == 0) { 
    inform(paste0("DatabaseConnector JDBC drivers downloaded to '", pathToDriver, "'."))
  } else {
    abort(paste0("Downloading and unzipping of JDBC drivers to '", pathToDriver, "' has failed."))
  }
  
  invisible(pathToDriver)
}

loadJdbcDriver <- function(driverClass, classPath) {
  rJava::.jaddClassPath(classPath)
  if (nchar(driverClass) && rJava::is.jnull(rJava::.jfindClass(as.character(driverClass)[1])))
    abort("Cannot find JDBC driver class ", driverClass)
  jdbcDriver <- rJava::.jnew(driverClass, check = FALSE)
  rJava::.jcheck(TRUE)
  return(jdbcDriver)
}

# Singleton pattern to ensure driver is instantiated only once
getJbcDriverSingleton <- function(driverClass = "", classPath = "") {
  key <- paste(driverClass, classPath)
  if (key %in% ls(jdbcDrivers)) {
    driver <- get(key, jdbcDrivers)
    if (rJava::is.jnull(driver)) {
      driver <- loadJdbcDriver(driverClass, classPath)
      assign(key, driver, envir = jdbcDrivers)
    }
  } else {
    driver <- loadJdbcDriver(driverClass, classPath)
    assign(key, driver, envir = jdbcDrivers)
  }
  driver
}

findPathToJar <- function(name, pathToDriver = Sys.getenv("DATABASECONNECTOR_JAR_FOLDER")) {
  if (missing(pathToDriver) || is.null(pathToDriver) || is.na(pathToDriver)) {
    abort("The pathToDriver argument must be provided")
  } else if (!dir.exists(pathToDriver)) {
    abort("The folder location pathToDriver = '", pathToDriver, "' does not exist. ")
  } else {
    if (grepl(".jar$", tolower(pathToDriver))) {
      pathToDriver <- basename(pathToDriver)
    }
  }
  files <- list.files(path = pathToDriver, pattern = name, full.names = TRUE)
  if (length(files) == 0) {
    abort(paste("No drives matching pattern", name, "found in folder", pathToDriver, ".",
                "\nPlease download the JDBC drivers for your database to the folder.",
                "You can download most drivers using the `downloadJdbcDrivers()` function."))
  } else {
    return(files)
  }
}
