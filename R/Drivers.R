# @file Drivers.R
#
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

jdbcDrivers <- new.env()

#' Download DatabaseConnector JDBC Jar files
#'
#' Download the DatabaseConnector JDBC drivers from https://ohdsi.github.io/DatabaseConnectorJars/
#'
#' @param pathToDriver The full path to the folder where the JDBC driver .jar files should be downloaded to.
#'        By default the value of the environment variable "DATABASECONNECTOR_JAR_FOLDER" is used.
#' @param dbms The type of DBMS to download Jar files for.
#'  
#' - "postgresql" for PostgreSQL
#' - "redshift" for Amazon Redshift
#' - "sql server", "pdw" or "synapse" for Microsoft SQL Server
#' - "oracle" for Oracle
#' - "spark" for Spark
#' - "snowflake" for Snowflake
#' - "bigquery" for Google BigQuery
#' - "all" for all aforementioned platforms
#'  
#' @param method The method used for downloading files. See `?download.file` for details and options.
#' @param ... Further arguments passed on to `download.file`.
#'
#' @details
#' The following versions of the JDBC drivers are currently used:
#' 
#' - PostgreSQL: V42.7.3
#' - RedShift: V2.1.0.9
#' - SQL Server: V9.2.0
#' - Oracle: V19.8
#' - Spark (Databricks): V2.6.36
#' - Snowflake: V3.16.01
#' - BigQuery: v1.3.2.1003
#' 
#' @return Invisibly returns the destination if the download was successful.
#' @export
#'
#' @examples
#' \dontrun{
#' downloadJdbcDrivers("redshift")
#' }
downloadJdbcDrivers <- function(dbms, pathToDriver = Sys.getenv("DATABASECONNECTOR_JAR_FOLDER"), method = "auto", ...) {
  if (is.null(pathToDriver) || is.na(pathToDriver) || pathToDriver == "") {
    abort("The pathToDriver argument must be specified. Consider setting the DATABASECONNECTOR_JAR_FOLDER environment variable, for example in the .Renviron file.")
  }
  
  if (pathToDriver != Sys.getenv("DATABASECONNECTOR_JAR_FOLDER")) {
    if (Sys.getenv("DATABASECONNECTOR_JAR_FOLDER") != pathToDriver) {
      inform(paste0(
        "Consider adding `DATABASECONNECTOR_JAR_FOLDER='",
        pathToDriver,
        "'` to ",
        path.expand("~/.Renviron"), " and restarting R."
      ))
    }
  }
  
  pathToDriver <- path.expand(pathToDriver)
  
  if (!dir.exists(pathToDriver)) {
    if (file.exists(pathToDriver)) {
      abort(paste0("The folder location pathToDriver = '", pathToDriver, "' points to a file, but should point to a folder."))
    }
    warn(paste0("The folder location '", pathToDriver, "' does not exist. Attempting to create."))
    dir.create(pathToDriver, recursive = TRUE)
  }
  
  stopifnot(is.character(dbms), length(dbms) == 1, dbms %in% c("all", "postgresql", "redshift", "sql server", "oracle", "pdw", "snowflake", "spark", "bigquery"))
  
  if (dbms == "pdw" || dbms == "synapse") {
    dbms <- "sql server"
  }
  
  jdbcDriverSources <- utils::read.csv(text = 
                                         "row,dbms, fileName, baseUrl
    1,postgresql,postgresql-42.7.3.jar,https://jdbc.postgresql.org/download/
    2,redshift,redshift-jdbc42-2.1.0.20.zip,https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/2.1.0.20/
    3,sql server,sqlServerV9.2.0.zip,https://ohdsi.github.io/DatabaseConnectorJars/
    4,oracle,oracleV19.8.zip,https://ohdsi.github.io/DatabaseConnectorJars/
    5,spark,DatabricksJDBC42-2.6.36.1062.zip,https://databricks-bi-artifacts.s3.us-east-2.amazonaws.com/simbaspark-drivers/jdbc/2.6.36/
    6,snowflake,snowflake-jdbc-3.16.1.jar,https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.16.1/
    7,bigquery,SimbaJDBCDriverforGoogleBigQuery42_1.6.2.1003.zip,https://storage.googleapis.com/simba-bq-release/jdbc/"
  )

  if (dbms == "all") {
    dbms <- jdbcDriverSources$dbms
  }
  for (db in dbms) {
    if (db == "redshift") {
      oldFiles <- list.files(pathToDriver, "Redshift")
      if (length(oldFiles) > 0) {
        message(sprintf("Prior JAR files have already been detected: '%s'. Do you want to delete them?", paste(oldFiles, collapse = "', '")))
        if (interactive() && utils::menu(c("Yes", "No")) == 1) {
          unlink(file.path(pathToDriver, oldFiles))
        }
      }
    }
    driverSource <- jdbcDriverSources[jdbcDriverSources$dbms == db, ]
    if (grepl("\\.zip$", driverSource$fileName)) {
      # Zip file. Download and unzip
      result <- download.file(
        url = paste0(driverSource$baseUrl, driverSource$fileName),
        destfile = file.path(pathToDriver, driverSource$fileName),
        method = method
      )
      
      extractedFilename <- unzip(file.path(pathToDriver, driverSource$fileName), 
                                 exdir = pathToDriver,
                                 junkpaths = TRUE)
      unzipSuccess <- is.character(extractedFilename)
      
      if (unzipSuccess) {
        file.remove(file.path(pathToDriver, driverSource$fileName))
      }
    } else {
      # Jar file. Download directly to jar folder
      unzipSuccess <- TRUE
      result <- download.file(
        url = paste0(driverSource$baseUrl, driverSource$fileName),
        destfile = file.path(pathToDriver, driverSource$fileName),
        method = method
      )
    }
    if (unzipSuccess && result == 0) {
      inform(paste0("DatabaseConnector ", db, " JDBC driver downloaded to '", pathToDriver, "'."))
    } else {
      abort(paste0("Downloading and unzipping of ", db, " JDBC driver to '", pathToDriver, "' has failed."))
    } 
  }
  invisible(pathToDriver)
}

loadJdbcDriver <- function(driverClass, classPath) {
  rJava::.jaddClassPath(classPath)
  if (nchar(driverClass) && rJava::is.jnull(rJava::.jfindClass(as.character(driverClass)[1]))) {
    abort("Cannot find JDBC driver class ", driverClass)
  }
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

checkPathToDriver <- function(pathToDriver, dbms) {
  if (!is.null(dbms) && dbms %in% c("sqlite", "sqlite extended", "duckdb")) {
    return()
  }
  if (pathToDriver == "") {
    abort(paste(
      "The `pathToDriver` argument hasn't been specified.",
      "Please set the path to the location containing the JDBC driver.",
      "See `?jdbcDrivers` for instructions on downloading the drivers."
    ))
  }
  if (!dir.exists(pathToDriver)) {
    if (file.exists(pathToDriver)) {
      abort(sprintf(
        "The folder location pathToDriver = '%s' points to a file, but should point to a folder.",
        pathToDriver
      ))
    } else {
      abort(paste(
        "The folder location pathToDriver = '", pathToDriver, "' does not exist.",
        "Please set the path to the location containing the JDBC driver.",
        "See `?jdbcDrivers` for instructions on downloading the drivers."
      ))
    }
  }
}

findPathToJar <- function(name, pathToDriver) {
  checkPathToDriver(pathToDriver, NULL)
  files <- list.files(path = pathToDriver, pattern = name, full.names = TRUE)
  if (length(files) == 0) {
    abort(paste(
      sprintf("No drivers matching pattern '%s'found in folder '%s'.", name, pathToDriver),
      "\nPlease download the JDBC drivers for your database to the folder.",
      "See `?jdbcDrivers` for instructions on downloading the drivers."
    ))
  } else {
    return(files)
  }
}
