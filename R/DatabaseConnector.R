# @file DatabaseConnector.R
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

#' @keywords internal
"_PACKAGE"

#' @importFrom utils sessionInfo setTxtProgressBar txtProgressBar object.size write.csv write.table read.csv install.packages menu download.file unzip packageVersion
#' @importFrom bit64 integer64
#' @importFrom rlang warn abort inform
NULL

.onLoad <- function(libname, pkgname) {
  rJava::.jpackage(pkgname, jars = "DatabaseConnector.jar", lib.loc = libname)
}

#' @name jdbcDrivers
#'
#' @title
#' How to download and use JDBC drivers for the various data platforms.
#'
#' @description
#' Below are instructions for downloading JDBC drivers for the various data platforms. Once downloaded
#' use the \code{pathToDriver} argument in the \code{\link{connect}} or \code{\link{createConnectionDetails}}
#' functions to point to the driver. Alternatively, you can set the 'DATABASECONNECTOR_JAR_FOLDER' environmental
#' variable, for example in your .Renviron file (recommended).
#'
#' @section
#' SQL Server, Oracle, PostgreSQL, PDW, Snowflake, Spark RedShift, Azure Synapse: Use the \code{\link{downloadJdbcDrivers}} function to download these drivers
#' from the OHDSI GitHub pages.
#'
#' @section
#' Netezza: Read the instructions
#' \href{https://www.ibm.com/docs/en/SSULQD_7.2.1/com.ibm.nz.datacon.doc/t_datacon_setup_JDBC.html}{here}
#' on how to obtain the Netezza JDBC driver.
#'
#' @section
#' BigQuery: Go to \href{https://cloud.google.com/bigquery/docs/reference/odbc-jdbc-drivers}{Google's site} and
#' download the latest JDBC driver. Unzip the file, and locate the appropriate jar files.
#'
#' @section
#' Impala: Go to
#' \href{https://www.cloudera.com/downloads/connectors/impala/jdbc/2-5-5.html}{Cloudera's site}, pick
#' your OS version, and click "GET IT NOW!'. Register, and you should be able to download the driver.
#'
#' @section
#' SQLite: For SQLite we actually don't use a JDBC driver. Instead, we use the RSQLite package, which can be installed
#' using \code{install.packages("RSQLite")}.
#'
NULL

# Borrowed from devtools: https://github.com/hadley/devtools/blob/ba7a5a4abd8258c52cb156e7b26bb4bf47a79f0b/R/utils.r#L44
is_installed <- function(pkg, version = 0) {
  installed_version <- tryCatch(utils::packageVersion(pkg),
    error = function(e) NA
  )
  !is.na(installed_version) && installed_version >= version
}

# Borrowed and adapted from devtools: https://github.com/hadley/devtools/blob/ba7a5a4abd8258c52cb156e7b26bb4bf47a79f0b/R/utils.r#L74
ensure_installed <- function(pkg) {
  if (!is_installed(pkg)) {
    msg <- paste0(sQuote(pkg), " must be installed for this functionality.")
    if (interactive()) {
      inform(paste(msg, "Would you like to install it?", sep = "\n"))
      if (menu(c("Yes", "No")) == 1) {
        install.packages(pkg)
      } else {
        stop(msg, call. = FALSE)
      }
    } else {
      stop(msg, call. = FALSE)
    }
  }
}
