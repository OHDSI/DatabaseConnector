# @file DatabaseConnector.R
#
# Copyright 2020 Observational Health Data Sciences and Informatics
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

#' @importFrom utils sessionInfo setTxtProgressBar txtProgressBar object.size write.csv write.table read.csv
#' install.packages menu
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
#' functions to point to the driver.
#' 
#' @section
#' PostgresSql: Go to \href{https://jdbc.postgresql.org/download.html}{the PostgresSQL JDBC site} and
#' download the current version. The file is called something like 'postgresql-42.2.2.jar'.
#'
#' @section
#' Oracle: Go to
#' \href{http://www.oracle.com/technetwork/database/features/jdbc/jdbc-drivers-12c-download-1958347.html}{the
#' Oracle JDBC site}. Select 'Accept License Agreement' and download the jar file. The file is called
#' something like 'ojdbc7.jar'.
#'
#' @section
#' SQL Server and PDW: Go to \href{https://www.microsoft.com/en-us/download/details.aspx?id=11774}{the
#' Microsoft SQL Server JDBC site}, click 'Download' and select the tar.gz file. Click 'Next' to start
#' the download. Decompress the file and find a file called something like 'sqljdbc41.jar' in the a
#' folder named something like 'sqljdbc_6.0/enu/jre7'.
#'
#' @section
#' RedShift: Go to the
#' \href{https://docs.aws.amazon.com/redshift/latest/mgmt/configure-jdbc-connection.html#download-jdbc-driver}{Amazon
#' RedShfit JDBC driver page} and download the latest JDBC driver. The file is called something like
#' 'RedshiftJDBC42-1.2.12.1017.jar'.
#'
#' @section
#' Netezza: Read the instructions
#' \href{https://www.ibm.com/support/knowledgecenter/en/SSULQD_7.2.1/com.ibm.nz.datacon.doc/t_datacon_setup_JDBC.html}{here}
#' on how to obtain the Netezza JDBC driver.
#'
#' @section
#' BigQuery: Go to \href{https://cloud.google.com/bigquery/partners/simba-drivers/}{Google's site} and
#' download the latest JDBC driver. Unzip the file, and locate the appropriate jar files.
#'
#' @section
#' Impala: Go to
#' \href{https://www.cloudera.com/downloads/connectors/impala/jdbc/2-5-5.html}{Cloudera's site}, pick
#' your OS version, and click "GET IT NOW!'. Register, and you should be able to download the driver.
#' 
NULL

# Borrowed from devtools: https://github.com/hadley/devtools/blob/ba7a5a4abd8258c52cb156e7b26bb4bf47a79f0b/R/utils.r#L44
is_installed <- function(pkg, version = 0) {
  installed_version <- tryCatch(utils::packageVersion(pkg), 
                                error = function(e) NA)
  !is.na(installed_version) && installed_version >= version
}

# Borrowed and adapted from devtools: https://github.com/hadley/devtools/blob/ba7a5a4abd8258c52cb156e7b26bb4bf47a79f0b/R/utils.r#L74
ensure_installed <- function(pkg) {
  if (!is_installed(pkg)) {
    msg <- paste0(sQuote(pkg), " must be installed for this functionality.")
    if (interactive()) {
      message(msg, "\nWould you like to install it?")
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
