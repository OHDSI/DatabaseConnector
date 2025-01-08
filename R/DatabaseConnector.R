# @file DatabaseConnector.R
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

#' @keywords internal
"_PACKAGE"

#' @importFrom utils sessionInfo setTxtProgressBar txtProgressBar object.size write.csv write.table read.csv install.packages menu download.file unzip packageVersion
#' @importFrom bit64 integer64
#' @importFrom rlang warn abort inform
NULL

.onLoad <- function(libname, pkgname) {
  rJava::.jpackage(pkgname, jars = "DatabaseConnector.jar", lib.loc = libname)
  
  # Verify checksum of JAR:
  storedChecksum <- scan(
    file = system.file("csv", "jarChecksum.txt", package = "DatabaseConnector"),
    what = character(), quiet = TRUE
  )
  computedChecksum <- tryCatch(rJava::J("org.ohdsi.databaseConnector.JarChecksum", "computeJarChecksum"),
                               error = function(e) {
                                 warning("Problem connecting to Java. This is normal when runing roxygen.")
                                 return("")
                               }
  )
  if (computedChecksum != "" && (storedChecksum != computedChecksum)) {
    warning("Java library version does not match R package version! Please try reinstalling the DatabaseConnector package.
            Make sure to close all instances of R, and open only one instance before reinstalling. Also make sure your
            R workspace is not reloaded on startup. Delete your .Rdata file if necessary")
  }
}

#' @name jdbcDrivers
#'
#' @title
#' How to download and use JDBC drivers for the various data platforms.
#'
#' @description
#' Below are instructions for downloading JDBC drivers for the various data platforms. Once downloaded
#' use the `pathToDriver` argument in the [connect()] or [createConnectionDetails()]
#' functions to point to the driver. Alternatively, you can set the 'DATABASECONNECTOR_JAR_FOLDER' environmental
#' variable, for example in your .Renviron file (recommended).
#'
#' # SQL Server, Oracle, PostgreSQL, PDW, Snowflake, Spark, RedShift, Azure Synapse, BigQuery
#' 
#' Use the [downloadJdbcDrivers()] function to download these drivers from the OHDSI GitHub pages.
#'
#' # Netezza
#' 
#' Read the instructions
#' [here](https://www.ibm.com/docs/en/SSULQD_7.2.1/com.ibm.nz.datacon.doc/t_datacon_setup_JDBC.html)
#' on how to obtain the Netezza JDBC driver.
#'
#' # Impala
#' 
#' Go to [Cloudera's site](https://www.cloudera.com/downloads/connectors/impala/jdbc/2-5-5.html), pick
#' your OS version, and click "GET IT NOW!'. Register, and you should be able to download the driver.
#'
#' # SQLite
#' 
#' For SQLite we actually don't use a JDBC driver. Instead, we use the RSQLite package, which can be installed
#' using `install.packages("RSQLite")`.
#'
NULL

globalVars <- new.env()
