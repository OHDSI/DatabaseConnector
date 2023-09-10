# @file BulkLoad.R
#
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

checkBulkLoadCredentials <- function(connection) {
  if (dbms(connection) == "pdw" && tolower(Sys.info()["sysname"]) == "windows") {
    if (Sys.getenv("DWLOADER_PATH") == "") {
      inform("Please set environment variable DWLOADER_PATH to DWLoader binary path.")
      return(FALSE)
    }
    return(TRUE)
  } else if (dbms(connection) == "redshift") {
    envSet <- FALSE
    bucket <- FALSE

    if (Sys.getenv("AWS_ACCESS_KEY_ID") != "" && Sys.getenv("AWS_SECRET_ACCESS_KEY") != "" && Sys.getenv("AWS_BUCKET_NAME") !=
      "" && Sys.getenv("AWS_DEFAULT_REGION") != "") {
      envSet <- TRUE
    }
    rlang::check_installed("aws.s3")
    if (aws.s3::bucket_exists(bucket = Sys.getenv("AWS_BUCKET_NAME"))) {
      bucket <- TRUE
    }

    if (Sys.getenv("AWS_SSE_TYPE") == "") {
      warn("Not using Server Side Encryption for AWS S3")
    }
    return(envSet & bucket)
  } else if (dbms(connection) == "hive") {
    if (Sys.getenv("HIVE_NODE_HOST") == "") {
      inform("Please set environment variable HIVE_NODE_HOST to the Hive Node's host:port")
      return(FALSE)
    }
    if (Sys.getenv("HIVE_SSH_USER") == "") {
      warn(paste("HIVE_SSH_USER is not set, using default", getHiveSshUser()))
    }
    if (Sys.getenv("HIVE_SSH_PASSWORD") == "" && Sys.getenv("HIVE_KEYFILE") == "") {
      inform("At least one of the following environment variables: HIVE_PASSWORD and/or HIVE_KEYFILE should be set")
      return(FALSE)
    }
    if (Sys.getenv("HIVE_KEYFILE") == "") {
      warn("Using ssh password authentication, it's recommended to use keyfile instead")
    }
    return(TRUE)
  } else if (dbms(connection) == "postgresql") {
    if (Sys.getenv("POSTGRES_PATH") == "") {
      inform("Please set environment variable POSTGRES_PATH to Postgres binary path (e.g. 'C:/Program Files/PostgreSQL/11/bin'.")
      return(FALSE)
    }
    return(TRUE)
  } else {
    return(FALSE)
  }
}

countRows <- function(connection, sqlTableName) {
  sql <- "SELECT COUNT(*) FROM @table"
  count <- renderTranslateQuerySql(
    connection = connection,
    sql = sql,
    table = sqlTableName
  )
  return(count[1, 1])
}

bulkLoadRedshift <- function(connection, sqlTableName, data) {
  rlang::check_installed("R.utils")
  rlang::check_installed("aws.s3")
  logTrace(sprintf("Inserting %d rows into table '%s' using RedShift bulk load", nrow(data), sqlTableName))
  start <- Sys.time()

  csvFileName <- tempfile("redshift_insert_", fileext = ".csv")
  gzFileName <- tempfile("redshift_insert_", fileext = ".gz")
  write.csv(x = data, na = "", file = csvFileName, row.names = FALSE, quote = TRUE)
  on.exit(unlink(csvFileName))
  R.utils::gzip(filename = csvFileName, destname = gzFileName, remove = TRUE)
  on.exit(unlink(gzFileName), add = TRUE)

  s3Put <- aws.s3::put_object(
    file = gzFileName,
    check_region = FALSE,
    headers = list(`x-amz-server-side-encryption` = Sys.getenv("AWS_SSE_TYPE")),
    object = paste(Sys.getenv("AWS_OBJECT_KEY"), basename(gzFileName), sep = "/"),
    bucket = Sys.getenv("AWS_BUCKET_NAME")
  )
  if (!s3Put) {
    abort("Failed to upload data to AWS S3. Please check your credentials and access.")
  }
  on.exit(aws.s3::delete_object(
    object = paste(Sys.getenv("AWS_OBJECT_KEY"), basename(gzFileName), sep = "/"),
    bucket = Sys.getenv("AWS_BUCKET_NAME")
  ),
  add = TRUE
  )

  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "redshiftCopy.sql",
    packageName = "DatabaseConnector",
    dbms = "redshift",
    sqlTableName = sqlTableName,
    fileName = basename(gzFileName),
    s3RepoName = Sys.getenv("AWS_BUCKET_NAME"),
    pathToFiles = Sys.getenv("AWS_OBJECT_KEY"),
    awsAccessKey = Sys.getenv("AWS_ACCESS_KEY_ID"),
    awsSecretAccessKey = Sys.getenv("AWS_SECRET_ACCESS_KEY")
  )

  tryCatch(
    {
      DatabaseConnector::executeSql(connection = connection, sql = sql, reportOverallTime = FALSE)
    },
    error = function(e) {
      abort("Error in Redshift bulk upload. Please check stl_load_errors and Redshift/S3 access.")
    }
  )
  delta <- Sys.time() - start
  inform(paste("Bulk load to Redshift took", signif(delta, 3), attr(delta, "units")))
}

bulkLoadPostgres <- function(connection, sqlTableName, sqlFieldNames, sqlDataTypes, data) {
  logTrace(sprintf("Inserting %d rows into table '%s' using PostgreSQL bulk load", nrow(data), sqlTableName))
  startTime <- Sys.time()

  for (i in 1:ncol(data)) {
    if (sqlDataTypes[i] == "INT") {
      data[, i] <- format(data[, i], scientific = FALSE)
    }
  }
  csvFileName <- tempfile("pdw_insert_", fileext = ".csv")
  readr::write_excel_csv(data, csvFileName, na = "")
  on.exit(unlink(csvFileName))

  hostServerDb <- strsplit(attr(connection, "server")(), "/")[[1]]
  port <- attr(connection, "port")()
  user <- attr(connection, "user")()
  password <- attr(connection, "password")()

  if (.Platform$OS.type == "windows") {
    winPsqlPath <- Sys.getenv("POSTGRES_PATH")
    command <- file.path(winPsqlPath, "psql.exe")
    if (!file.exists(command)) {
      abort(paste("Could not find psql.exe in ", winPsqlPath))
    }
  } else {
    command <- "psql"
  }
  headers <- paste0("(", sqlFieldNames, ")")
  if (is.null(port)) {
    port <- 5432
  }

  connInfo <- sprintf("host='%s' port='%s' dbname='%s' user='%s' password='%s'", hostServerDb[[1]], port, hostServerDb[[2]], user, password)
  copyCommand <- paste(
    shQuote(command),
    "-d \"",
    connInfo,
    "\" -c \"\\copy", sqlTableName,
    headers,
    "FROM", shQuote(csvFileName),
    "NULL AS '' DELIMITER ',' CSV HEADER;\""
  )

  countBefore <- countRows(connection, sqlTableName)
  result <- base::system(copyCommand)
  countAfter <- countRows(connection, sqlTableName)

  if (result != 0) {
    abort(paste("Error while bulk uploading data, psql returned a non zero status. Status = ", result))
  }
  if (countAfter - countBefore != nrow(data)) {
    abort(paste("Something went wrong when bulk uploading. Data has", nrow(data), "rows, but table has", (countAfter - countBefore), "new records"))
  }

  delta <- Sys.time() - startTime
  inform(paste("Bulk load to PostgreSQL took", signif(delta, 3), attr(delta, "units")))
}
