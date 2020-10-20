# @file BulkLoad.R
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

checkBulkLoadCredentials <- function(connection) {
  if (connection@dbms == "pdw" && tolower(Sys.info()["sysname"]) == "windows") {
    if (Sys.getenv("DWLOADER_PATH") == "") {
      writeLines("Please set environment variable DWLOADER_PATH to DWLoader binary path.")
      return(FALSE)
    }
    return(TRUE)
  } else if (connection@dbms == "redshift") {
    envSet <- FALSE
    bucket <- FALSE
    
    if (Sys.getenv("AWS_ACCESS_KEY_ID") != "" && Sys.getenv("AWS_SECRET_ACCESS_KEY") != "" && Sys.getenv("AWS_BUCKET_NAME") !=
        "" && Sys.getenv("AWS_DEFAULT_REGION") != "") {
      envSet <- TRUE
    }
    ensure_installed("aws.s3")
    if (aws.s3::bucket_exists(bucket = Sys.getenv("AWS_BUCKET_NAME"))) {
      bucket <- TRUE
    }
    
    if (Sys.getenv("AWS_SSE_TYPE") == "") {
      warning("Not using Server Side Encryption for AWS S3")
    }
    return(envSet & bucket)
  } else if (connection@dbms == "hive") {
    if (Sys.getenv("HIVE_NODE_HOST") == "") {
      writeLines("Please set environment variable HIVE_NODE_HOST to the Hive Node's host:port")
      return(FALSE)
    }
    if (Sys.getenv("HIVE_SSH_USER") == "") {
      warning(paste("HIVE_SSH_USER is not set, using default", getHiveSshUser()))
    }
    if (Sys.getenv("HIVE_SSH_PASSWORD") == "" && Sys.getenv("HIVE_KEYFILE") == "") {
      writeLines("At least one of the following environment variables: HIVE_PASSWORD and/or HIVE_KEYFILE should be set")
      return(FALSE)
    }
    if (Sys.getenv("HIVE_KEYFILE") == "") {
      warning("Using ssh password authentication, it's recommended to use keyfile instead")
    }
    
    return(TRUE)
  } else {
    return(FALSE)
  }
}

getHiveSshUser <- function() {
  sshUser <- Sys.getenv("HIVE_SSH_USER")
  return(if (sshUser == "") "root" else sshUser)
}

bulkLoadPdw <- function(connection, sqlTableName, sqlDataTypes, data) {
  ensure_installed("urltools")
  start <- Sys.time()
  # Format integer fields to prevent scientific notation:
  for (i in 1:ncol(data)) {
    if (sqlDataTypes[i] == "INT") {
      data[, i] <- format(data[, i], scientific = FALSE)
    }
  }
  eol <- "\r\n"
  csvFileName <- tempfile("pdw_insert_", fileext = ".csv")
  gzFileName <- tempfile("pdw_insert_", fileext = ".gz")
  write.table(x = data,
              na = "",
              file = csvFileName,
              row.names = FALSE,
              quote = FALSE,
              col.names = TRUE,
              sep = "~*~")
  on.exit(unlink(csvFileName))
  R.utils::gzip(filename = csvFileName, destname = gzFileName, remove = TRUE)
  on.exit(unlink(gzFileName), add = TRUE)
  
  if (is.null(attr(connection, "user")()) && is.null(attr(connection, "password")())) {
    auth <- "-W"
  } else {
    auth <- sprintf("-U %1s -P %2s", attr(connection, "user")(), attr(connection, "password")())  
  }
  
  databaseMetaData <- rJava::.jcall(connection@jConnection,
                                    "Ljava/sql/DatabaseMetaData;",
                                    "getMetaData")
  url <- rJava::.jcall(databaseMetaData, "Ljava/lang/String;", "getURL")
  pdwServer <- urltools::url_parse(url)$domain
  
  if (pdwServer == "" | is.null(pdwServer)) {
    stop("PDW Server name cannot be parsed from JDBC URL string")
  }
  
  command <- sprintf("%1s -M append -e UTF8 -i %2s -T %3s -R dwloader.txt -fh 1 -t %4s -r %5s -D ymd -E -se -rv 1 -S %6s %7s",
                     shQuote(Sys.getenv("DWLOADER_PATH")),
                     shQuote(gzFileName),
                     sqlTableName,
                     shQuote("~*~"),
                     shQuote(eol),
                     pdwServer,
                     auth)
  sql <- "SELECT COUNT(*) FROM @table"
  countBefore <- renderTranslateQuerySql(connection = connection, sql  = sql, table = sqlTableName)[1, 1]
  
  tryCatch({
    system(command,
           intern = FALSE,
           ignore.stdout = FALSE,
           ignore.stderr = FALSE,
           wait = TRUE,
           input = NULL,
           show.output.on.console = FALSE,
           minimized = FALSE,
           invisible = TRUE)
    delta <- Sys.time() - start
    writeLines(paste("Bulk load to PDW took", signif(delta, 3), attr(delta, "units")))
  }, error = function(e) {
    stop("Error in PDW bulk upload. Please check dwloader.txt and dwloader.txt.reason.")
  })
  
  countAfter <- renderTranslateQuerySql(connection = connection, sql  = sql, table = sqlTableName)[1, 1]
  if (countAfter - countBefore != nrow(data)) {
    stop("Something went wrong when bulk uploading. Data has ", nrow(data), " rows, but table has ", (countAfter - countBefore), " new records")
  }
}

bulkLoadRedshift <- function(connection, sqlTableName, data) {
  ensure_installed("uuid")
  ensure_installed("R.utils")
  start <- Sys.time()
  fileName <- file.path(tempdir(), sprintf("redshift_insert_%s", uuid::UUIDgenerate(use.time = TRUE)))
  write.csv(x = data, na = "", file = sprintf("%s.csv", fileName), row.names = FALSE, quote = TRUE)
  R.utils::gzip(filename = sprintf("%s.csv",
                                   fileName), destname = sprintf("%s.gz", fileName), remove = TRUE)
  
  s3Put <- aws.s3::put_object(file = sprintf("%s.gz", fileName),
                              check_region = FALSE,
                              headers = list(`x-amz-server-side-encryption` = Sys.getenv("AWS_SSE_TYPE")),
                              object = paste(Sys.getenv("AWS_OBJECT_KEY"), fileName, sep = "/"),
                              bucket = Sys.getenv("AWS_BUCKET_NAME"))
  
  if (!s3Put) {
    stop("Failed to upload data to AWS S3. Please check your credentials and access.")
  }
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "redshiftCopy.sql",
                                           packageName = "DatabaseConnector",
                                           dbms = "redshift",
                                           sqlTableName = sqlTableName,
                                           fileName = fileName,
                                           s3RepoName = Sys.getenv("AWS_BUCKET_NAME"),
                                           pathToFiles = Sys.getenv("AWS_OBJECT_KEY"),
                                           awsAccessKey = Sys.getenv("AWS_ACCESS_KEY_ID"),
                                           awsSecretAccessKey = Sys.getenv("AWS_SECRET_ACCESS_KEY"))
  
  tryCatch({
    DatabaseConnector::executeSql(connection = connection, sql = sql, reportOverallTime = FALSE)
    delta <- Sys.time() - start
    writeLines(paste("Bulk load to Redshift took", signif(delta, 3), attr(delta, "units")))
  }, error = function(e) {
    stop("Error in Redshift bulk upload. Please check stl_load_errors and Redshift/S3 access.")
  }, finally = {
    try(file.remove(sprintf("%s.gz", fileName)), silent = TRUE)
    try(aws.s3::delete_object(object = sprintf("%s.gz", fileName),
                              bucket = Sys.getenv("AWS_BUCKET_NAME")), silent = TRUE)
  })
}

bulkLoadHive <- function(connection, sqlTableName, sqlFieldNames, data) {
  ensure_installed("uuid")
  if (tolower(Sys.info()["sysname"]) == "windows") {
    ensure_installed("ssh")
  } 
  start <- Sys.time()
  fileName <- sprintf("hive_insert_%s.csv", uuid::UUIDgenerate(use.time = TRUE))
  filePath <- file.path(tempdir(), fileName)
  write.csv(x = data, na = "", file = filePath, row.names = FALSE, quote = TRUE)
  
  hiveUser <- getHiveSshUser()
  hivePasswd <- Sys.getenv("HIVE_SSH_PASSWORD")
  hiveHost <- Sys.getenv("HIVE_NODE_HOST")
  sshPort <- (function(port) if (port == "") "2222" else port)(Sys.getenv("HIVE_SSH_PORT"))
  nodePort <- (function(port) if (port == "") "8020" else port)(Sys.getenv("HIVE_NODE_PORT"))
  hiveKeyFile <- (function(keyfile) if (keyfile == "") NULL else keyfile)(Sys.getenv("HIVE_KEYFILE"))
  hadoopUser <- (function(hadoopUser) if (hadoopUser == "") "hive" else hadoopUser)(Sys.getenv("HADOOP_USER_NAME"))
  
  tryCatch({
    if (tolower(Sys.info()["sysname"]) == "windows") {
      session <- ssh::ssh_connect(host = sprintf("%s@%s:%s", hiveUser, hiveHost, sshPort), passwd = hivePasswd, keyfile = hiveKeyFile)
      remoteFile <- paste0("/tmp/", fileName)
      ssh::scp_upload(session, filePath, to = remoteFile, verbose = FALSE)
      hadoopDir <- sprintf("/user/%s/%s", hadoopUser, uuid::UUIDgenerate(use.time = TRUE))
      hadoopFile <- paste0(hadoopDir, "/", fileName)
      ssh::ssh_exec_wait(session, sprintf("HADOOP_USER_NAME=%s hadoop fs -mkdir %s", hadoopUser, hadoopDir))
      command <- sprintf("HADOOP_USER_NAME=%s hadoop fs -put %s %s", hadoopUser, remoteFile, hadoopFile)
      ssh::ssh_exec_wait(session, command = command)
    } else {
      remoteFile <- paste0("/tmp/", fileName)
      scp_command <- sprintf("sshpass -p \'%s\' scp -P %s %s %s:%s", hivePasswd, sshPort, filePath, hiveHost, remoteFile)
      system(scp_command)
      hadoopDir <- sprintf("/user/%s/%s", hadoopUser, uuid::UUIDgenerate(use.time = TRUE))
      hadoopFile <- paste0(hadoopDir, "/", fileName)
      hdp_mk_dir_command <- sprintf("sshpass -p \'%s\' ssh %s -p %s HADOOP_USER_NAME=%s hadoop fs -mkdir %s", hivePasswd, hiveHost, sshPort, hadoopUser, hadoopDir)
      system(hdp_mk_dir_command)
      hdp_put_command <- sprintf("sshpass -p \'%s\' ssh %s -p %s HADOOP_USER_NAME=%s hadoop fs -put %s %s", hivePasswd, hiveHost, sshPort, hadoopUser, remoteFile, hadoopFile)
      system(hdp_put_command)
    }
    def <- function(name) {
      return(paste(name, "STRING"))
    }
    fdef <- paste(sapply(sqlFieldNames, def), collapse = ", ")
    sql <- SqlRender::render("CREATE TABLE @table(@fdef) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION 'hdfs://@hiveHost:@nodePort@filename';", 
                             filename = hadoopDir, table = sqlTableName, fdef = fdef, hiveHost = hiveHost, nodePort = nodePort)
    sql <- SqlRender::translate(sql, targetDialect = "hive", oracleTempSchema = NULL)
    
    tryCatch({
      DatabaseConnector::executeSql(connection = connection, sql = sql, reportOverallTime = FALSE)
      delta <- Sys.time() - start
      writeLines(paste("Bulk load to Hive took", signif(delta, 3), attr(delta, "units")))
    }, error = function(e) {
      writeLines(paste("Error:", e))
      stop("Error in Hive bulk upload.")
    }, finally = {
      try(invisible(file.remove(filePath)))
    })
  }, finally = {
    if (tolower(Sys.info()["sysname"]) == "windows")
      ssh::ssh_disconnect(session)
  })
}

# Borrowed from devtools:
# https://github.com/hadley/devtools/blob/ba7a5a4abd8258c52cb156e7b26bb4bf47a79f0b/R/utils.r#L44
is_installed <- function(pkg, version = 0) {
  installed_version <- tryCatch(utils::packageVersion(pkg), error = function(e) NA)
  !is.na(installed_version) && installed_version >= version
}

# Borrowed and adapted from devtools:
# https://github.com/hadley/devtools/blob/ba7a5a4abd8258c52cb156e7b26bb4bf47a79f0b/R/utils.r#L74
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