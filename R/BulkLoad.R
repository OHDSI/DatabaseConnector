# @file BulkLoad.R
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
    ensure_installed("aws.s3")
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
  } else if (dbms(connection) == "spark") {
    envSet <- FALSE
    container <- FALSE
    
    if (Sys.getenv("AZR_STORAGE_ACCOUNT") != "" && Sys.getenv("AZR_ACCOUNT_KEY") != "" && Sys.setenv("AZR_CONTAINER_NAME") != "") {
      envSet <- TRUE
    }
    
    # List storage containers to confirm the container
    # specified in the configuration exists
    ensure_installed("AzureStor")
    azureEndpoint <- getAzureEndpoint()
    containerList <- getAzureContainerNames(azureEndpoint)
    
    if (Sys.getenv("AZR_CONTAINER_NAME") %in% containerList) {
      container <- TRUE
    }
    
    return(envSet & container)
  } else {
    return(FALSE)
  }
}

getHiveSshUser <- function() {
  sshUser <- Sys.getenv("HIVE_SSH_USER")
  return(if (sshUser == "") "root" else sshUser)
}

getAzureEndpoint <- function() {
  azureEndpoint <- AzureStor::storage_endpoint(
    paste0("https://", Sys.getenv("AZR_STORAGE_ACCOUNT"), ".dfs.core.windows.net"),
    key = Sys.getenv("AZR_ACCOUNT_KEY")
  )
  return(azureEndpoint)  
}

getAzureContainerNames <- function(azureEndpoint) {
  return(names(AzureStor::list_storage_containers(azureEndpoint)))
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

bulkLoadPdw <- function(connection, sqlTableName, sqlDataTypes, data) {
  ensure_installed("urltools")
  logTrace(sprintf("Inserting %d rows into table '%s' using PDW bulk load", nrow(data), sqlTableName))
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
  write.table(
    x = data,
    na = "",
    file = csvFileName,
    row.names = FALSE,
    quote = FALSE,
    col.names = TRUE,
    sep = "~*~"
  )
  on.exit(unlink(csvFileName))
  R.utils::gzip(filename = csvFileName, destname = gzFileName, remove = TRUE)
  on.exit(unlink(gzFileName), add = TRUE)

  if (is.null(attr(connection, "user")()) && is.null(attr(connection, "password")())) {
    auth <- "-W"
  } else {
    auth <- sprintf("-U %1s -P %2s", attr(connection, "user")(), attr(connection, "password")())
  }

  databaseMetaData <- rJava::.jcall(
    connection@jConnection,
    "Ljava/sql/DatabaseMetaData;",
    "getMetaData"
  )
  url <- rJava::.jcall(databaseMetaData, "Ljava/lang/String;", "getURL")
  pdwServer <- urltools::url_parse(url)$domain

  if (pdwServer == "" | is.null(pdwServer)) {
    abort("PDW Server name cannot be parsed from JDBC URL string")
  }

  command <- sprintf(
    "%1s -M append -e UTF8 -i %2s -T %3s -R dwloader.txt -fh 1 -t %4s -r %5s -D ymd -E -se -rv 1 -S %6s %7s",
    shQuote(Sys.getenv("DWLOADER_PATH")),
    shQuote(gzFileName),
    sqlTableName,
    shQuote("~*~"),
    shQuote(eol),
    pdwServer,
    auth
  )
  countBefore <- countRows(connection, sqlTableName)
  tryCatch(
    {
      system(command,
        intern = FALSE,
        ignore.stdout = FALSE,
        ignore.stderr = FALSE,
        wait = TRUE,
        input = NULL,
        show.output.on.console = FALSE,
        minimized = FALSE,
        invisible = TRUE
      )
      delta <- Sys.time() - start
      inform(paste("Bulk load to PDW took", signif(delta, 3), attr(delta, "units")))
    },
    error = function(e) {
      abort("Error in PDW bulk upload. Please check dwloader.txt and dwloader.txt.reason.")
    }
  )
  countAfter <- countRows(connection, sqlTableName)

  if (countAfter - countBefore != nrow(data)) {
    abort(paste("Something went wrong when bulk uploading. Data has", nrow(data), "rows, but table has", (countAfter - countBefore), "new records"))
  }
}

bulkLoadRedshift <- function(connection, sqlTableName, data) {
  ensure_installed("R.utils")
  ensure_installed("aws.s3")
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

bulkLoadHive <- function(connection, sqlTableName, sqlFieldNames, data) {
  sqlFieldNames <- strsplit(sqlFieldNames, ",")[[1]]
  if (tolower(Sys.info()["sysname"]) == "windows") {
    ensure_installed("ssh")
  }
  logTrace(sprintf("Inserting %d rows into table '%s' using Hive bulk load", nrow(data), sqlTableName))
  start <- Sys.time()
  csvFileName <- tempfile("hive_insert_", fileext = ".csv")
  write.csv(x = data, na = "", file = csvFileName, row.names = FALSE, quote = TRUE)
  on.exit(unlink(csvFileName))

  hiveUser <- getHiveSshUser()
  hivePasswd <- Sys.getenv("HIVE_SSH_PASSWORD")
  hiveHost <- Sys.getenv("HIVE_NODE_HOST")
  sshPort <- (function(port) if (port == "") "2222" else port)(Sys.getenv("HIVE_SSH_PORT"))
  nodePort <- (function(port) if (port == "") "8020" else port)(Sys.getenv("HIVE_NODE_PORT"))
  hiveKeyFile <- (function(keyfile) if (keyfile == "") NULL else keyfile)(Sys.getenv("HIVE_KEYFILE"))
  hadoopUser <- (function(hadoopUser) if (hadoopUser == "") "hive" else hadoopUser)(Sys.getenv("HADOOP_USER_NAME"))

  tryCatch(
    {
      if (tolower(Sys.info()["sysname"]) == "windows") {
        session <- ssh::ssh_connect(host = sprintf("%s@%s:%s", hiveUser, hiveHost, sshPort), passwd = hivePasswd, keyfile = hiveKeyFile)
        remoteFile <- paste0("/tmp/", basename(csvFileName))
        ssh::scp_upload(session, csvFileName, to = remoteFile, verbose = FALSE)
        hadoopDir <- sprintf("/user/%s/%s", hadoopUser, generateRandomString(30))
        hadoopFile <- paste0(hadoopDir, "/", basename(csvFileName))
        ssh::ssh_exec_wait(session, sprintf("HADOOP_USER_NAME=%s hadoop fs -mkdir %s", hadoopUser, hadoopDir))
        command <- sprintf("HADOOP_USER_NAME=%s hadoop fs -put %s %s", hadoopUser, remoteFile, hadoopFile)
        ssh::ssh_exec_wait(session, command = command)
      } else {
        remoteFile <- paste0("/tmp/", basename(csvFileName))
        scp_command <- sprintf("sshpass -p \'%s\' scp -P %s %s %s:%s", hivePasswd, sshPort, csvFileName, hiveHost, remoteFile)
        system(scp_command)
        hadoopDir <- sprintf("/user/%s/%s", hadoopUser, generateRandomString(30))
        hadoopFile <- paste0(hadoopDir, "/", basename(csvFileName))
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
        filename = hadoopDir, table = sqlTableName, fdef = fdef, hiveHost = hiveHost, nodePort = nodePort
      )
      sql <- SqlRender::translate(sql, targetDialect = "hive", tempEmulationSchema = NULL)

      tryCatch(
        {
          DatabaseConnector::executeSql(connection = connection, sql = sql, reportOverallTime = FALSE)
          delta <- Sys.time() - start
          inform(paste("Bulk load to Hive took", signif(delta, 3), attr(delta, "units")))
        },
        error = function(e) {
          abort(paste("Error in Hive bulk upload: ", e$message))
        }
      )
    },
    finally = {
      if (tolower(Sys.info()["sysname"]) == "windows") {
        ssh::ssh_disconnect(session)
      }
    }
  )
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

  server <- attr(connection, "server")()
  if (is.null(server)) {
    # taken directly from DatabaseConnector R/RStudio.R - getServer.default, could an attr too?
    databaseMetaData <- rJava::.jcall(
      connection@jConnection,
      "Ljava/sql/DatabaseMetaData;",
      "getMetaData"
    )
    server <- rJava::.jcall(databaseMetaData, "Ljava/lang/String;", "getURL")
    server <- strsplit(server, "//")[[1]][2]
  }

  hostServerDb <- strsplit(server, "/")[[1]]
  port <- attr(connection, "port")()
  user <- attr(connection, "user")()
  password <- attr(connection, "password")()

  if (.Platform$OS.type == "windows") {
    command <- file.path(Sys.getenv("POSTGRES_PATH"), "psql.exe")
  } else {
    command <- file.path(Sys.getenv("POSTGRES_PATH"), "psql")
  }
  if (!file.exists(command)) {
    abort(paste("Could not find psql.exe in ", Sys.getenv("POSTGRES_PATH")))
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

bulkLoadSpark <- function(connection, sqlTableName, data) {
  ensure_installed("AzureStor")
  logTrace(sprintf("Inserting %d rows into table '%s' using DataBricks bulk load", nrow(data), sqlTableName))
  start <- Sys.time()
  
  csvFileName <- tempfile("spark_insert_", fileext = ".csv")
  write.csv(x = data, na = "", file = csvFileName, row.names = FALSE, quote = TRUE)
  on.exit(unlink(csvFileName))

  azureEndpoint <- getAzureEndpoint()
  containers <- AzureStor::list_storage_containers(azureEndpoint)
  targetContainer <- containers[[Sys.getenv("AZR_CONTAINER_NAME")]]
  AzureStor::storage_upload(
    targetContainer, 
    src=csvFileName, 
    dest=csvFileName
  )  

  on.exit(
    AzureStor::delete_storage_file(
      targetContainer, 
      file = csvFileName,
      confirm = FALSE
    ),
    add = TRUE
  )
  
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "sparkCopy.sql",
    packageName = "DatabaseConnector",
    dbms = "spark",
    sqlTableName = sqlTableName,
    fileName = basename(csvFileName),
    azureAccountKey = Sys.getenv("AZR_ACCOUNT_KEY"),
    azureStorageAccount = Sys.getenv("AZR_STORAGE_ACCOUNT")
  )
  
  tryCatch(
    {
      DatabaseConnector::executeSql(connection = connection, sql = sql, reportOverallTime = FALSE)
    },
    error = function(e) {
      abort("Error in DataBricks bulk upload. Please check DataBricks/Azure Storage access.")
    }
  )
  delta <- Sys.time() - start
  inform(paste("Bulk load to DataBricks took", signif(delta, 3), attr(delta, "units")))
}

