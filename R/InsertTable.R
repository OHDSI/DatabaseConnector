# @file InsertTable.R
#
# Copyright 2018 Observational Health Data Sciences and Informatics
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

.sql.qescape <- function(s, identifier = FALSE, quote = "\"") {
  s <- as.character(s)
  if (identifier) {
    vid <- grep("^[A-Za-z]+([A-Za-z0-9_]*)$", s)
    if (length(s[-vid])) {
      if (is.na(quote))
        stop("The JDBC connection doesn't support quoted identifiers, but table/column name contains characters that must be quoted (",
             paste(s[-vid], collapse = ","),
             ")")
      s[-vid] <- .sql.qescape(s[-vid], FALSE, quote)
    }
    return(s)
  }
  if (is.na(quote))
    quote <- ""
  s <- gsub("\\\\", "\\\\\\\\", s)
  if (nchar(quote))
    s <- gsub(paste("\\", quote, sep = ""), paste("\\\\\\", quote, sep = ""), s, perl = TRUE)
  paste(quote, s, quote, sep = "")
}

mergeTempTables <- function(connection, tableName, varNames, sourceNames, location, distribution) {
  unionString <- paste("\nUNION ALL\nSELECT ", varNames, " FROM ", sep = "")
  valueString <- paste(sourceNames, collapse = unionString)
  sql <- paste("IF XACT_STATE() = 1 COMMIT; CREATE TABLE ",
               tableName,
               " (",
               varNames,
               " ) WITH (",
               location,
               "DISTRIBUTION=",
               distribution,
               ") AS SELECT ",
               varNames,
               " FROM ",
               valueString,
               sep = "")
  executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  
  # Drop source tables:
  for (sourceName in sourceNames) {
    sql <- paste("DROP TABLE", sourceName)
    executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  }
}

ctasHack <- function(connection, qname, tempTable, varNames, fts, data) {
  batchSize <- 1000
  mergeSize <- 300
  if (any(tolower(names(data)) == "subject_id")) {
    distribution <- "HASH(SUBJECT_ID)"
  } else if (any(tolower(names(data)) == "person_id")) {
    distribution <- "HASH(PERSON_ID)"
  } else {
    distribution <- "REPLICATE"
  }
  if (tempTable) {
    location <- "LOCATION = USER_DB, "
  } else {
    location <- ""
  }
  esc <- function(str) {
    result <- paste("'", gsub("'", "''", str), "'", sep = "")
    result[is.na(str)] <- "NULL"
    return(result)
  }
  
  # Insert data in batches in temp tables using CTAS:
  tempNames <- c()
  for (start in seq(1, nrow(data), by = batchSize)) {
    if (length(tempNames) == mergeSize) {
      mergedName <- paste("#", paste(sample(letters, 24, replace = TRUE), collapse = ""), sep = "")
      mergeTempTables(connection, mergedName, varNames, tempNames, location, distribution)
      tempNames <- c(mergedName)
    }
    # First line gets type information
    valueString <- paste(paste("CAST(",
                               sapply(data[start, , drop = FALSE], esc),
                               " AS ",
                               fts,
                               ")",
                               sep = ""), collapse = ",")
    end <- min(start + batchSize - 1, nrow(data))
    if (end == start + 1) {
      valueString <- paste(c(valueString, paste(sapply(data[start + 1, , drop = FALSE], esc),
                                                collapse = ",")), collapse = "\nUNION ALL\nSELECT ")
    } else if (end > start + 1) {
      valueString <- paste(c(valueString, apply(sapply(data[(start + 1):end, , drop = FALSE], esc),
                                                MARGIN = 1,
                                                FUN = paste,
                                                collapse = ",")), collapse = "\nUNION ALL\nSELECT ")
    }
    tempName <- paste("#", paste(sample(letters, 24, replace = TRUE), collapse = ""), sep = "")
    tempNames <- c(tempNames, tempName)
    sql <- paste("IF XACT_STATE() = 1 COMMIT; CREATE TABLE ",
                 tempName,
                 " (",
                 varNames,
                 " ) WITH (LOCATION = USER_DB, DISTRIBUTION=",
                 distribution,
                 ") AS SELECT ",
                 valueString,
                 sep = "")
    executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  }
  mergeTempTables(connection, qname, varNames, tempNames, location, distribution)
}

#' Insert a table on the server
#'
#' @description
#' This function sends the data in a data frame or ffdf to a table on the server. Either a new table
#' is created, or the data is appended to an existing table.
#'
#' @param connection          The connection to the database server.
#' @param tableName           The name of the table where the data should be inserted.
#' @param data                The data frame or ffdf containing the data to be inserted.
#' @param dropTableIfExists   Drop the table if the table already exists before writing?
#' @param createTable         Create a new table? If false, will append to existing table.
#' @param tempTable           Should the table created as a temp table?
#' @param oracleTempSchema    Specifically for Oracle, a schema with write priviliges where temp tables
#'                            can be created.
#' @param useMppBulkLoad      If using Redshift or PDW, use more performant bulk loading techniques. 
#'                            Setting the system environment variable "USE_MPP_BULK_LOAD" to TRUE is another way to enable this mode.
#'                            Please note, Redshift requires valid S3 credentials; 
#'                            PDW requires valid DWLoader installation. 
#'                            This can only be used for permanent tables, and cannot be used to append to an existing table.
#'
#' @details
#' This function sends the data in a data frame to a table on the server. Either a new table is
#' created, or the data is appended to an existing table.
#'
#' If using Redshift or PDW, bulk uploading techniques may be more performant than relying upon 
#' a batch of insert statements, depending upon data size and network throughput.
#' 
#' Redshift: The MPP bulk loading relies upon the CloudyR S3 library to test a connection to an S3 bucket using
#' AWS S3 credentials. Credentials are configured either directly into the System Environment
#' using the following keys: 
#' 
#' Sys.setenv("AWS_ACCESS_KEY_ID" = "some_access_key_id",
#'            "AWS_SECRET_ACCESS_KEY" = "some_secret_access_key",
#'            "AWS_DEFAULT_REGION" = "some_aws_region",
#'            "AWS_BUCKET_NAME" = "some_bucket_name",
#'            "AWS_OBJECT_KEY" = "some_object_key",
#'            "AWS_SSE_TYPE" = "server_side_encryption_type")
#'            
#' PDW: The MPP bulk loading relies upon the client having a Windows OS and the DWLoader exe installed,
#' and the following permissions granted:
#'   --Grant BULK Load permissions – needed at a server level
#'   USE master;
#'   GRANT ADMINISTER BULK OPERATIONS TO user;
#'   --Grant Staging database permissions – we will use the user db.
#'   USE scratch;
#'   EXEC sp_addrolemember 'db_ddladmin', user;

#' Set the R environment variable DWLOADER_PATH to the location of the binary.
#'            
#' @examples
#' \dontrun{
#' connectionDetails <- createConnectionDetails(dbms = "mysql",
#'                                              server = "localhost",
#'                                              user = "root",
#'                                              password = "blah",
#'                                              schema = "cdm_v5")
#' conn <- connect(connectionDetails)
#' data <- data.frame(x = c(1, 2, 3), y = c("a", "b", "c"))
#' insertTable(conn, "my_table", data)
#' disconnect(conn)
#' 
#' ## bulk data insert with Redshift or PDW
#' connectionDetails <- createConnectionDetails(dbms = "redshift",
#'                                              server = "localhost",
#'                                              user = "root",
#'                                              password = "blah",
#'                                              schema = "cdm_v5")
#' conn <- connect(connectionDetails)
#' data <- data.frame(x = c(1, 2, 3), y = c("a", "b", "c"))
#' insertTable(connection = connection, 
#'             tableName = "scratch.somedata", 
#'             data = data, 
#'             dropTableIfExists = TRUE, 
#'             createTable = TRUE, 
#'             tempTable = FALSE, 
#'             useMppBulkLoad = TRUE) # or, Sys.setenv("USE_MPP_BULK_LOAD" = TRUE)
#' }
#' @export
insertTable <- function(connection,
                        tableName,
                        data,
                        dropTableIfExists = TRUE,
                        createTable = TRUE,
                        tempTable = FALSE,
                        oracleTempSchema = NULL,
                        useMppBulkLoad = FALSE) 
{
  if (Sys.getenv("USE_MPP_BULK_LOAD") == "TRUE") {
    useMppBulkLoad <- TRUE
  }
  if (dropTableIfExists)
    createTable <- TRUE
  if (tempTable & substr(tableName, 1, 1) != "#")
    tableName <- paste("#", tableName, sep = "")
  if (!tempTable & substr(tableName, 1, 1) == "#") {
    tempTable <- TRUE
    warning("Temp table name detected, setting tempTable parameter to TRUE")
  }
  if (is.vector(data) && !is.list(data))
    data <- data.frame(x = data)
  if (length(data) < 1)
    stop("data must have at least one column")
  if (is.null(names(data)))
    names(data) <- paste("V", 1:length(data), sep = "")
  if (length(data[[1]]) > 0) {
    if (!is.data.frame(data))
      data <- as.data.frame(data, row.names = 1:length(data[[1]]))
  } else {
    if (!is.data.frame(data))
      data <- as.data.frame(data)
  }
  
  def <- function(obj) {
    if (is.integer(obj))
      "INTEGER" else if (is.numeric(obj))
        "FLOAT" else if (class(obj) == "Date")
          "DATE" else "VARCHAR(255)"
  }
  fts <- sapply(data[1, ], def)
  isDate <- (fts == "DATE")
  fdef <- paste(.sql.qescape(names(data), TRUE, connection$identifierQuote), fts, collapse = ",")
  qname <- .sql.qescape(tableName, TRUE, connection$identifierQuote)
  esc <- function(str) {
    paste("'", gsub("'", "''", str), "'", sep = "")
  }
  varNames <- paste(.sql.qescape(names(data), TRUE, connection$identifierQuote), collapse = ",")
  
  if (dropTableIfExists) {
    if (tempTable) {
      sql <- "IF OBJECT_ID('tempdb..@tableName', 'U') IS NOT NULL DROP TABLE @tableName;"
    } else {
      sql <- "IF OBJECT_ID('@tableName', 'U') IS NOT NULL DROP TABLE @tableName;"
    }
    sql <- SqlRender::renderSql(sql, tableName = tableName)$sql
    sql <- SqlRender::translateSql(sql,
                                   targetDialect = attr(connection, "dbms"),
                                   oracleTempSchema = oracleTempSchema)$sql
    executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  }
  
  if (createTable && !tempTable && useMppBulkLoad) {
    ensure_installed("aws.s3")
    ensure_installed("uuid")
    ensure_installed("R.utils")
    if (!.checkMppCredentials(connection)) {
      stop("MPP credentials could not be confirmed. Please review them or set 'useMppBulkLoad' to FALSE")
    }
    writeLines("Attempting to use MPP bulk loading...")
    sql <- paste("CREATE TABLE ", qname, " (", fdef, ");", sep = "")
    sql <- SqlRender::translateSql(sql,
                                   targetDialect = attr(connection, "dbms"))$sql
    executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
    
    if (attr(connection, "dbms") == "redshift") {
      .bulkLoadRedshift(connection, qname, data)
    } else if (attr(connection, "dbms") == "pdw") {
      .bulkLoadPdw(connection, qname, data)
    }
  } else {
    if (attr(connection, "dbms") == "pdw" && createTable) {
      ctasHack(connection, qname, tempTable, varNames, fts, data)
    } else {
      if (createTable) {
        sql <- paste("CREATE TABLE ", qname, " (", fdef, ");", sep = "")
        sql <- SqlRender::translateSql(sql,
                                       targetDialect = attr(connection, "dbms"),
                                       oracleTempSchema = oracleTempSchema)$sql
        executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
      }
      
      insertSql <- paste("INSERT INTO ",
                         qname,
                         " (",
                         varNames,
                         ") VALUES(",
                         paste(rep("?", length(fts)), collapse = ","),
                         ")",
                         sep = "")
      insertSql <- SqlRender::translateSql(insertSql,
                                           targetDialect = attr(connection, "dbms"),
                                           oracleTempSchema = oracleTempSchema)$sql
      
      batchSize <- 10000
      
      autoCommit <- rJava::.jcall(connection$jConnection, "Z", "getAutoCommit")
      if (autoCommit) {
        rJava::.jcall(connection$jConnection, "V", "setAutoCommit", FALSE)
        on.exit(rJava::.jcall(connection$jConnection, "V", "setAutoCommit", TRUE))
      }
      
      insertRow <- function(row, statement) {
        for (i in 1:length(row)) {
          if (is.na(row[i])) {
            rJava::.jcall(statement, "V", "setString", i, rJava::.jnull(class = "java/lang/String"))
          } else {
            rJava::.jcall(statement, "V", "setString", i, as.character(row[i]))
          }
        }
        rJava::.jcall(statement, "V", "addBatch")
      }
      insertRowPostgreSql <- function(row, statement) {
        other <- rJava::.jfield("java/sql/Types", "I", "OTHER")
        for (i in 1:length(row)) {
          if (is.na(row[i])) {
            rJava::.jcall(statement, "V", "setObject", i, rJava::.jnull(), other)
          } else {
            value <- rJava::.jnew("java/lang/String", as.character(row[i]))
            rJava::.jcall(statement,
                          "V",
                          "setObject",
                          i,
                          rJava::.jcast(value, "java/lang/Object"),
                          other)
          }
        }
        rJava::.jcall(statement, "V", "addBatch")
      }
      insertRowOracle <- function(row, statement, isDate) {
        for (i in 1:length(row)) {
          if (is.na(row[i])) {
            rJava::.jcall(statement, "V", "setString", i, rJava::.jnull(class = "java/lang/String"))
          } else if (isDate[i]) {
            date <- rJava::.jcall("java/sql/Date", "Ljava/sql/Date;", "valueOf", as.character(row[i]))
            rJava::.jcall(statement, "V", "setDate", i, date)
          } else rJava::.jcall(statement, "V", "setString", i, as.character(row[i]))
        }
        rJava::.jcall(statement, "V", "addBatch")
      }
      insertRowImpala <- function(row, statement) {
        for (i in 1:length(row)) {
          if (is.na(row[i])) {
            rJava::.jcall(statement, "V", "setString", i, rJava::.jnull(class = "java/lang/String"))
          } else if (is.integer(row[i])) {
            rJava::.jcall(statement, "V", "setInt", i, as.integer(row[i]))
          } else {
            rJava::.jcall(statement, "V", "setString", i, as.character(row[i]))
          }
        }
        rJava::.jcall(statement, "V", "addBatch")
      }
      
      for (start in seq(1, nrow(data), by = batchSize)) {
        end <- min(start + batchSize - 1, nrow(data))
        statement <- rJava::.jcall(connection$jConnection,
                                   "Ljava/sql/PreparedStatement;",
                                   "prepareStatement",
                                   insertSql,
                                   check = FALSE)
        if (attr(connection, "dbms") == "postgresql") {
          apply(data[start:end,
                     ,
                     drop = FALSE],
                statement = statement,
                MARGIN = 1,
                FUN = insertRowPostgreSql)
        } else if (attr(connection, "dbms") == "oracle" | attr(connection, "dbms") == "redshift") {
          apply(data[start:end,
                     ,
                     drop = FALSE],
                statement = statement,
                isDate = isDate,
                MARGIN = 1,
                FUN = insertRowOracle)
        } else if (attr(connection, "dbms") == "impala") {
          apply(data[start:end,
                     ,
                     drop = FALSE],
                statement = statement,
                MARGIN = 1,
                FUN = insertRowImpala)
        } else {
          apply(data[start:end, , drop = FALSE], statement = statement, MARGIN = 1, FUN = insertRow)
        }
        rJava::.jcall(statement, "[I", "executeBatch")
      }
    }
  }
}

.checkMppCredentials <- function(connection) { 
  if (attr(connection, "dbms") == "pdw" && tolower(Sys.info()["sysname"]) == "windows" ) {
    if (Sys.getenv("DWLOADER_PATH") == "") {
      writeLines("Please set environment variable DWLOADER_PATH to DWLoader binary path.")
      return (FALSE)
    }
    return (TRUE)
  } else if (attr(connection, "dbms") == "redshift") {
    envSet <- FALSE
    bucket <- FALSE
    
    if (Sys.getenv("AWS_ACCESS_KEY_ID") != "" && 
        Sys.getenv("AWS_SECRET_ACCESS_KEY") != "" &&
        Sys.getenv("AWS_BUCKET_NAME") != "" &&
        Sys.getenv("AWS_DEFAULT_REGION") != "") {
      envSet <- TRUE
    }
    
    if (aws.s3::bucket_exists(bucket = Sys.getenv("AWS_BUCKET_NAME"))) {
      bucket <- TRUE
    }
    
    if (Sys.getenv("AWS_SSE_TYPE") == "") {
      warning("Not using Server Side Encryption for AWS S3")
    }
    return(envSet & bucket)
  } else {
    return (FALSE)
  }
}

.bulkLoadPdw <- function(connection,
                         qname,
                         data) {
  start <- Sys.time()
  eol <- "\r\n"
  fileName <- sprintf("pdw_insert_%s", uuid::UUIDgenerate(use.time = TRUE))
  write.table(x = data, na = "", file = sprintf("%s.csv", fileName), row.names = FALSE, quote = FALSE, 
              col.names = TRUE, sep = "~*~")
  R.utils::gzip(filename = sprintf("%s.csv", fileName), destname = sprintf("%s.gz", fileName),
                remove = TRUE)
  
  auth <- sprintf("-U %1s -P %2s", attr(connection, "user"), attr(connection, "password"))
  if (is.null(attr(connection, "user")) && is.null(attr(connection, "password"))) {
    auth <- "-W"
  }
  
  databaseMetaData <- rJava::.jcall(connection$jConnection, "Ljava/sql/DatabaseMetaData;", "getMetaData")
  url <- rJava::.jcall(databaseMetaData, "Ljava/lang/String;", "getURL")
  pdwServer <- urltools::url_parse(url)$domain
  
  if (pdwServer == "" | is.null(pdwServer)) {
    stop("PDW Server name cannot be parsed from JDBC URL string")
  }
  
  command <- sprintf("%1s -M append -e UTF8 -i %2s -T %3s -R dwloader.txt -fh 1 -t %4s -r %5s -D ymd -E -se -rv 1 -S %6s %7s", 
                     shQuote(Sys.getenv("DWLOADER_PATH")), 
                     shQuote(sprintf("%s.gz", fileName)), 
                     qname, 
                     shQuote("~*~"), 
                     shQuote(eol),
                     pdwServer, 
                     auth)
  
  tryCatch({
    system(command, intern = FALSE,
           ignore.stdout = FALSE, ignore.stderr = FALSE,
           wait = TRUE, input = NULL, show.output.on.console = FALSE,
           minimized = FALSE, invisible = TRUE)
    delta <- Sys.time() - start
    writeLines(paste("Bulk load to PDW took", signif(delta, 3), attr(delta, "units")))
  },
  error = function (e) {
    stop("Error in PDW bulk upload. Please check dwloader.txt and dwloader.txt.reason.")
  },
  finally = {
    try(file.remove(sprintf("%s.gz", fileName)), silent = TRUE)    
  }
  )
}

.bulkLoadRedshift <- function (connection,
                               qname,
                               data) {
  start <- Sys.time()
  fileName <- sprintf("redshift_insert_%s", uuid::UUIDgenerate(use.time = TRUE))
  write.csv(x = data, na = "", file = sprintf("%s.csv", fileName),
            row.names = FALSE, quote = TRUE)
  R.utils::gzip(filename = sprintf("%s.csv", fileName), destname = sprintf("%s.gz", fileName),
                remove = TRUE)
  
  s3Put <- aws.s3::put_object(file = sprintf("%s.gz", fileName), 
                              check_region = FALSE, 
                              headers = list("x-amz-server-side-encryption" = Sys.getenv("AWS_SSE_TYPE")), 
                              object = paste(Sys.getenv("AWS_OBJECT_KEY"), fileName, sep = "/"), 
                              bucket = Sys.getenv("AWS_BUCKET_NAME"))
  
  if (!s3Put) {
    stop("Failed to upload data to AWS S3. Please check your credentials and access.")
  }
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "redshiftCopy.sql", 
                                           packageName = "DatabaseConnector", 
                                           dbms = "redshift",
                                           qname = qname,
                                           fileName = fileName,
                                           s3RepoName = Sys.getenv("AWS_BUCKET_NAME"), 
                                           pathToFiles = Sys.getenv("AWS_OBJECT_KEY"),
                                           awsAccessKey = Sys.getenv("AWS_ACCESS_KEY_ID"), 
                                           awsSecretAccessKey = Sys.getenv("AWS_SECRET_ACCESS_KEY"))
  
  tryCatch({
    DatabaseConnector::executeSql(connection = connection, sql = sql, reportOverallTime = FALSE)
    delta <- Sys.time() - start
    writeLines(paste("Bulk load to Redshift took", signif(delta, 3), attr(delta, "units")))
  }, 
  error = function(e) {
    stop("Error in Redshift bulk upload. Please check stl_load_errors and Redshift/S3 access.")
  },
  finally = {
    DatabaseConnector::disconnect(connection = connection)
    #try(file.remove(sprintf("%s.csv", fileName)), silent = TRUE)
    try(file.remove(sprintf("%s.gz", fileName)), silent = TRUE)
    try(aws.s3::delete_object(object = sprintf("%s.gz", fileName), 
                              bucket = Sys.getenv("AWS_BUCKET_NAME")), silent = TRUE)
  }
  )
}


# Borrowed from devtools: https://github.com/hadley/devtools/blob/ba7a5a4abd8258c52cb156e7b26bb4bf47a79f0b/R/utils.r#L44
is_installed <- function (pkg, version = 0) {
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
