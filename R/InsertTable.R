# @file InsertTable.R
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

mergeTempTables <- function(connection, tableName, varNames, sourceNames, distribution, oracleTempSchema) {
  unionString <- paste("\nUNION ALL\nSELECT ", varNames, " FROM ", sep = "")
  valueString <- paste(sourceNames, collapse = unionString)
  sql <- paste(distribution,
               "\n",
               "SELECT ",
               varNames,
               " INTO ",
               tableName,
               " FROM ",
               valueString,
               ";",
               sep = "")
  sql <- SqlRender::translate(sql, targetDialect = connection@dbms, oracleTempSchema = oracleTempSchema)
  executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  
  # Drop source tables:
  for (sourceName in sourceNames) {
    sql <- paste("DROP TABLE", sourceName)
    sql <- SqlRender::translate(sql, targetDialect = connection@dbms, oracleTempSchema = oracleTempSchema)
    executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  }
}

toStrings <- function(data, fts) {
  bigIntIdx <- fts == "BIGINT"
  if (nrow(data) == 1) {
    result <- sapply(data, as.character)
    if (any(bigIntIdx)) {
      result[bigIntIdx] <- sapply(data[, bigIntIdx], format, scientific = FALSE)
    }
    result <- paste("'", gsub("'", "''", result), "'", sep = "")
    result[is.na(data)] <- "NULL"
    return(as.data.frame(t(result), stringsAsFactors = FALSE))
  } else {
    result <- sapply(data, as.character)
    if (any(bigIntIdx)) {
      result[ ,bigIntIdx] <- sapply(data[ ,bigIntIdx], format, scientific = FALSE)
    }
    result <- apply(result, FUN = function(x) paste("'", gsub("'", "''", x), "'", sep = ""), MARGIN = 2)
    result[is.na(data)] <- "NULL"
    return(result)
  }
}

castValues <- function(values, fts) {
  paste("CAST(",
        values,
        " AS ",
        fts,
        ")",
        sep = "")
}

formatRow <- function(data, aliases = c(), castValues, fts) {
  if (castValues) {
    data <- castValues(data, fts)
  }
  return(paste(data, aliases, collapse = ","))
}

ctasHack <- function(connection, qname, tempTable, varNames, fts, data, progressBar, oracleTempSchema) {
  if (attr(connection, "dbms") == "hive") {
    batchSize <- 750
  } else {
    batchSize <- 1000
  }
  mergeSize <- 300
  
  if (any(tolower(names(data)) == "subject_id")) {
    distribution <- "--HINT DISTRIBUTE_ON_KEY(SUBJECT_ID)\n"
  } else if (any(tolower(names(data)) == "person_id")) {
    distribution <- "--HINT DISTRIBUTE_ON_KEY(PERSON_ID)\n"
  } else {
    distribution <- ""
  }

  # Insert data in batches in temp tables using CTAS:
  if (progressBar) {
    pb <- txtProgressBar(style = 3)
  }
  tempNames <- c()
  for (start in seq(1, nrow(data), by = batchSize)) {
    if (progressBar) {
      setTxtProgressBar(pb, start/nrow(data))
    }
    if (length(tempNames) == mergeSize) {
      mergedName <- paste("#", paste(sample(letters, 20, replace = TRUE), collapse = ""), sep = "")
      mergeTempTables(connection, mergedName, varNames, tempNames, distribution, oracleTempSchema)
      tempNames <- c(mergedName)
    }
    end <- min(start + batchSize - 1, nrow(data))
    batch <- toStrings(data[start:end, , drop = FALSE], fts)

    varAliases <- strsplit(varNames, ",")[[1]]
    # First line gets type information:
    valueString <- formatRow(batch[1, , drop = FALSE], varAliases, castValues = TRUE, fts = fts)
    if (end > start) {
      # Other lines only get type information if BigQuery:
      valueString <- paste(c(valueString, apply(batch[2:nrow(batch), , drop = FALSE],
                                                MARGIN = 1,
                                                FUN = formatRow,
                                                aliases = varAliases,
                                                castValues = attr(connection, "dbms") %in% c("bigquery", "hive"),
                                                fts = fts)), 
                           collapse = "\nUNION ALL\nSELECT ")
    }
    tempName <- paste("#", paste(sample(letters, 20, replace = TRUE), collapse = ""), sep = "")
    tempNames <- c(tempNames, tempName)
    sql <- paste(distribution,
                 "WITH data (",
                 varNames,
                 ") AS (SELECT ",
                 valueString,
                 " ) SELECT ",
                 varNames,
                 " INTO ",
                 tempName,
                 " FROM data;",
                 sep = "")
    sql <- SqlRender::translate(sql, targetDialect = connection@dbms, oracleTempSchema = oracleTempSchema)
    executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  }
  if (progressBar) {
    setTxtProgressBar(pb, 1)
    close(pb)
  }
  mergeTempTables(connection, qname, varNames, tempNames, distribution, oracleTempSchema)
}

is.bigint <- function(x) {
  num <- 2^63
  
  bigint.min <- -num
  bigint.max <- num - 1
  
  return(!all(is.na(x)) && is.numeric(x) && !is.factor(x) && all(x == round(x), na.rm = TRUE) &&  all(x >= bigint.min, na.rm = TRUE) && all(x <= bigint.max, na.rm = TRUE))
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
#'                            Setting the system environment variable "USE_MPP_BULK_LOAD" to TRUE is
#'                            another way to enable this mode. Please note, Redshift requires valid S3
#'                            credentials; PDW requires valid DWLoader installation. This can only be
#'                            used for permanent tables, and cannot be used to append to an existing
#'                            table.
#' @param progressBar         Show a progress bar when uploading?
#' @param camelCaseToSnakeCase If TRUE, the data frame column names are assumed to use camelCase and
#'                             are converted to snake_case before uploading.
#'
#' @details
#' This function sends the data in a data frame to a table on the server. Either a new table is
#' created, or the data is appended to an existing table. NA values are inserted as null values in the
#' database. If using Redshift or PDW, bulk uploading
#' techniques may be more performant than relying upon a batch of insert statements, depending upon
#' data size and network throughput. Redshift: The MPP bulk loading relies upon the CloudyR S3 library
#' to test a connection to an S3 bucket using AWS S3 credentials. Credentials are configured either
#' directly into the System Environment using the following keys: Sys.setenv("AWS_ACCESS_KEY_ID" =
#' "some_access_key_id", "AWS_SECRET_ACCESS_KEY" = "some_secret_access_key", "AWS_DEFAULT_REGION" =
#' "some_aws_region", "AWS_BUCKET_NAME" = "some_bucket_name", "AWS_OBJECT_KEY" = "some_object_key",
#' "AWS_SSE_TYPE" = "server_side_encryption_type") PDW: The MPP bulk loading relies upon the client
#' having a Windows OS and the DWLoader exe installed, and the following permissions granted: --Grant
#' BULK Load permissions - needed at a server level USE master; GRANT ADMINISTER BULK OPERATIONS TO
#' user; --Grant Staging database permissions - we will use the user db. USE scratch; EXEC
#' sp_addrolemember 'db_ddladmin', user;

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
#'             useMppBulkLoad = TRUE)  # or, Sys.setenv('USE_MPP_BULK_LOAD' = TRUE)
#' }
#' @export
insertTable <- function(connection,
                        tableName,
                        data,
                        dropTableIfExists = TRUE,
                        createTable = TRUE,
                        tempTable = FALSE,
                        oracleTempSchema = NULL,
                        useMppBulkLoad = FALSE,
                        progressBar = FALSE,
                        camelCaseToSnakeCase = FALSE) {
  UseMethod("insertTable", connection)
}

#' @export
insertTable.default <- function(connection,
                                tableName,
                                data,
                                dropTableIfExists = TRUE,
                                createTable = TRUE,
                                tempTable = FALSE,
                                oracleTempSchema = NULL,
                                useMppBulkLoad = FALSE,
                                progressBar = FALSE,
                                camelCaseToSnakeCase = FALSE) {
  if (camelCaseToSnakeCase) {
    colnames(data) <- SqlRender::camelCaseToSnakeCase(colnames(data))
  }
  if (!tempTable & substr(tableName, 1, 1) == "#") {
    tempTable <- TRUE
    warning("Temp table name detected, setting tempTable parameter to TRUE")
  }
  if (toupper(Sys.getenv("USE_MPP_BULK_LOAD")) == "TRUE") {
    useMppBulkLoad <- TRUE
  }
  if (dropTableIfExists)
    createTable <- TRUE
  if (tempTable & substr(tableName, 1, 1) != "#" & attr(connection, "dbms") != "redshift")
    tableName <- paste("#", tableName, sep = "")
  
  
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
    if (is.integer(obj)) {
      return("INTEGER")
    } else if (identical(class(obj), c("POSIXct", "POSIXt"))) {
      return("DATETIME2")
    } else if (class(obj) == "Date") {
      return("DATE")
    } else if (is.bigint(obj)) {
      return("BIGINT")
    } else if (is.numeric(obj)) {
      return("FLOAT")
    } else {
      if (is.factor(obj)) {
        maxLength <- max(nchar(levels(obj)), na.rm = TRUE)
      } else {
        maxLength <- max(nchar(as.character(obj)), na.rm = TRUE)
      }
      if (is.na(maxLength) || maxLength <= 255) {
        return("VARCHAR(255)")
      } else {
        return(sprintf("VARCHAR(%s)", maxLength))
      }
    }
  }
  fts <- sapply(data, def)
  fdef <- paste(.sql.qescape(names(data), TRUE, connection@identifierQuote), fts, collapse = ",")
  qname <- .sql.qescape(tableName, TRUE, connection@identifierQuote)
  varNames <- paste(.sql.qescape(names(data), TRUE, connection@identifierQuote), collapse = ",")

  if (dropTableIfExists) {
    if (tempTable) {
      sql <- "IF OBJECT_ID('tempdb..@tableName', 'U') IS NOT NULL DROP TABLE @tableName;"
    } else {
      sql <- "IF OBJECT_ID('@tableName', 'U') IS NOT NULL DROP TABLE @tableName;"
    }
    sql <- SqlRender::render(sql, tableName = tableName)
    sql <- SqlRender::translate(sql,
                                targetDialect = attr(connection, "dbms"),
                                oracleTempSchema = oracleTempSchema)
    executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  }
  
  if (createTable && (!tempTable || attr(connection, "dbms") == "hive") && useMppBulkLoad) {
    ensure_installed("uuid")
    ensure_installed("R.utils")
    ensure_installed("urltools")
    if (!.checkMppCredentials(connection)) {
      stop("MPP credentials could not be confirmed. Please review them or set 'useMppBulkLoad' to FALSE")
    }
    writeLines("Attempting to use MPP bulk loading...")
    dbms <- attr(connection, "dbms")

    if (dbms != "hive") {
      sql <- paste("CREATE TABLE ", qname, " (", fdef, ");", sep = "")
      sql <- SqlRender::translate(sql, targetDialect = attr(connection, "dbms"))
      executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
    }
    
    if (dbms == "redshift") {
      ensure_installed("aws.s3")
      .bulkLoadRedshift(connection, qname, data)
    } else if (dbms == "pdw") {
      .bulkLoadPdw(connection, qname, fts, data)
    } else if (dbms == "hive") {
      if (tolower(Sys.info()["sysname"]) == "windows") {
          ensure_installed("ssh")
      }            
      .bulkLoadHive(connection, qname, strsplit(varNames, ",")[[1]], data)
    }
  } else {
    if (attr(connection, "dbms") %in% c("pdw", "redshift", "bigquery", "hive") && createTable && nrow(data) > 0) {
      ctasHack(connection, qname, tempTable, varNames, fts, data, progressBar, oracleTempSchema)
    } else {
      if (createTable) {
        sql <- paste("CREATE TABLE ", qname, " (", fdef, ");", sep = "")
        sql <- SqlRender::translate(sql,
                                    targetDialect = attr(connection, "dbms"),
                                    oracleTempSchema = oracleTempSchema)
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
      insertSql <- SqlRender::translate(insertSql,
                                        targetDialect = connection@dbms,
                                        oracleTempSchema = oracleTempSchema)
      
      batchSize <- 10000
      
      autoCommit <- rJava::.jcall(connection@jConnection, "Z", "getAutoCommit")
      if (autoCommit) {
        rJava::.jcall(connection@jConnection, "V", "setAutoCommit", FALSE)
        on.exit(rJava::.jcall(connection@jConnection, "V", "setAutoCommit", TRUE))
      }
      if (nrow(data) > 0) {
        if (progressBar) {
          pb <- txtProgressBar(style = 3)
        }
        batchedInsert <- rJava::.jnew("org.ohdsi.databaseConnector.BatchedInsert",
                                      connection@jConnection,
                                      insertSql,
                                      ncol(data))
        for (start in seq(1, nrow(data), by = batchSize)) {
          if (progressBar) {
            setTxtProgressBar(pb, start/nrow(data))
          }
          end <- min(start + batchSize - 1, nrow(data))
          setColumn <- function(i, start, end) {
            column <- data[start:end, i]
            if (is.integer(column)) {
              rJava::.jcall(batchedInsert, "V", "setInteger", i, column)
            } else if (is.numeric(column)) {
              rJava::.jcall(batchedInsert, "V", "setNumeric", i, column)
            } else if (identical(class(column), c("POSIXct", "POSIXt"))) {
              rJava::.jcall(batchedInsert, "V", "setDateTime", i, as.character(column))
            } else if (class(column) == "Date") {
              rJava::.jcall(batchedInsert, "V", "setDate", i, as.character(column))
            } else if (is.bigint(column)) {
              rJava::.jcall(batchedInsert, "V", "setBigint", i, as.numeric(column))
            } else {
              rJava::.jcall(batchedInsert, "V", "setString", i, as.character(column))
            }
            return(NULL)
          }
          lapply(1:ncol(data), setColumn, start = start, end = end)
          rJava::.jcall(batchedInsert, "V", "executeBatch")
        }
        if (progressBar) {
          setTxtProgressBar(pb, 1)
          close(pb)
        }
      }
    }
  }
}

#' @export
insertTable.DatabaseConnectorDbiConnection <- function(connection,
                                                       tableName,
                                                       data,
                                                       dropTableIfExists = TRUE,
                                                       createTable = TRUE,
                                                       tempTable = FALSE,
                                                       oracleTempSchema = NULL,
                                                       useMppBulkLoad = FALSE,
                                                       progressBar = FALSE,
                                                       camelCaseToSnakeCase = FALSE) {
  if (camelCaseToSnakeCase) {
    colnames(data) <- SqlRender::camelCaseToSnakeCase(colnames(data))
  }
  if (!tempTable & substr(tableName, 1, 1) == "#") {
    tempTable <- TRUE
    warning("Temp table name detected, setting tempTable parameter to TRUE")
  }
  tableName <- gsub("^#", "", tableName)
  # Convert dates and datetime to UNIX timestamp:
  for (i in 1:ncol(data)) {
    if (inherits(data[, i], "Date")) {
      data[, i] <- as.numeric(as.POSIXct(as.character(data[, i]), origin = "1970-01-01", tz = "GMT"))
    }
    if (inherits(data[, i], "POSIXct")) {
      data[, i] <- as.numeric(as.POSIXct(data[, i], origin = "1970-01-01", tz = "GMT"))
    }
  }
  DBI::dbWriteTable(connection@dbiConnection, tableName, data, overwrite = dropTableIfExists, temporary = tempTable)
  invisible(NULL)
}

.checkMppCredentials <- function(connection) {
  if (attr(connection, "dbms") == "pdw" && tolower(Sys.info()["sysname"]) == "windows") {
    if (Sys.getenv("DWLOADER_PATH") == "") {
      writeLines("Please set environment variable DWLOADER_PATH to DWLoader binary path.")
      return(FALSE)
    }
    return(TRUE)
  } else if (attr(connection, "dbms") == "redshift") {
    envSet <- FALSE
    bucket <- FALSE
    
    if (Sys.getenv("AWS_ACCESS_KEY_ID") != "" && Sys.getenv("AWS_SECRET_ACCESS_KEY") != "" && Sys.getenv("AWS_BUCKET_NAME") !=
        "" && Sys.getenv("AWS_DEFAULT_REGION") != "") {
      envSet <- TRUE
    }
    
    if (aws.s3::bucket_exists(bucket = Sys.getenv("AWS_BUCKET_NAME"))) {
      bucket <- TRUE
    }
    
    if (Sys.getenv("AWS_SSE_TYPE") == "") {
      warning("Not using Server Side Encryption for AWS S3")
    }
    return(envSet & bucket)
  } else if (attr(connection, "dbms") == "hive") {
    if (tolower(Sys.info()["sysname"]) == "windows" && !require("ssh")) {
      writeLines("Please install ssh package, it's required")
      return(FALSE)
    }
    if (Sys.getenv("HIVE_NODE_HOST") == "") {
      writeLines("Please set environment variable HIVE_NODE_HOST to the Hive Node's host:port")
      return(FALSE)
    }
    if (Sys.getenv("HIVE_SSH_USER") == "") {
      warning(paste("HIVE_SSH_USER is not set, using default", .getHiveSshUser()))
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

.getHiveSshUser <- function() {
  sshUser <- Sys.getenv("HIVE_SSH_USER")
  return (if (sshUser == "") "root" else sshUser)
}

.bulkLoadPdw <- function(connection, qname, fts, data) {
  start <- Sys.time()
  # Format integer fields to prevent scientific notation:
  for (i in 1:ncol(data)) {
    if (fts[i] %in% c("INTEGER", "BIGINT")) {
      data[, i] <- format(data[, i], scientific = FALSE)
    }
  }
  eol <- "\r\n"
  fileName <- file.path(tempdir(), sprintf("pdw_insert_%s", uuid::UUIDgenerate(use.time = TRUE)))
  write.table(x = data,
              na = "",
              file = sprintf("%s.csv", fileName),
              row.names = FALSE,
              quote = FALSE,
              col.names = TRUE,
              sep = "~*~")
  R.utils::gzip(filename = sprintf("%s.csv",
                                   fileName), destname = sprintf("%s.gz", fileName), remove = TRUE)
  
  auth <- sprintf("-U %1s -P %2s", attr(connection, "user"), attr(connection, "password"))
  if (is.null(attr(connection, "user")) && is.null(attr(connection, "password"))) {
    auth <- "-W"
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
                     shQuote(sprintf("%s.gz", fileName)),
                     qname,
                     shQuote("~*~"),
                     shQuote(eol),
                     pdwServer,
                     auth)
  
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
  }, finally = {
    try(file.remove(sprintf("%s.gz", fileName)), silent = TRUE)
  })
  
  sql <- "SELECT COUNT(*) FROM @table"
  sql <- SqlRender::render(sql, table = qname)
  count <- querySql(connection, sql)
  if (count[1, 1] != nrow(data)) {
    stop("Something went wrong when bulk uploading. Data has ", nrow(data), " rows, but table has ", count[1, 1], " records")
  }
}

.bulkLoadRedshift <- function(connection, qname, data) {
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
  }, error = function(e) {
    stop("Error in Redshift bulk upload. Please check stl_load_errors and Redshift/S3 access.")
  }, finally = {
    try(file.remove(sprintf("%s.gz", fileName)), silent = TRUE)
    try(aws.s3::delete_object(object = sprintf("%s.gz", fileName),
                              bucket = Sys.getenv("AWS_BUCKET_NAME")), silent = TRUE)
  })
}

.bulkLoadHive <- function(connection, qname, varNames, data) {
  start <- Sys.time()
  fileName <- sprintf("hive_insert_%s.csv", uuid::UUIDgenerate(use.time = TRUE))
  filePath <- file.path(tempdir(), fileName)
  write.csv(x = data, na = "", file = filePath, row.names = FALSE, quote = TRUE)

  hiveUser <- .getHiveSshUser()
  hivePasswd <- Sys.getenv("HIVE_SSH_PASSWORD")
  hiveHost <- Sys.getenv("HIVE_NODE_HOST")
  sshPort <- (function(port) if (port == "") "2222" else port)(Sys.getenv("HIVE_SSH_PORT"))
  nodePort <- (function(port) if (port == "") "8020" else port)(Sys.getenv("HIVE_NODE_PORT"))
  hiveKeyFile <- (function(keyfile) if (keyfile == "") NULL else keyfile)(Sys.getenv("HIVE_KEYFILE"))
  hadoopUser <- (function(hadoopUser) if (hadoopUser == "") "hive" else hadoopUser)(Sys.getenv("HADOOP_USER_NAME"))

    tryCatch({
    if (tolower(Sys.info()["sysname"]) == "windows") {
        session <- ssh_connect(host = sprintf("%s@%s:%s", hiveUser, hiveHost, sshPort), passwd = hivePasswd, keyfile = hiveKeyFile)
        remoteFile <- paste0("/tmp/", fileName)
        scp_upload(session, filePath, to = remoteFile, verbose = FALSE)
        hadoopDir <- sprintf("/user/%s/%s", hadoopUser, uuid::UUIDgenerate(use.time = TRUE))
        hadoopFile <- paste0(hadoopDir, "/", fileName)
        ssh_exec_wait(session, sprintf("HADOOP_USER_NAME=%s hadoop fs -mkdir %s", hadoopUser, hadoopDir))
        command <- sprintf("HADOOP_USER_NAME=%s hadoop fs -put %s %s", hadoopUser, remoteFile, hadoopFile)
        ssh_exec_wait(session, command = command)
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
    fdef <- paste(sapply(varNames, def), collapse = ", ")
    sql <- SqlRender::render("CREATE TABLE @table(@fdef) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION 'hdfs://@hiveHost:@nodePort@filename';", 
      filename = hadoopDir, table = qname, fdef = fdef, hiveHost = hiveHost, nodePort = nodePort)
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
        ssh_disconnect(session)
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
