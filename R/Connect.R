# @file Connect.R
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

supportedDbms <- c(
  "oracle",
  "postgresql",
  "redshift",
  "sql server",
  "bigquery",
  "spark",
  "snowflake")

#' @title dbConnect
#'
#' @description
#' Creates a connection to a database server .There are four ways to call this
#' function:
#' 
#' - `connect(dbms, user, password, server, port, extraSettings, oracleDriver, pathToDriver)`
#' - `connect(connectionDetails)`
#' - `connect(dbms, connectionString, pathToDriver))`
#' - `connect(dbms, connectionString, user, password, pathToDriver)`
#'
#' @usage
#' NULL
#'
#' @template Dbms
#' @template DefaultConnectionDetails
#' @param connectionDetails   An object of class `connectionDetails` as created by the
#'                            [createConnectionDetails()] function.
#'
#' @details
#' This function creates a connection to a database.
#'
#' @return
#' An object that extends `DBIConnection` in a database-specific manner. This object is used to
#' direct commands to the database engine.
#'
#' @examples
#' \dontrun{
#' DBI::dbConnect(DatabaseConnectorDriver(),
#'   dbms = "postgresql",
#'   server = "localhost/postgres",
#'   user = "root",
#'   password = "xxx"
#' )
#' conn <- connect(connectionDetails)
#' dbGetQuery(conn, "SELECT COUNT(*) FROM person")
#' disconnect(conn)
#'
#' conn <- connect(dbms = "sql server", server = "RNDUSRDHIT06.jnj.com")
#' dbGetQuery(conn, "SELECT COUNT(*) FROM concept")
#' disconnect(conn)
#'
#' conn <- connect(
#'   dbms = "oracle",
#'   server = "127.0.0.1/xe",
#'   user = "system",
#'   password = "xxx",
#'   pathToDriver = "c:/temp"
#' )
#' dbGetQuery(conn, "SELECT COUNT(*) FROM test_table")
#' disconnect(conn)
#'
#' conn <- connect(
#'   dbms = "postgresql",
#'   connectionString = "jdbc:postgresql://127.0.0.1:5432/cmd_database"
#' )
#' dbGetQuery(conn, "SELECT COUNT(*) FROM person")
#' disconnect(conn)
#' }
connect <- function(connectionDetails = NULL,
                    dbms = NULL,
                    user = NULL,
                    password = NULL,
                    server = NULL,
                    port = NULL,
                    extraSettings = NULL,
                    oracleDriver = "thin",
                    connectionString = NULL,
                    pathToDriver = Sys.getenv("DATABASECONNECTOR_JAR_FOLDER")) {
  if (missing(connectionDetails) || is.null(connectionDetails)) {
    # warn("Use of dbms, server, etc. when calling connect() is deprecated. Use connectionDetails instead.")
    DBI::dbConnect(DatabaseConnectorDriver(),
      dbms = dbms,
      user = user,
      password = password,
      server = server,
      port = port,
      extraSettings = extraSettings,
      oracleDriver = oracleDriver,
      connectionString = connectionString,
      pathToDriver = pathToDriver
    )
    return(connect(connectionDetails))
  } else if (is(connectionDetails, "DbiConnectionDetails")) {
    return(connectUsingDbi(connectionDetails))
  } else {
    # Using default connectionDetails
    assertDetailsCanBeValidated(connectionDetails)
    checkIfDbmsIsSupported(connectionDetails$dbms)
    
    if (connectionDetails$dbms %in% c("sqlite", "sqlite extended")) {
      connectSqlite(connectionDetails)
    } else if (connectionDetails$dbms == "duckdb") {
      connectDuckdb(connectionDetails)
    } else if (connectionDetails$dbms == "spark" && is.null(connectionDetails$connectionString())) {
      connectSparkUsingOdbc(connectionDetails)
    } else {
      return(connectUsingJdbc(connectionDetails))
    }
  }
}

connectUsingJdbc <- function(connectionDetails) {
  dbms <- connectionDetails$dbms
  connectionDetails$pathToDriver <- path.expand(connectionDetails$pathToDriver)
  checkPathToDriver(connectionDetails$pathToDriver, dbms)

  if (dbms == "sql server" || dbms == "synapse" || dbms == "pdw") {
    return(connectSqlServer(connectionDetails))
  } else if (dbms == "oracle") {
    return(connectOracle(connectionDetails))
  } else if (dbms == "postgresql") {
    return(connectPostgreSql(connectionDetails))
  } else if (dbms == "redshift") {
    return(connectRedShift(connectionDetails))
  } else if (dbms == "netezza") {
    return(connectNetezza(connectionDetails))
  } else if (dbms == "impala") {
    return(connectImpala(connectionDetails))
  } else if (dbms == "hive") {
    return(connectHive(connectionDetails))
  } else if (dbms == "bigquery") {
    return(connectBigQuery(connectionDetails))
  } else if (dbms == "spark") {
    return(connectSpark(connectionDetails))
  } else if (dbms == "snowflake") {
    return(connectSnowflake(connectionDetails))
  } else {
    abort("Something went wrong when trying to connect to ", dbms)
  }
}

connectSqlServer <- function(connectionDetails) {
  jarPath <- findPathToJar("^mssql-jdbc.*.jar$|^sqljdbc.*\\.jar$", connectionDetails$pathToDriver)
  driver <- getJbcDriverSingleton("com.microsoft.sqlserver.jdbc.SQLServerDriver", jarPath)
  inform("Connecting using SQL Server driver")
  if (!is.null(connectionDetails$connectionString()) && connectionDetails$connectionString() != "") {
    connectionString <- connectionDetails$connectionString()
  } else {
    connectionString <- paste0("jdbc:sqlserver://", connectionDetails$server())
    if (!is.null(connectionDetails$port())) {
      connectionString <- paste0(connectionString, ";port=", connectionDetails$port())
    }
    if (!is.null(connectionDetails$extraSettings)) {
      connectionString <- paste(connectionString, connectionDetails$extraSettings, sep = ";")
    }
    if (is.null(connectionDetails$user())) {
      connectionString <- paste(connectionString, "integratedSecurity=true", sep = ";")
    }
  }
  if (is.null(connectionDetails$user())) {
    inform("- Using Windows integrated security")
    setPathToDll()
    connection <- connectUsingJdbcDriver(driver, connectionString, dbms = connectionDetails$dbms)
  } else {
    connection <- connectUsingJdbcDriver(driver,
      connectionString,
      user = connectionDetails$user(),
      password = connectionDetails$password(),
      dbms = connectionDetails$dbms
    )
  }
  if (connectionDetails$dbms == "pdw") {
    # Used for bulk upload:
    attr(connection, "user") <- connectionDetails$user
    attr(connection, "password") <- connectionDetails$password
  }
  return(connection)
}

connectOracle <- function(connectionDetails) {
  inform("Connecting using Oracle driver")
  jarPath <- findPathToJar("^ojdbc.*\\.jar$", connectionDetails$pathToDriver)
  driver <- getJbcDriverSingleton("oracle.jdbc.driver.OracleDriver", jarPath)
  if (is.null(connectionDetails$connectionString()) || connectionDetails$connectionString() == "") {
    # Build connection string from parts
    if (connectionDetails$oracleDriver == "thin") {
      inform("- using THIN to connect")
      if (is.null(connectionDetails$port())) {
        port <- "1521"
      } else {
        port <- connectionDetails$port()
      }
      host <- "127.0.0.1"
      sid <- connectionDetails$server()
      if (grepl("/", sid)) {
        parts <- unlist(strsplit(sid, "/"))
        host <- parts[1]
        sid <- parts[2]
      }
      connectionString <- paste0("jdbc:oracle:thin:@", host, ":", port, ":", sid)
      if (!is.null(connectionDetails$extraSettings)) {
        connectionString <- paste(connectionString, connectionDetails$extraSettings, sep = ";")
      }
      result <- class(
        try(
          connection <- connectUsingJdbcDriver(
            driver,
            connectionString,
            user = connectionDetails$user(),
            password = connectionDetails$password(),
            oracle.jdbc.mapDateToTimestamp = "false",
            dbms = connectionDetails$dbms
          ),
          silent = FALSE
        )
      )[1]

      # Try using TNSName instead:
      if (result == "try-error") {
        inform("- Trying using TNSName")
        connectionString <- paste0("jdbc:oracle:thin:@", connectionDetails$server())
        connection <- connectUsingJdbcDriver(driver,
          connectionString,
          user = connectionDetails$user(),
          password = connectionDetails$password(),
          oracle.jdbc.mapDateToTimestamp = "false",
          dbms = connectionDetails$dbms
        )
      }
    }
    if (connectionDetails$oracleDriver == "oci") {
      inform("- using OCI to connect")
      connectionString <- paste0("jdbc:oracle:oci8:@", connectionDetails$server())
      connection <- connectUsingJdbcDriver(driver,
        connectionString,
        user = connectionDetails$user(),
        password = connectionDetails$password(),
        oracle.jdbc.mapDateToTimestamp = "false",
        dbms = connectionDetails$dbms
      )
    }
  } else {
    # User has provided the connection string:
    if (is.null(connectionDetails$user())) {
      connection <- connectUsingJdbcDriver(driver,
        connectionDetails$connectionString(),
        oracle.jdbc.mapDateToTimestamp = "false",
        dbms = connectionDetails$dbms
      )
    } else {
      connection <- connectUsingJdbcDriver(driver,
        connectionDetails$connectionString(),
        user = connectionDetails$user(),
        password = connectionDetails$password(),
        oracle.jdbc.mapDateToTimestamp = "false",
        dbms = connectionDetails$dbms
      )
    }
  }
  return(connection)
}

connectPostgreSql <- function(connectionDetails) {
  inform("Connecting using PostgreSQL driver")
  jarPath <- findPathToJar("^postgresql-.*\\.jar$", connectionDetails$pathToDriver)
  driver <- getJbcDriverSingleton("org.postgresql.Driver", jarPath)
  if (is.null(connectionDetails$connectionString()) || connectionDetails$connectionString() == "") {
    if (!grepl("/", connectionDetails$server())) {
      abort("Error: database name not included in server string but is required for PostgreSQL. Please specify server as <host>/<database>")
    }
    parts <- unlist(strsplit(connectionDetails$server(), "/"))
    host <- parts[1]
    database <- parts[2]
    if (is.null(connectionDetails$port())) {
      port <- "5432"
    } else {
      port <- connectionDetails$port()
    }
    connectionString <- paste0("jdbc:postgresql://", host, ":", port, "/", database)
    if (!is.null(connectionDetails$extraSettings)) {
      connectionString <- paste(connectionString, connectionDetails$extraSettings, sep = "?")
    }
  } else {
    connectionString <- connectionDetails$connectionString()
  }
  if (is.null(connectionDetails$user())) {
    connection <- connectUsingJdbcDriver(driver, connectionString, dbms = connectionDetails$dbms)
  } else {
    connection <- connectUsingJdbcDriver(driver,
      connectionString,
      user = connectionDetails$user(),
      password = connectionDetails$password(),
      dbms = connectionDetails$dbms
    )
  }
  # Used for bulk upload:
  attr(connection, "user") <- connectionDetails$user
  attr(connection, "password") <- connectionDetails$password
  attr(connection, "server") <- connectionDetails$server
  attr(connection, "port") <- connectionDetails$port
  return(connection)
}

connectRedShift <- function(connectionDetails) {
  inform("Connecting using Redshift driver")
  jarPath <- findPathToJar("^RedshiftJDBC.*\\.jar$", connectionDetails$pathToDriver)
  if (grepl("RedshiftJDBC42", jarPath)) {
    driver <- getJbcDriverSingleton("com.amazon.redshift.jdbc42.Driver", jarPath)
  } else {
    driver <- getJbcDriverSingleton("com.amazon.redshift.jdbc4.Driver", jarPath)
  }
  if (is.null(connectionDetails$connectionString()) || connectionDetails$connectionString() == "") {
    if (!grepl("/", connectionDetails$server())) {
      abort("Error: database name not included in server string but is required for Redshift Please specify server as <host>/<database>")
    }
    parts <- unlist(strsplit(connectionDetails$server(), "/"))
    host <- parts[1]
    database <- parts[2]
    if (is.null(connectionDetails$port())) {
      port <- "5439"
    } else {
      port <- connectionDetails$port()
    }
    connectionString <- paste("jdbc:redshift://", host, ":", port, "/", database, sep = "")
    if (!is.null(connectionDetails$extraSettings)) {
      connectionString <- paste(connectionString, connectionDetails$extraSettings, sep = ";")
    }
  } else {
    connectionString <- connectionDetails$connectionString()
  }
  # Default fetchRingBufferSize is 1GB, which is a bit extreme and causing Java out of heap
  # error, so set to something more reasonable if the users didn't already do that:
  if (!grepl("fetchRingBufferSize", connectionString)) {
    connectionString <- paste(connectionString, "fetchRingBufferSize=100M", sep = ";")
  }
  if (is.null(connectionDetails$user())) {
    connection <- connectUsingJdbcDriver(driver, connectionString, dbms = connectionDetails$dbms)
  } else {
    connection <- connectUsingJdbcDriver(driver,
      connectionString,
      user = connectionDetails$user(),
      password = connectionDetails$password(),
      dbms = connectionDetails$dbms
    )
  }
  return(connection)
}

connectBigQuery <- function(connectionDetails) {
  inform("Connecting using BigQuery driver")
  files <- list.files(path = connectionDetails$pathToDriver, full.names = TRUE)
  for (jar in files) {
    rJava::.jaddClassPath(jar)
  }
  jarPath <- findPathToJar("^GoogleBigQueryJDBC42\\.jar$", connectionDetails$pathToDriver)
  driver <- getJbcDriverSingleton("com.simba.googlebigquery.jdbc42.Driver", jarPath)
  if (is.null(connectionDetails$connectionString()) || connectionDetails$connectionString() == "") {
    connectionString <- paste0("jdbc:BQDriver:", connectionDetails$server)
    if (!is.null(connectionDetails$extraSettings)) {
      connectionString <- paste(connectionString, connectionDetails$extraSettings, sep = "?")
    }
  } else {
    connectionString <- connectionDetails$connectionString()
  }
  connection <- connectUsingJdbcDriver(driver,
    connectionString,
    user = connectionDetails$user(),
    password = connectionDetails$password(),
    dbms = connectionDetails$dbms
  )
  return(connection)
}

connectSpark <- function(connectionDetails) {
  inform("Connecting using Spark JDBC driver")
  # jarPath <- findPathToJar("^SparkJDBC42\\.jar$", connectionDetails$pathToDriver)
  jarPath <- findPathToJar("^DatabricksJDBC42\\.jar$", connectionDetails$pathToDriver)
  # driver <- getJbcDriverSingleton("com.simba.spark.jdbc.Driver", jarPath)
  driver <- getJbcDriverSingleton("com.databricks.client.jdbc.Driver", jarPath)
  connectionString <- connectionDetails$connectionString()
  if (is.null(connectionString) || connectionString == "") {
    abort("Error: Connection string required for connecting to Spark.")
  }
  if (!grepl("UseNativeQuery", connectionString)) {
    if (!endsWith(connectionString, ";")) {
      connectionString <- paste0(connectionString, ";")
    }
    connectionString <- paste0(connectionString, "UseNativeQuery=1")
  }
  if (is.null(connectionDetails$user())) {
    connection <- connectUsingJdbcDriver(driver, connectionString, dbms = connectionDetails$dbms)
  } else {
    connection <- connectUsingJdbcDriver(
      jdbcDriver = driver,
      url = connectionString,
      user = connectionDetails$user(),
      password = connectionDetails$password(),
      dbms = connectionDetails$dbms
    )
  }
  return(connection)
}

connectSnowflake <- function(connectionDetails) {
  inform("Connecting using Snowflake driver")
  jarPath <- findPathToJar("^snowflake-jdbc-.*\\.jar$", connectionDetails$pathToDriver)
  driver <- getJbcDriverSingleton("net.snowflake.client.jdbc.SnowflakeDriver", jarPath)
  if (is.null(connectionDetails$connectionString()) || connectionDetails$connectionString() == "") {
    abort("Error: Connection string required for connecting to Snowflake.")
  }
  if (is.null(connectionDetails$user())) {
    connection <- connectUsingJdbcDriver(driver, connectionDetails$connectionString(), dbms = connectionDetails$dbms,
                    "CLIENT_TIMESTAMP_TYPE_MAPPING"="TIMESTAMP_NTZ")
  } else {
    connection <- connectUsingJdbcDriver(driver,
      connectionDetails$connectionString(),
      user = connectionDetails$user(),
      password = connectionDetails$password(),
      dbms = connectionDetails$dbms,
      "CLIENT_TIMESTAMP_TYPE_MAPPING"="TIMESTAMP_NTZ"
    )
  }
  return(connection)
}

connectUsingJdbcDriver <- function(jdbcDriver,
                                   url,
                                   identifierQuote = "'",
                                   stringQuote = "'",
                                   dbms = "Unknown",
                                   ...) {
  properties <- list(...)
  p <- rJava::.jnew("java/util/Properties")
  if (length(properties) > 0) {
    for (i in 1:length(properties)) {
      if (is.null(properties[[i]])) {
        abort(sprintf("Connection propery '%s' is NULL.", names(properties)[i]))
      }
      rJava::.jcall(
        p,
        "Ljava/lang/Object;",
        "setProperty",
        names(properties)[i],
        as.character(properties[[i]])[1]
      )
    }
  }
  jConnection <- rJava::.jcall(jdbcDriver, "Ljava/sql/Connection;", "connect", as.character(url), p)
  if (rJava::is.jnull(jConnection)) {
    x <- rJava::.jgetEx(TRUE)
    if (rJava::is.jnull(x)) {
      abort(paste("Unable to connect JDBC to", url))
    } else {
      abort(paste0("Unable to connect JDBC to ", url, " (", rJava::.jcall(x, "S", "getMessage"), ")"))
    }
  }
  ensureDatabaseConnectorConnectionClassExists()
  # class <- getClassDef("DatabaseConnectorConnection", where = class_cache, inherits = FALSE)
  # if (is.null(class) || methods::isVirtualClass(class)) {
  #   setClass("DatabaseConnectorJdbcConnection",
  #            contains = "DatabaseConnectorConnection", 
  #            slots = list(jConnection = "jobjRef"),
  #            where = class_cache)
  # }
  connection <- new("DatabaseConnectorConnection",
    jConnection = jConnection,
    identifierQuote = "",
    stringQuote = "'",
    dbms = dbms,
    uuid = generateRandomString()
  )
  registerWithRStudio(connection)
  attr(connection, "dbms") <- dbms
  return(connection)
}

ensureDatabaseConnectorConnectionClassExists <- function() {
  class <- getClassDef("Microsoft SQL Server", where = class_cache, inherits = FALSE)
  if (is.null(class) || methods::isVirtualClass(class)) {
    setClass("Microsoft SQL Server",
             where = class_cache)
  }
  class <- getClassDef("DatabaseConnectorConnection", where = class_cache, inherits = FALSE)
  if (is.null(class) || methods::isVirtualClass(class)) {
    setClass("DatabaseConnectorConnection", 
             contains = c("Microsoft SQL Server", "DBIConnection"),
             slots = list(
               identifierQuote = "character",
               stringQuote = "character",
               dbms = "character",
               uuid = "character"
             ),
             where = class_cache)
  }
}

connectUsingDbi <- function(dbiConnectionDetails) {
  dbms <- dbiConnectionDetails$dbms
  dbiConnectionDetails$dbms <- NULL
  dbiConnection <- do.call(DBI::dbConnect, dbiConnectionDetails)
  ensureDatabaseConnectorConnectionClassExists()
  class <- getClassDef("DatabaseConnectorDbiConnection", where = class_cache, inherits = FALSE)
  if (is.null(class) || methods::isVirtualClass(class)) {
    setClass("DatabaseConnectorDbiConnection",
             contains = "DatabaseConnectorConnection", 
             slots = list(
               dbiConnection = "DBIConnection",
               server = "character"
             ),
             where = class_cache)
  }
  connection <- new("DatabaseConnectorDbiConnection",
    server = dbms,
    dbiConnection = dbiConnection,
    identifierQuote = "",
    stringQuote = "'",
    dbms = dbms,
    uuid = generateRandomString()
  )
  registerWithRStudio(connection)
  attr(connection, "dbms") <- dbms
  return(connection)
}

generateRandomString <- function(length = 20) {
  return(paste(sample(c(letters, 0:9), length, TRUE), collapse = ""))
}

#' Disconnect from the server
#'
#' @description
#' Close the connection to the server.
#'
#' @template Connection
#'
#' @examples
#' \dontrun{
#' DBI::dbConnect(DatabaseConnectorDriver(),
#'   dbms = "postgresql",
#'   server = "localhost",
#'   user = "root",
#'   password = "blah"
#' )
#' conn <- connect(connectionDetails)
#' count <- querySql(conn, "SELECT COUNT(*) FROM person")
#' disconnect(conn)
#' }
disconnect <- function(connection) {
  if (rJava::is.jnull(connection@jConnection)) {
    rlang::warn("Connection is already closed")
  } else {
    unregisterWithRStudio(connection)
  }
  rJava::.jcall(connection@jConnection, "V", "close")
  invisible(TRUE)
}

setPathToDll <- function() {
  pathToDll <- Sys.getenv("PATH_TO_AUTH_DLL")
  if (pathToDll != "") {
    inform(paste("Looking for authentication DLL in path specified in PATH_TO_AUTH_DLL:", pathToDll))
    rJava::J("org.ohdsi.databaseConnector.Authentication")$addPathToJavaLibrary(pathToDll)
  }
}
