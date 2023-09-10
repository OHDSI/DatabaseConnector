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
connect <- function(dbms = NULL,
                    user = NULL,
                    password = NULL,
                    server = NULL,
                    port = NULL,
                    extraSettings = NULL,
                    oracleDriver = "thin",
                    connectionString = NULL,
                    pathToDriver = Sys.getenv("DATABASECONNECTOR_JAR_FOLDER")) {
  
  checkmate::assertChoice(dbms, supportedDbms)
  checkmate::assertCharacter(user, len = 1, min.chars = 1, null.ok = TRUE)
  checkmate::assertCharacter(password, len = 1, min.chars = 1, null.ok = TRUE)
  checkmate::assertCharacter(server, len = 1, min.chars = 1, null.ok = TRUE)
  checkmate::assertIntegerish(as.numeric(port), len = 1, lower = 0, null.ok = TRUE)
  checkmate::assertCharacter(extraSettings, len = 1, min.chars = 1, null.ok = TRUE)
  checkmate::assertCharacter(connectionString, len = 1, min.chars = 1, null.ok = TRUE)
  checkmate::assertCharacter(pathToDriver, len = 1, min.chars = 1, null.ok = FALSE)
  
  pathToDriver <- path.expand(pathToDriver)
  checkPathToDriver(pathToDriver, dbms)

  if (dbms == "sql server") {
    
    jarPath <- findPathToJar("^mssql-jdbc.*.jar$|^sqljdbc.*\\.jar$", pathToDriver)
    driver <- getJbcDriverSingleton("com.microsoft.sqlserver.jdbc.SQLServerDriver", jarPath)
    
    rlang::inform("Connecting using SQL Server driver")
    
    if (is.null(connectionString) || connectionString == "") {
      s <- paste0("jdbc:sqlserver://", server)
      if (!is.null(port)) s <- paste0(s, ";port=", port)
      if (!is.null(extraSettings)) s <- paste(s, extraSettings, sep = ";")
      if (is.null(user)) s <- paste(s, "integratedSecurity=true", sep = ";")
      connectionString <- s
    }
    
    if (is.null(user)) {
      rlang::inform("- Using Windows integrated security")
      setPathToDll()
      connection <- connectUsingJdbcDriver(driver, connectionString, dbms = dbms)
    } else {
      connection <- connectUsingJdbcDriver(driver,
        connectionString,
        user = user,
        password = password,
        dbms = dbms
      )
    }
    return(connection)
  }
  
  if (dbms == "oracle") {

    inform("Connecting using Oracle driver")
    jarPath <- findPathToJar("^ojdbc.*\\.jar$", pathToDriver)
    driver <- getJbcDriverSingleton("oracle.jdbc.driver.OracleDriver", jarPath)
    
    if (is.null(connectionString) || connectionString == "") {
      
      # Build connection string from parts
      if (oracleDriver == "thin") {
        rlang::inform("- using THIN to connect")
        port <- port %||% "1521"
        host <- "127.0.0.1"
        sid <- server
        if (grepl("/", sid)) {
          parts <- unlist(strsplit(sid, "/"))
          host <- parts[1]
          sid <- parts[2]
        }
        connectionString <- paste0("jdbc:oracle:thin:@", host, ":", port, ":", sid)
        if (!is.null(extraSettings)) {
          connectionString <- paste(connectionString, extraSettings, sep = ";")
        }
        result <- class(
          try(
            connection <- connectUsingJdbcDriver(
              driver,
              connectionString,
              user = user,
              password = password,
              oracle.jdbc.mapDateToTimestamp = "false",
              dbms = dbms
            ),
            silent = FALSE
          )
        )[1]
  
        # Try using TNSName instead:
        if (result == "try-error") {
          inform("- Trying using TNSName")
          connectionString <- paste0("jdbc:oracle:thin:@", server)
          connection <- connectUsingJdbcDriver(driver,
            connectionString,
            user = user,
            password = password,
            oracle.jdbc.mapDateToTimestamp = "false",
            dbms = dbms
          )
        }
      }
      if (oracleDriver == "oci") {
        inform("- using OCI to connect")
        connectionString <- paste0("jdbc:oracle:oci8:@", server)
        connection <- connectUsingJdbcDriver(driver,
          connectionString,
          user = user,
          password = password,
          oracle.jdbc.mapDateToTimestamp = "false",
          dbms = dbms
        )
      }
    } else {
      # User has provided the connection string:
      if (is.null(user)) {
        connection <- connectUsingJdbcDriver(driver,
          connectionString,
          oracle.jdbc.mapDateToTimestamp = "false",
          dbms = dbms
        )
      } else {
        connection <- connectUsingJdbcDriver(driver,
          connectionString,
          user = user,
          password = password,
          oracle.jdbc.mapDateToTimestamp = "false",
          dbms = dbms
        )
      }
    }
    return(connection)
  }
  
  if (dbms == "postgresql") {

    inform("Connecting using PostgreSQL driver")
    
    jarPath <- findPathToJar("^postgresql-.*\\.jar$", pathToDriver)
    driver <- getJbcDriverSingleton("org.postgresql.Driver", jarPath)
    
    if (is.null(connectionString) || connectionString == "") {
      if (!grepl("/", server)) {
        rlang::abort("Error: database name not included in server string but is required for PostgreSQL. Please specify server as <host>/<database>")
      }
      
      parts <- unlist(strsplit(server, "/"))
      host <- parts[1]
      database <- parts[2]
      port <- port %||% 5432
      connectionString <- paste0("jdbc:postgresql://", host, ":", port, "/", database)
      
      if (!is.null(extraSettings)) {
        connectionString <- paste(connectionString, extraSettings, sep = "?")
      }
    }
    
    if (is.null(user)) {
      connection <- connectUsingJdbcDriver(driver, connectionString, dbms = dbms)
    } else {
      connection <- connectUsingJdbcDriver(driver,
        connectionString,
        user = user,
        password = password,
        dbms = dbms
      )
    }
    return(connection)
  }
  
  if (dbms == "redshift") {

    rlang::inform("Connecting using Redshift driver")
    jarPath <- findPathToJar("^RedshiftJDBC.*\\.jar$", pathToDriver)
    if (grepl("RedshiftJDBC42", jarPath)) {
      driver <- getJbcDriverSingleton("com.amazon.redshift.jdbc42.Driver", jarPath)
    } else {
      driver <- getJbcDriverSingleton("com.amazon.redshift.jdbc4.Driver", jarPath)
    }
    if (is.null(connectionString) || connectionString == "") {
      if (!grepl("/", server)) {
        rlang::abort("Error: database name not included in server string but is required for Redshift Please specify server as <host>/<database>")
      }
      parts <- unlist(strsplit(server, "/"))
      host <- parts[1]
      database <- parts[2]
      port <- port %||% "5439"
      connectionString <- paste("jdbc:redshift://", host, ":", port, "/", database, sep = "")
      if (!is.null(extraSettings)) {
        connectionString <- paste(connectionString, extraSettings, sep = ";")
      }
    } 
    
    # Default fetchRingBufferSize is 1GB, which is a bit extreme and causing Java out of heap
    # error, so set to something more reasonable if the users didn't already do that:
    if (!grepl("fetchRingBufferSize", connectionString)) {
      connectionString <- paste(connectionString, "fetchRingBufferSize=100M", sep = ";")
    }
    
    if (is.null(user)) {
      connection <- connectUsingJdbcDriver(driver, connectionString, dbms = dbms)
    } else {
      connection <- connectUsingJdbcDriver(driver,
        connectionString,
        user = user,
        password = password,
        dbms = dbms
      )
    }
    return(connection)
  }
  
  if (dbms == "bigquery") {
    inform("Connecting using BigQuery driver")
    files <- list.files(path = pathToDriver, full.names = TRUE)
    for (jar in files) {
      rJava::.jaddClassPath(jar)
    }
    jarPath <- findPathToJar("^GoogleBigQueryJDBC42\\.jar$", pathToDriver)
    driver <- getJbcDriverSingleton("com.simba.googlebigquery.jdbc42.Driver", jarPath)
    if (is.null(connectionString) || connectionString == "") {
      connectionString <- paste0("jdbc:BQDriver:", server)
      if (!is.null(extraSettings)) {
        connectionString <- paste(connectionString, extraSettings, sep = "?")
      }
    } 
    
    connection <- connectUsingJdbcDriver(driver,
                                         connectionString,
                                         user = user,
                                         password = password,
                                         dbms = dbms)
    return(connection)
  }

  if (dbms == "spark") {
    
    inform("Connecting using Spark JDBC driver")
    # jarPath <- findPathToJar("^SparkJDBC42\\.jar$", pathToDriver)
    jarPath <- findPathToJar("^DatabricksJDBC42\\.jar$", pathToDriver)
    # driver <- getJbcDriverSingleton("com.simba.spark.jdbc.Driver", jarPath)
    driver <- getJbcDriverSingleton("com.databricks.client.jdbc.Driver", jarPath)
    
    if (is.null(connectionString) || connectionString == "") {
      rlang::abort("Error: Connection string required for connecting to Spark.")
    }
    
    if (!grepl("UseNativeQuery", connectionString)) {
      if (!endsWith(connectionString, ";")) {
        connectionString <- paste0(connectionString, ";")
      }
      connectionString <- paste0(connectionString, "UseNativeQuery=1")
    }
    
    if (is.null(user)) {
      connection <- connectUsingJdbcDriver(driver, connectionString, dbms = dbms)
    } else {
      connection <- connectUsingJdbcDriver(
        jdbcDriver = driver,
        url = connectionString,
        user = user,
        password = password,
        dbms = dbms
      )
    }
    return(connection)
  }
  
  if (dbms == "snowflake") {
    
    inform("Connecting using Snowflake driver")
    jarPath <- findPathToJar("^snowflake-jdbc-.*\\.jar$", pathToDriver)
    driver <- getJbcDriverSingleton("net.snowflake.client.jdbc.SnowflakeDriver", jarPath)
    
    if (is.null(connectionString) || connectionString == "") {
      abort("Error: Connection string required for connecting to Snowflake.")
    }
    
    if (is.null(user)) {
      connection <- connectUsingJdbcDriver(
        jdbcDriver = driver, 
        url = connectionString, 
        dbms = dbms,
       "CLIENT_TIMESTAMP_TYPE_MAPPING"="TIMESTAMP_NTZ")
    } else {
      connection <- connectUsingJdbcDriver(
        driver,
        url = connectionString,
        user = user,
        password = password,
        dbms = dbms,
        "CLIENT_TIMESTAMP_TYPE_MAPPING"="TIMESTAMP_NTZ"
      )
    }
    return(connection)
  }
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
