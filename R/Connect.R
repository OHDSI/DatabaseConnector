# @file Connect.R
#
# Copyright 2019 Observational Health Data Sciences and Informatics
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

#' @title
#' createConnectionDetails
#'
#' @description
#' \code{createConnectionDetails} creates a list containing all details needed to connect to a
#' database. There are three ways to call this function:
#' \itemize{
#'   \item \code{createConnectionDetails(dbms, user, password, server, port, schema, extraSettings,
#'         oracleDriver, pathToDriver)}
#'   \item \code{createConnectionDetails(dbms, connectionString, pathToDriver)}
#'   \item \code{createConnectionDetails(dbms, connectionString, user, password, pathToDriver)}
#' }
#'
#'
#'
#'
#'
#' @usage
#' NULL
#'
#' @template DbmsDetails
#'
#' @details
#' This function creates a list containing all details needed to connect to a database. The list can
#' then be used in the \code{\link{connect}} function.
#'
#' @return
#' A list with all the details needed to connect to a database.
#' @examples
#' \dontrun{
#' connectionDetails <- createConnectionDetails(dbms = "postgresql",
#'                                              server = "localhost/postgres",
#'                                              user = "root",
#'                                              password = "blah",
#'                                              schema = "cdm_v4")
#' conn <- connect(connectionDetails)
#' dbGetQuery(conn, "SELECT COUNT(*) FROM person")
#' disconnect(conn)
#' }
#' @export
createConnectionDetails <- function(dbms,
                                    user = NULL,
                                    password = NULL,
                                    server = NULL,
                                    port = NULL,
                                    schema = NULL,
                                    extraSettings = NULL,
                                    oracleDriver = "thin",
                                    connectionString = NULL,
                                    pathToDriver = getOption("pathToDriver")) {
  # First: get default values:
  result <- list()
  for (name in names(formals(createConnectionDetails))) {
    result[[name]] <- get(name)
  }
  # Second: overwrite defaults with actual values:
  values <- lapply(as.list(match.call())[-1], function(x) eval(x, envir = sys.frame(-3)))
  for (name in names(values)) {
    if (name %in% names(result))
      result[[name]] <- values[[name]]
  }
  class(result) <- "connectionDetails"
  return(result)
}

jdbcDrivers <- new.env()

loadJdbcDriver <- function(driverClass, classPath) {
  rJava::.jaddClassPath(classPath)
  if (nchar(driverClass) && rJava::is.jnull(rJava::.jfindClass(as.character(driverClass)[1])))
    stop("Cannot find JDBC driver class ", driverClass)
  jdbcDriver <- rJava::.jnew(driverClass, check = FALSE)
  rJava::.jcheck(TRUE)
  return(jdbcDriver)
}

# Singleton pattern to ensure driver is instantiated only once
getJbcDriverSingleton <- function(driverClass = "", classPath = "") {
  key <- paste(driverClass, classPath)
  
  if (key %in% ls(jdbcDrivers)) {
    driver <- get(key, jdbcDrivers)
    if (rJava::is.jnull(driver)) {
      driver <- loadJdbcDriver(driverClass, classPath)
      assign(key, driver, envir = jdbcDrivers)
    }
  } else {
    driver <- loadJdbcDriver(driverClass, classPath)
    assign(key, driver, envir = jdbcDrivers)
  }
  driver
}

findPathToJar <- function(name, pathToDriver) {
  if (missing(pathToDriver) || is.null(pathToDriver)) {
    pathToDriver <- system.file("java", package = "DatabaseConnectorJars")
  } else {
    if (grepl(".jar$", tolower(pathToDriver))) {
      pathToDriver <- basename(pathToDriver)
    }
  }
  files <- list.files(path = pathToDriver, pattern = name, full.names = TRUE)
  if (length(files) == 0) {
    stop("No drives matching pattern ",
         name,
         " found in folder ",
         pathToDriver,
         ". Please download the JDBC ",
         "driver, then add the argument 'pathToDriver', pointing to the local path to directory containing ",
         "the JDBC JAR file. Type ?jdbcDrivers for help on downloading drivers.")
  } else {
    return(files)
  }
}

#' @title
#' connect
#'
#' @description
#' \code{connect} creates a connection to a database server .There are four ways to call this
#' function:
#' \itemize{
#'   \item \code{connect(dbms, user, password, server, port, schema, extraSettings, oracleDriver,
#'         pathToDriver)}
#'   \item \code{connect(connectionDetails)}
#'   \item \code{connect(dbms, connectionString, pathToDriver))}
#'   \item \code{connect(dbms, connectionString, user, password, pathToDriver)}
#' }
#'
#' @usage
#' NULL
#'
#' @template DbmsDetails
#' @param connectionDetails   An object of class \code{connectionDetails} as created by the
#'                            \code{\link{createConnectionDetails}} function.
#'
#' @details
#' This function creates a connection to a database.
#'
#' @return
#' An object that extends \code{DBIConnection} in a database-specific manner. This object is used to
#' direct commands to the database engine.
#'
#' @examples
#' \dontrun{
#' conn <- connect(dbms = "postgresql",
#'                 server = "localhost/postgres",
#'                 user = "root",
#'                 password = "xxx",
#'                 schema = "cdm_v4")
#' dbGetQuery(conn, "SELECT COUNT(*) FROM person")
#' disconnect(conn)
#'
#' conn <- connect(dbms = "sql server", server = "sqlServerHost", schema = "Vocabulary")
#' dbGetQuery(conn, "SELECT COUNT(*) FROM concept")
#' disconnect(conn)
#'
#' conn <- connect(dbms = "oracle",
#'                 server = "127.0.0.1/xe",
#'                 user = "system",
#'                 password = "xxx",
#'                 schema = "test",
#'                 pathToDriver = "c:/temp")
#' dbGetQuery(conn, "SELECT COUNT(*) FROM test_table")
#' disconnect(conn)
#'
#' conn <- connect(dbms = "postgresql",
#'                 connectionString = "jdbc:postgresql://127.0.0.1:5432/cdm_database")
#' dbGetQuery(conn, "SELECT COUNT(*) FROM person")
#' disconnect(conn)
#'
#' }
#' @export
connect <- function(connectionDetails = NULL,
                    dbms = NULL,
                    user = NULL,
                    password = NULL,
                    server = NULL,
                    port = NULL,
                    schema = NULL,
                    extraSettings = NULL,
                    oracleDriver = "thin",
                    connectionString = NULL,
                    pathToDriver = getOption("pathToDriver")) {
  if (!missing(connectionDetails) && !is.null(connectionDetails)) {
    connection <- connect(dbms = connectionDetails$dbms,
                          user = connectionDetails$user,
                          password = connectionDetails$password,
                          server = connectionDetails$server,
                          port = connectionDetails$port,
                          schema = connectionDetails$schema,
                          extraSettings = connectionDetails$extraSettings,
                          oracleDriver = connectionDetails$oracleDriver,
                          connectionString = connectionDetails$connectionString,
                          pathToDriver = connectionDetails$pathToDriver)
    
    return(connection)
  }
  if (dbms == "sql server") {
    jarPath <- findPathToJar("^sqljdbc.*\\.jar$", pathToDriver)
    driver <- getJbcDriverSingleton("com.microsoft.sqlserver.jdbc.SQLServerDriver", jarPath)
    if (missing(user) || is.null(user)) {
      # Using Windows integrated security
      writeLines("Connecting using SQL Server driver using Windows integrated security")
      setPathToDll()
      
      if (missing(connectionString) || is.null(connectionString)) {
        connectionString <- paste("jdbc:sqlserver://", server, ";integratedSecurity=true", sep = "")
        if (!missing(port) && !is.null(port))
          connectionString <- paste(connectionString, ";port=", port, sep = "")
        if (!missing(extraSettings) && !is.null(extraSettings))
          connectionString <- paste(connectionString, ";", extraSettings, sep = "")
      }
      connection <- connectUsingJdbcDriver(driver, connectionString, dbms = dbms)
    } else {
      # Using regular user authentication
      writeLines("Connecting using SQL Server driver")
      if (missing(connectionString) || is.null(connectionString)) {
        connectionString <- paste("jdbc:sqlserver://", server, sep = "")
        if (!missing(port) && !is.null(port))
          connectionString <- paste(connectionString, ";port=", port, sep = "")
        if (!missing(extraSettings) && !is.null(extraSettings))
          connectionString <- paste(connectionString, ";", extraSettings, sep = "")
      }
      connection <- connectUsingJdbcDriver(driver,
                                           connectionString,
                                           user = user,
                                           password = password,
                                           dbms = dbms)
    }
    if (!missing(schema) && !is.null(schema)) {
      database <- strsplit(schema, "\\.")[[1]][1]
      lowLevelExecuteSql(connection, paste("USE", database))
    }
    attr(connection, "dbms") <- dbms
    return(connection)
  }
  if (dbms == "pdw") {
    writeLines("Connecting using SQL Server driver")
    jarPath <- findPathToJar("^sqljdbc.*\\.jar$", pathToDriver)
    driver <- getJbcDriverSingleton("com.microsoft.sqlserver.jdbc.SQLServerDriver", jarPath)
    if (missing(user) || is.null(user)) {
      # Using Windows integrated security
      setPathToDll()
      
      if (missing(connectionString) || is.null(connectionString)) {
        connectionString <- paste("jdbc:sqlserver://", server, ";integratedSecurity=true", sep = "")
        if (!missing(port) && !is.null(port))
          connectionString <- paste(connectionString, ";port=", port, sep = "")
        if (!missing(extraSettings) && !is.null(extraSettings))
          connectionString <- paste(connectionString, ";", extraSettings, sep = "")
      }
      connection <- connectUsingJdbcDriver(driver, connectionString, dbms = dbms)
    } else {
      if (missing(connectionString) || is.null(connectionString)) {
        connectionString <- paste("jdbc:sqlserver://",
                                  server,
                                  ";integratedSecurity=false",
                                  sep = "")
        if (!missing(port) && !is.null(port))
          connectionString <- paste(connectionString, ";port=", port, sep = "")
        if (!missing(extraSettings) && !is.null(extraSettings))
          connectionString <- paste(connectionString, ";", extraSettings, sep = "")
      }
      connection <- connectUsingJdbcDriver(driver,
                                           connectionString,
                                           user = user,
                                           password = password,
                                           dbms = dbms)
    }
    if (!missing(schema) && !is.null(schema)) {
      database <- strsplit(schema, "\\.")[[1]][1]
      lowLevelExecuteSql(connection, paste("USE", database))
    }
    attr(connection, "dbms") <- dbms
    return(connection)
  }
  if (dbms == "oracle") {
    writeLines("Connecting using Oracle driver")
    jarPath <- findPathToJar("^ojdbc.*\\.jar$", pathToDriver)
    driver <- getJbcDriverSingleton("oracle.jdbc.driver.OracleDriver", jarPath)
    if (missing(connectionString) || is.null(connectionString)) {
      # Build connection string from parts
      if (oracleDriver == "thin") {
        writeLines("- using THIN to connect")
        if (missing(port) || is.null(port))
          port <- "1521"
        host <- "127.0.0.1"
        sid <- server
        if (grepl("/", server)) {
          parts <- unlist(strsplit(server, "/"))
          host <- parts[1]
          sid <- parts[2]
        }
        connectionString <- paste0("jdbc:oracle:thin:@", host, ":", port, ":", sid)
        if (!missing(extraSettings) && !is.null(extraSettings))
          connectionString <- paste0(connectionString, extraSettings)
        result <- class(try(connection <- connectUsingJdbcDriver(driver,
                                                                 connectionString,
                                                                 user = user,
                                                                 password = password,
                                                                 oracle.jdbc.mapDateToTimestamp = "false",
                                                                 dbms = dbms), silent = FALSE))[1]
        
        # Try using TNSName instead:
        if (result == "try-error") {
          writeLines("- Trying using TNSName")
          connectionString <- paste0("jdbc:oracle:thin:@", server)
          connection <- connectUsingJdbcDriver(driver,
                                               connectionString,
                                               user = user,
                                               password = password,
                                               oracle.jdbc.mapDateToTimestamp = "false",
                                               dbms = dbms)
        }
      }
      if (oracleDriver == "oci") {
        writeLines("- using OCI to connect")
        connectionString <- paste0("jdbc:oracle:oci8:@", server)
        connection <- connectUsingJdbcDriver(driver,
                                             connectionString,
                                             user = user,
                                             password = password,
                                             oracle.jdbc.mapDateToTimestamp = "false",
                                             dbms = dbms)
      }
    } else {
      # User has provided the connection string:
      if (missing(user) || is.null(user)) {
        connection <- connectUsingJdbcDriver(driver,
                                             connectionString,
                                             oracle.jdbc.mapDateToTimestamp = "false",
                                             dbms = dbms)
      } else {
        connection <- connectUsingJdbcDriver(driver,
                                             connectionString,
                                             user = user,
                                             password = password,
                                             oracle.jdbc.mapDateToTimestamp = "false",
                                             dbms = dbms)
      }
    }
    if (!missing(schema) && !is.null(schema)) {
      lowLevelExecuteSql(connection, paste("ALTER SESSION SET current_schema = ", schema))
    }
    attr(connection, "dbms") <- dbms
    return(connection)
  }
  if (dbms == "postgresql") {
    writeLines("Connecting using PostgreSQL driver")
    jarPath <- findPathToJar("^postgresql-.*\\.jar$", pathToDriver)
    driver <- getJbcDriverSingleton("org.postgresql.Driver", jarPath)
    if (missing(connectionString) || is.null(connectionString)) {
      if (!grepl("/", server))
        stop("Error: database name not included in server string but is required for PostgreSQL. Please specify server as <host>/<database>")
      
      parts <- unlist(strsplit(server, "/"))
      host <- parts[1]
      database <- parts[2]
      if (missing(port) || is.null(port)) {
        port <- "5432"
      }
      connectionString <- paste0("jdbc:postgresql://", host, ":", port, "/", database)
      if (!missing(extraSettings) && !is.null(extraSettings))
        connectionString <- paste(connectionString, "?", extraSettings, sep = "")
    }
    if (missing(user) || is.null(user)) {
      connection <- connectUsingJdbcDriver(driver, connectionString, dbms = dbms)
    } else {
      connection <- connectUsingJdbcDriver(driver,
                                           connectionString,
                                           user = user,
                                           password = password,
                                           dbms = dbms)
    }
    if (!missing(schema) && !is.null(schema))
      lowLevelExecuteSql(connection, paste("SET search_path TO ", schema))
    attr(connection, "dbms") <- dbms
    return(connection)
  }
  if (dbms == "redshift") {
    writeLines("Connecting using Redshift driver")
    jarPath <- findPathToJar("^RedshiftJDBC.*\\.jar$", pathToDriver)
    driver <- getJbcDriverSingleton("com.amazon.redshift.jdbc4.Driver", jarPath)
    if (missing(connectionString) || is.null(connectionString)) {
      if (!grepl("/", server))
        stop("Error: database name not included in server string but is required for Redshift Please specify server as <host>/<database>")
      parts <- unlist(strsplit(server, "/"))
      host <- parts[1]
      database <- parts[2]
      if (missing(port) || is.null(port)) {
        port <- "5439"
      }
      connectionString <- paste("jdbc:redshift://", host, ":", port, "/", database, sep = "")
      
      if (!missing(extraSettings) && !is.null(extraSettings))
        connectionString <- paste(connectionString, "?", extraSettings, sep = "")
    }
    if (missing(user) || is.null(user)) {
      connection <- connectUsingJdbcDriver(driver, connectionString, dbms = dbms)
    } else {
      connection <- connectUsingJdbcDriver(driver,
                                           connectionString,
                                           user = user,
                                           password = password,
                                           dbms = dbms)
    }
    if (!missing(schema) && !is.null(schema))
      lowLevelExecuteSql(connection, paste("SET search_path TO ", schema))
    attr(connection, "dbms") <- dbms
    return(connection)
  }
  if (dbms == "netezza") {
    writeLines("Connecting using Netezza driver")
    jarPath <- findPathToJar("^nzjdbc\\.jar$", pathToDriver)
    driver <- getJbcDriverSingleton("org.netezza.Driver", jarPath)
    if (missing(connectionString) || is.null(connectionString)) {
      if (!grepl("/", server))
        stop("Error: database name not included in server string but is required for Netezza. Please specify server as <host>/<database>")
      parts <- unlist(strsplit(server, "/"))
      host <- parts[1]
      database <- parts[2]
      if (missing(port) || is.null(port))
        port <- "5480"
      connectionString <- paste0("jdbc:netezza://", host, ":", port, "/", database)
      if (!missing(extraSettings) && !is.null(extraSettings)) {
        connectionString <- paste0(connectionString, "?", extraSettings)
      }
    }
    if (missing(user) || is.null(user)) {
      connection <- connectUsingJdbcDriver(driver, connectionString, dbms = dbms)
    } else {
      connection <- connectUsingJdbcDriver(driver,
                                           connectionString,
                                           user = user,
                                           password = password,
                                           dbms = dbms)
    }
    if (!missing(schema) && !is.null(schema)) {
      lowLevelExecuteSql(connection, paste("SET schema TO ", schema))
    }
    attr(connection, "dbms") <- dbms
    return(connection)
  }
  if (dbms == "impala") {
    writeLines("Connecting using Impala driver")
    jarPath <- findPathToJar("\\.jar$", pathToDriver)
    driver <- getJbcDriverSingleton("com.cloudera.impala.jdbc4.Driver", jarPath)
    if (missing(connectionString) || is.null(connectionString)) {
      if (missing(port) || is.null(port)) {
        port <- "21050"
      }
      if (missing(schema) || is.null(schema)) {
        connectionString <- paste0("jdbc:impala://", server, ":", port)
      } else {
        connectionString <- paste0("jdbc:impala://", server, ":", port, "/", schema)
      }
      if (!missing(extraSettings) && !is.null(extraSettings)) {
        connectionString <- paste0(connectionString, ";", extraSettings)
      }
    }
    if (missing(user) || is.null(user)) {
      connection <- connectUsingJdbcDriver(driver, connectionString, dbms = dbms)
    } else {
      connection <- connectUsingJdbcDriver(driver,
                                           connectionString,
                                           user = user,
                                           password = password,
                                           dbms = dbms)
    }
    if (!missing(schema) && !is.null(schema)) {
      lowLevelExecuteSql(connection, paste("USE", schema))
    }
    attr(connection, "dbms") <- dbms
    return(connection)
  }
  if (dbms == "bigquery") {
    writeLines("Connecting using BigQuery driver")
    
    files <- list.files(path = pathToDriver, full.names = TRUE)
    for (jar in files) {
      .jaddClassPath(jar)
    }
    
    jarPath <- findPathToJar("^GoogleBigQueryJDBC42\\.jar$", pathToDriver)
    driver <- getJbcDriverSingleton("com.simba.googlebigquery.jdbc42.Driver", jarPath)
    if (missing(connectionString) || is.null(connectionString)) {
      connectionString <- paste0("jdbc:BQDriver:", server)
      if (!missing(extraSettings) && !is.null(extraSettings)) {
        connectionString <- paste0(connectionString, "?", extraSettings)
      }
    }
    connection <- connectUsingJdbcDriver(driver,
                                         connectionString,
                                         user = user,
                                         password = password,
                                         dbms = dbms)
    attr(connection, "dbms") <- dbms
    return(connection)
  }
  if (dbms == "sqlite") {
    writeLines("Connecting using SQLite driver")
    ensure_installed("RSQLite")
    connection <- connectUsingRsqLite(server = server)
    attr(connection, "dbms") <- dbms
    return(connection)
  }
  if (dbms == "spark") {
    writeLines("Connecting using Spark driver")
    jarPath <- findPathToJar("^SparkJDBC.*\\.jar$", pathToDriver)
    driver <- getJbcDriverSingleton("com.simba.spark.jdbc41.Driver", jarPath)
    
    if (missing(connectionString) || is.null(connectionString)) {
      
      if (!grepl("/", server))
        stop("Error: database name not included in server string but is required for Spark. Please specify server as <host>/<database>")
      parts <- unlist(strsplit(server, "/"))
      host <- parts[1]
      database <- parts[2]
      if (missing(port) || is.null(port)) {
        port <- "443"
      }
      connectionString <- paste("jdbc:spark://", host, ":", port, "/", database, sep = "")
      
      if (!missing(extraSettings) && !is.null(extraSettings)) {
        connectionString <- sprintf("%s;%s", connectionString,
                                    paste(names(extraSettings), extraSettings, sep = "=", collapse=";"))
      }
      
      connectionString <- sprintf("%s;UID=%s;PWD=%s", connectionString, user, password)
    }
    
    connection <- connectUsingJdbcDriver(driver,
                                         connectionString,
                                         dbms = dbms)
    
    if (!missing(schema) && !is.null(schema)) {
      lowLevelExecuteSql(connection, paste("USE", schema))
    }
    attr(connection, "dbms") <- dbms
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
      rJava::.jcall(p,
                    "Ljava/lang/Object;",
                    "setProperty",
                    names(properties)[i],
                    as.character(properties[[i]])[1])
    }
  }
  jConnection <- rJava::.jcall(jdbcDriver, "Ljava/sql/Connection;", "connect", as.character(url), p)
  if (rJava::is.jnull(jConnection)) {
    x <- rJava::.jgetEx(TRUE)
    if (rJava::is.jnull(x)) {
      stop("Unable to connect JDBC to ", url)
    } else {
      stop("Unable to connect JDBC to ", url, " (", rJava::.jcall(x, "S", "getMessage"), ")")
    }
  }
  uuid <- paste(sample(c(LETTERS, letters, 0:9), 20, TRUE), collapse = "")
  connection <- new("DatabaseConnectorJdbcConnection",
                    jConnection = jConnection,
                    identifierQuote = identifierQuote,
                    stringQuote = stringQuote,
                    dbms = dbms,
                    uuid = uuid)
  if (dbms != "spark") {
    registerWithRStudio(connection)  
  }
  return(connection)
}

connectUsingRsqLite <- function(server) {
  uuid <- paste(sample(c(LETTERS, letters, 0:9), 20, TRUE), collapse = "")
  dbiConnection <- DBI::dbConnect(RSQLite::SQLite(), server)
  connection <- new("DatabaseConnectorDbiConnection",
                    server = server,
                    dbiConnection = dbiConnection,
                    identifierQuote = "'",
                    stringQuote = "'",
                    dbms = "sqlite",
                    uuid = uuid)
  registerWithRStudio(connection)
  return(connection)
}

#' Disconnect from the server
#'
#' @description
#' This function sends SQL to the server, and returns the results in an ffdf object.
#'
#' @param connection   The connection to the database server.
#'
#' @examples
#' \dontrun{
#' library(ffbase)
#' connectionDetails <- createConnectionDetails(dbms = "postgresql",
#'                                              server = "localhost",
#'                                              user = "root",
#'                                              password = "blah",
#'                                              schema = "cdm_v4")
#' conn <- connect(connectionDetails)
#' count <- querySql.ffdf(conn, "SELECT COUNT(*) FROM person")
#' disconnect(conn)
#' }
#' @export
disconnect <- function(connection) {
  UseMethod("disconnect", connection) 
}

#' @export
disconnect.default <- function(connection) {
  if (rJava::is.jnull(connection@jConnection)) {
    warning("Connection is already closed")
  } else {
    tryCatch(unregisterWithRStudio(connection), 
             error = function(e) warning("Cannot unregister connection from RStudio"))
  }
  rJava::.jcall(connection@jConnection, "V", "close")
  invisible(TRUE)
}

#' @export
disconnect.DatabaseConnectorDbiConnection <- function(connection) {
  DBI::dbDisconnect(connection@dbiConnection)
  unregisterWithRStudio(connection)
  invisible(TRUE)
}

setPathToDll <- function() {
  pathToDll <- Sys.getenv("PATH_TO_AUTH_DLL") 
  if (pathToDll != "") {
    writeLines(paste("Looking for authentication DLL in path specified in PATH_TO_AUTH_DLL:", pathToDll))
    rJava::J("org.ohdsi.databaseConnector.Authentication")$addPathToJavaLibrary(pathToDll)
  }
}