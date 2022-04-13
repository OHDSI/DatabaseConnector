# @file Connect.R
#
# Copyright 2022 Observational Health Data Sciences and Informatics
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

checkIfDbmsIsSupported <- function(dbms) {
  supportedDbmss <- c(
    "oracle",
    "postgresql",
    "redshift",
    "sql server",
    "pdw",
    "netezza",
    "bigquery",
    "sqlite",
    "sqlite extended",
    "spark"
  )
  if (!dbms %in% supportedDbmss) {
    abort(sprintf(
      "DBMS '%s' not supported. Please use one of these values: '%s'",
      dbms,
      paste(supportedDbmss, collapse = "', '")
    ))
  }
}

#' @title
#' createConnectionDetails
#'
#' @description
#' \code{createConnectionDetails} creates a list containing all details needed to connect to a
#' database. There are three ways to call this function:
#' \itemize{
#'   \item \code{createConnectionDetails(dbms, user, password, server, port, extraSettings,
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
#' connectionDetails <- createConnectionDetails(
#'   dbms = "postgresql",
#'   server = "localhost/postgres",
#'   user = "root",
#'   password = "blah"
#' )
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
                                    extraSettings = NULL,
                                    oracleDriver = "thin",
                                    connectionString = NULL,
                                    pathToDriver = Sys.getenv("DATABASECONNECTOR_JAR_FOLDER")) {
  checkIfDbmsIsSupported(dbms)
  pathToDriver <- path.expand(pathToDriver)
  checkPathToDriver(pathToDriver, dbms)
  
  result <- list(
    dbms = dbms,
    extraSettings = extraSettings,
    oracleDriver = oracleDriver,
    pathToDriver = pathToDriver
  )
  
  userExpression <- rlang::enquo(user)
  result$user <- function() rlang::eval_tidy(userExpression)
  
  passWordExpression <- rlang::enquo(password)
  result$password <- function() rlang::eval_tidy(passWordExpression)
  
  serverExpression <- rlang::enquo(server)
  result$server <- function() rlang::eval_tidy(serverExpression)
  
  portExpression <- rlang::enquo(port)
  result$port <- function() rlang::eval_tidy(portExpression)
  
  csExpression <- rlang::enquo(connectionString)
  result$connectionString <- function() rlang::eval_tidy(csExpression)
  
  class(result) <- "connectionDetails"
  return(result)
}

#' @title
#' connect
#'
#' @description
#' \code{connect} creates a connection to a database server .There are four ways to call this
#' function:
#' \itemize{
#'   \item \code{connect(dbms, user, password, server, port, extraSettings, oracleDriver,
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
#' conn <- connect(
#'   dbms = "postgresql",
#'   server = "localhost/postgres",
#'   user = "root",
#'   password = "xxx"
#' )
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
#' @export
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
  if (!missing(connectionDetails) && !is.null(connectionDetails)) {
    connection <- connect(
      dbms = connectionDetails$dbms,
      user = connectionDetails$user(),
      password = connectionDetails$password(),
      server = connectionDetails$server(),
      port = connectionDetails$port(),
      extraSettings = connectionDetails$extraSettings,
      oracleDriver = connectionDetails$oracleDriver,
      connectionString = connectionDetails$connectionString(),
      pathToDriver = connectionDetails$pathToDriver
    )
    
    return(connection)
  }
  checkIfDbmsIsSupported(dbms)
  pathToDriver <- path.expand(pathToDriver)
  checkPathToDriver(pathToDriver, dbms)
  
  if (dbms == "sql server") {
    jarPath <- findPathToJar("^mssql-jdbc.*.jar$|^sqljdbc.*\\.jar$", pathToDriver)
    driver <- getJbcDriverSingleton("com.microsoft.sqlserver.jdbc.SQLServerDriver", jarPath)
    if (missing(user) || is.null(user)) {
      # Using Windows integrated security
      inform("Connecting using SQL Server driver using Windows integrated security")
      setPathToDll()
      
      if (missing(connectionString) || is.null(connectionString)) {
        connectionString <- paste("jdbc:sqlserver://", server, ";integratedSecurity=true", sep = "")
        if (!missing(port) && !is.null(port)) {
          connectionString <- paste(connectionString, ";port=", port, sep = "")
        }
        if (!missing(extraSettings) && !is.null(extraSettings)) {
          connectionString <- paste(connectionString, ";", extraSettings, sep = "")
        }
      }
      connection <- connectUsingJdbcDriver(driver, connectionString, dbms = dbms)
    } else {
      # Using regular user authentication
      inform("Connecting using SQL Server driver")
      if (missing(connectionString) || is.null(connectionString)) {
        connectionString <- paste("jdbc:sqlserver://", server, sep = "")
        if (!missing(port) && !is.null(port)) {
          connectionString <- paste(connectionString, ";port=", port, sep = "")
        }
        if (!missing(extraSettings) && !is.null(extraSettings)) {
          connectionString <- paste(connectionString, ";", extraSettings, sep = "")
        }
      }
      connection <- connectUsingJdbcDriver(driver,
                                           connectionString,
                                           user = user,
                                           password = password,
                                           dbms = dbms
      )
    }
    attr(connection, "dbms") <- dbms
    return(connection)
  }
  if (dbms == "pdw") {
    inform("Connecting using SQL Server driver")
    jarPath <- findPathToJar("^mssql-jdbc.*.jar$|^sqljdbc.*\\.jar$", pathToDriver)
    driver <- getJbcDriverSingleton("com.microsoft.sqlserver.jdbc.SQLServerDriver", jarPath)
    if (missing(user) || is.null(user)) {
      # Using Windows integrated security
      setPathToDll()
      
      if (missing(connectionString) || is.null(connectionString)) {
        connectionString <- paste("jdbc:sqlserver://", server, ";integratedSecurity=true", sep = "")
        if (!missing(port) && !is.null(port)) {
          connectionString <- paste(connectionString, ";port=", port, sep = "")
        }
        if (!missing(extraSettings) && !is.null(extraSettings)) {
          connectionString <- paste(connectionString, ";", extraSettings, sep = "")
        }
      }
      connection <- connectUsingJdbcDriver(driver, connectionString, dbms = dbms)
    } else {
      if (missing(connectionString) || is.null(connectionString)) {
        connectionString <- paste("jdbc:sqlserver://",
                                  server,
                                  ";integratedSecurity=false",
                                  sep = ""
        )
        if (!missing(port) && !is.null(port)) {
          connectionString <- paste(connectionString, ";port=", port, sep = "")
        }
        if (!missing(extraSettings) && !is.null(extraSettings)) {
          connectionString <- paste(connectionString, ";", extraSettings, sep = "")
        }
      }
      connection <- connectUsingJdbcDriver(driver,
                                           connectionString,
                                           user = user,
                                           password = password,
                                           dbms = dbms
      )
    }
    attr(connection, "dbms") <- dbms
    # Used for bulk upload:
    userExpression <- rlang::enquo(user)
    attr(connection, "user") <- function() rlang::eval_tidy(userExpression)
    passwordExpression <- rlang::enquo(password)
    attr(connection, "password") <- function() rlang::eval_tidy(passwordExpression)
    return(connection)
  }
  if (dbms == "oracle") {
    inform("Connecting using Oracle driver")
    jarPath <- findPathToJar("^ojdbc.*\\.jar$", pathToDriver)
    driver <- getJbcDriverSingleton("oracle.jdbc.driver.OracleDriver", jarPath)
    if (missing(connectionString) || is.null(connectionString)) {
      # Build connection string from parts
      if (oracleDriver == "thin") {
        inform("- using THIN to connect")
        if (missing(port) || is.null(port)) {
          port <- "1521"
        }
        host <- "127.0.0.1"
        sid <- server
        if (grepl("/", server)) {
          parts <- unlist(strsplit(server, "/"))
          host <- parts[1]
          sid <- parts[2]
        }
        connectionString <- paste0("jdbc:oracle:thin:@", host, ":", port, ":", sid)
        if (!missing(extraSettings) && !is.null(extraSettings)) {
          connectionString <- paste0(connectionString, extraSettings)
        }
        result <- class(try(connection <- connectUsingJdbcDriver(driver,
                                                                 connectionString,
                                                                 user = user,
                                                                 password = password,
                                                                 oracle.jdbc.mapDateToTimestamp = "false",
                                                                 dbms = dbms
        ), silent = FALSE))[1]
        
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
      if (missing(user) || is.null(user)) {
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
    attr(connection, "dbms") <- dbms
    return(connection)
  }
  if (dbms == "postgresql") {
    inform("Connecting using PostgreSQL driver")
    jarPath <- findPathToJar("^postgresql-.*\\.jar$", pathToDriver)
    driver <- getJbcDriverSingleton("org.postgresql.Driver", jarPath)
    if (missing(connectionString) || is.null(connectionString)) {
      if (!grepl("/", server)) {
        abort("Error: database name not included in server string but is required for PostgreSQL. Please specify server as <host>/<database>")
      }
      
      parts <- unlist(strsplit(server, "/"))
      host <- parts[1]
      database <- parts[2]
      if (missing(port) || is.null(port)) {
        port <- "5432"
      }
      connectionString <- paste0("jdbc:postgresql://", host, ":", port, "/", database)
      if (!missing(extraSettings) && !is.null(extraSettings)) {
        connectionString <- paste(connectionString, "?", extraSettings, sep = "")
      }
    }
    if (missing(user) || is.null(user)) {
      connection <- connectUsingJdbcDriver(driver, connectionString, dbms = dbms)
    } else {
      connection <- connectUsingJdbcDriver(driver,
                                           connectionString,
                                           user = user,
                                           password = password,
                                           dbms = dbms
      )
    }
    attr(connection, "dbms") <- dbms
    # Used for bulk upload:
    userExpression <- rlang::enquo(user)
    attr(connection, "user") <- function() rlang::eval_tidy(userExpression)
    passwordExpression <- rlang::enquo(password)
    attr(connection, "password") <- function() rlang::eval_tidy(passwordExpression)
    serverExpression <- rlang::enquo(server)
    attr(connection, "server") <- function() rlang::eval_tidy(serverExpression)
    portExpression <- rlang::enquo(port)
    attr(connection, "port") <- function() rlang::eval_tidy(portExpression)
    
    return(connection)
  }
  if (dbms == "redshift") {
    inform("Connecting using Redshift driver")
    jarPath <- findPathToJar("^RedshiftJDBC.*\\.jar$", pathToDriver)
    if (grepl("RedshiftJDBC42", jarPath)) {
      driver <- getJbcDriverSingleton("com.amazon.redshift.jdbc42.Driver", jarPath)
    } else {
      driver <- getJbcDriverSingleton("com.amazon.redshift.jdbc4.Driver", jarPath)
    }
    if (missing(connectionString) || is.null(connectionString)) {
      if (!grepl("/", server)) {
        abort("Error: database name not included in server string but is required for Redshift Please specify server as <host>/<database>")
      }
      parts <- unlist(strsplit(server, "/"))
      host <- parts[1]
      database <- parts[2]
      if (missing(port) || is.null(port)) {
        port <- "5439"
      }
      connectionString <- paste("jdbc:redshift://", host, ":", port, "/", database, sep = "")
      
      if (!missing(extraSettings) && !is.null(extraSettings)) {
        connectionString <- paste(connectionString, "?", extraSettings, sep = "")
      }
    }
    if (missing(user) || is.null(user)) {
      connection <- connectUsingJdbcDriver(driver, connectionString, dbms = dbms)
    } else {
      connection <- connectUsingJdbcDriver(driver,
                                           connectionString,
                                           user = user,
                                           password = password,
                                           dbms = dbms
      )
    }
    attr(connection, "dbms") <- dbms
    return(connection)
  }
  if (dbms == "netezza") {
    inform("Connecting using Netezza driver")
    jarPath <- findPathToJar("^nzjdbc\\.jar$", pathToDriver)
    driver <- getJbcDriverSingleton("org.netezza.Driver", jarPath)
    if (missing(connectionString) || is.null(connectionString)) {
      if (!grepl("/", server)) {
        abort("Error: database name not included in server string but is required for Netezza. Please specify server as <host>/<database>")
      }
      parts <- unlist(strsplit(server, "/"))
      host <- parts[1]
      database <- parts[2]
      if (missing(port) || is.null(port)) {
        port <- "5480"
      }
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
                                           dbms = dbms
      )
    }
    attr(connection, "dbms") <- dbms
    return(connection)
  }
  if (dbms == "impala") {
    inform("Connecting using Impala driver")
    jarPath <- findPathToJar("^ImpalaJDBC42\\.jar$", pathToDriver)
    driver <- getJbcDriverSingleton("com.cloudera.impala.jdbc.Driver", jarPath)
    if (missing(connectionString) || is.null(connectionString)) {
      if (missing(port) || is.null(port)) {
        port <- "21050"
      }
      connectionString <- paste0("jdbc:impala://", server, ":", port)
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
                                           dbms = dbms
      )
    }
    attr(connection, "dbms") <- dbms
    return(connection)
  }
  if (dbms == "hive") {
    inform("Connecting using Hive driver")
    jarPath <- findPathToJar("^hive-jdbc-standalone\\.jar$", pathToDriver)
    driver <- getJbcDriverSingleton("org.apache.hive.jdbc.HiveDriver", jarPath)
    
    if (missing(connectionString) || is.null(connectionString)) {
      connectionString <- paste0("jdbc:hive2://", server, ":", port)
      if (!missing(extraSettings) && !is.null(extraSettings)) {
        connectionString <- paste0(connectionString, ";", extraSettings)
      }
    }
    connection <- connectUsingJdbcDriver(driver,
                                         connectionString,
                                         user = user,
                                         password = password,
                                         dbms = dbms
    )
    
    attr(connection, "dbms") <- dbms
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
                                         dbms = dbms
    )
    attr(connection, "dbms") <- dbms
    return(connection)
  }
  if (dbms %in% c("sqlite", "sqlite extended")) {
    inform("Connecting using SQLite driver")
    ensure_installed("RSQLite")
    connection <- connectUsingRsqLite(server = server, extended = (dbms == "sqlite extended"))
    attr(connection, "dbms") <- dbms
    return(connection)
  }
  if (dbms == "spark") {
    inform("Connecting using Spark driver")
    jarPath <- findPathToJar("^SparkJDBC42\\.jar$", pathToDriver)
    driver <- getJbcDriverSingleton("com.simba.spark.jdbc.Driver", jarPath)
    if (missing(connectionString) || is.null(connectionString)) {
      abort("Error: Connection string required for connecting to Spark.")
    }
    if (missing(user) || is.null(user)) {
      connection <- connectUsingJdbcDriver(driver, connectionString, dbms = dbms)
    } else {
      connection <- connectUsingJdbcDriver(driver,
                                           connectionString,
                                           user = user,
                                           password = password,
                                           dbms = dbms
      )
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
  connection <- new("DatabaseConnectorJdbcConnection",
                    jConnection = jConnection,
                    identifierQuote = identifierQuote,
                    stringQuote = stringQuote,
                    dbms = dbms,
                    uuid = generateRandomString()
  )
  registerWithRStudio(connection)
  return(connection)
}

connectUsingRsqLite <- function(server, extended) {
  dbiConnection <- DBI::dbConnect(RSQLite::SQLite(), server, extended_types = extended)
  connection <- new("DatabaseConnectorDbiConnection",
                    server = server,
                    dbiConnection = dbiConnection,
                    identifierQuote = "'",
                    stringQuote = "'",
                    dbms = ifelse(extended, "sqlite extended", "sqlite"),
                    uuid = generateRandomString()
  )
  registerWithRStudio(connection)
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
#' @param connection   The connection to the database server.
#'
#' @examples
#' \dontrun{
#' connectionDetails <- createConnectionDetails(
#'   dbms = "postgresql",
#'   server = "localhost",
#'   user = "root",
#'   password = "blah"
#' )
#' conn <- connect(connectionDetails)
#' count <- querySql(conn, "SELECT COUNT(*) FROM person")
#' disconnect(conn)
#' }
#' @export
disconnect <- function(connection) {
  UseMethod("disconnect", connection)
}

#' @export
disconnect.default <- function(connection) {
  if (rJava::is.jnull(connection@jConnection)) {
    warn("Connection is already closed")
  } else {
    unregisterWithRStudio(connection)
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
    inform(paste("Looking for authentication DLL in path specified in PATH_TO_AUTH_DLL:", pathToDll))
    rJava::J("org.ohdsi.databaseConnector.Authentication")$addPathToJavaLibrary(pathToDll)
  }
}

#' Build a connection string using the JDBC symba driver
#' https://www.simba.com/products/BigQuery/doc/JDBC_InstallGuide/content/jdbc/config-intro-online.htm
#' @details
#' This is a helper function to ease the process of creating a connection string for the JDBC BigQuery Driver.
#'
#' @param projectId This is the project where you will connect and the submitted SQL queries jobs will be executed. This is not neccesarilly the project where the datasets you are interested lives, but rather the project you have permissions to execute jobs.
#' @param defaultDataset This is the dataset where tables referenced on SQL code without a project or dataset id in the name will be created.
#' @param authType 0 to use a service account and 2 (default) to use a pregenerated refresh token.
#' @param jsonCredentialsPath This is the path with the json credentials. It can be from the pregenerated refresh token or the service account
#' @param accountEmail This is the account email for the service account if authType = 0
#' @param timeOut The length of time, in seconds, that the driver waits for a query to retrieve the results of an executed job
#' @param logLevel 0 for desabling 6 to log everything. Details here https://www.simba.com/products/BigQuery/doc/JDBC_InstallGuide/content/jdbc/options/loglevel.htm
#' @param logPath The full path to the folder where the driver saves log files when logging is enabled.
#' @param proxyHost The IP address or host name of your proxy server.
#' @param proxyPort The listening port of your proxy server.
#' @param proxyPwd The password, if needed, for proxy server settings.
#' @param proxyUser The user name, if needed, for proxy server settings.
#' @param hostPort   Host and port the driver is connecting. This parameter usually does not change and its default value work
createBQConnectionString <- function(projectId='',
                                     defaultDataset='',
                                     authType = 2,
                                     jsonCredentialsPath = '',
                                     accountEmail = '',
                                     timeOut = 1000,
                                     logLevel = 0,
                                     logPath = '',
                                     proxyHost = '',
                                     proxyPort = '',
                                     proxyPwd = '',
                                     proxyUser = '',
                                     hostPort="jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443"){

  projectId <- paste0("ProjectId=", projectId)
  defaultDataset <- paste0("DefaultDataset=", defaultDataset)

  if (authType == 2){
    credentials <- jsonlite::fromJSON(jsonCredentialsPath)
    clientId <- paste0("OAuthClientId=", credentials$client_id)
    refreshToken <- paste0("OAuthRefreshToken=", credentials$refresh_token)
    clientSecret <- paste0("OAuthClientSecret=", credentials$client_secret)
    keyPath <- ''
    accountEmail <- ''
  }
  else if (authType == 0){
    clientId <- ''
    refreshToken <- ''
    clientSecret <- ''
    keyPath <- paste0("OAuthPvtKeyPath=", jsonCredentialsPath)
    accountEmail <- paste0("OAuthServiceAcctEmail=", accountEmail)
  }
  else{
    print('Only service account or pregenerated tokens supported')
    return(FALSE)
  }

  authType <- paste0("OAuthType=", toString(authType))

  timeOut <- paste0("Timeout=", toString(timeOut))

  # default parameters to ensure we can retrieve large ammounts of data
  largeResults <- "AllowLargeResults=0" # this is enabled for a value of 1
  highThroughput <- "EnableHighThroughputAPI=1" # enabled with 1

  # Enabling query cache by default possibly reduces the cost
  queryCache <- "UseQueryCache=1" # 1 enables the use of cached queries

  # Logging
  if (logLevel > 0 & logPath != ''){
    logPath <- paste0("LogPath=", logPath)
  }

  logLevel <- paste0("LogLevel=", toString(logLevel))

  # Proxy configuration
  if (proxyHost != '' & proxyPort != '') {proxyHost <- paste0("ProxyHost=", proxyHost)}
  if (proxyPort != '' & proxyHost != '') {proxyPort <- paste0("ProxyPort=", proxyPort)}
  if (proxyUser != '' & proxyHost != '' & proxyPort != '') {proxyUser <- paste0("ProxyUser=", proxyUser)}
  if (proxyPwd != '' & proxyHost != '' & proxyPort != '') {proxyPwd <- paste0("ProxyPassword=", proxyPwd)}

  connectionString <- paste(c(hostPort,
                            projectId,
                            defaultDataset,
                            authType,
                            clientId,
                            refreshToken,
                            clientSecret,
                            accountEmail,
                            keyPath,
                            timeOut,
                            largeResults,
                            highThroughput,
                            queryCache,
                            logLevel,
                            logPath,
                            proxyHost,
                            proxyPort,
                            proxyUser,
                            proxyPwd
                            ), collapse=";")

  connectionString <- gsub(pattern = ';{2,}', replacement = ';', connectionString)
  return(connectionString)

}
