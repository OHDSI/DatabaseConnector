# @file DatabaseConnector.R
#
# Copyright 2017 Observational Health Data Sciences and Informatics
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

#' DatabaseConnector
#'
#' @docType package
#' @name DatabaseConnector
#' @import RJDBC
#' @import ffbase
#' @import bit
#' @importFrom methods new
#' @importFrom utils sessionInfo setTxtProgressBar txtProgressBar object.size
NULL


#' @title
#' createConnectionDetails
#'
#' @description
#' \code{createConnectionDetails} creates a list containing all details needed to connect to a
#' database. There are three ways to call this function:
#' \itemize{
#'   \item \code{createConnectionDetails(dbms, user, domain, password, server, port, schema,
#'         extraSettings, oracleDriver = "thin")}
#'   \item \code{createConnectionDetails(dbms, connectionString)}
#'   \item \code{createConnectionDetails(dbms, connectionString, user, password)}
#' }
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
#' connectionDetails <- createConnectionDetails(dbms = "mysql",
#'                                              server = "localhost",
#'                                              user = "root",
#'                                              password = "blah",
#'                                              schema = "cdm_v4")
#' conn <- connect(connectionDetails)
#' dbGetQuery(conn, "SELECT COUNT(*) FROM person")
#' dbDisconnect(conn)
#' }
#' @export
createConnectionDetails <- function(dbms,
                                    user = NULL,
                                    domain = NULL,
                                    password = NULL,
                                    server = NULL,
                                    port = NULL,
                                    schema = NULL,
                                    extraSettings = NULL,
                                    oracleDriver = "thin",
                                    connectionString = NULL,
                                    pathToDriver = NULL) {
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

# Singleton pattern to ensure driver is instantiated only once
jdbcSingleton <- function(driverClass = "", classPath = "", identifier.quote = NA) {
  key <- paste(driverClass, classPath)
  if (key %in% ls(jdbcDrivers)) {
    driver <- get(key, jdbcDrivers)
    if (rJava::is.jnull(driver@jdrv)) {
      driver <- RJDBC::JDBC(driverClass, classPath, identifier.quote)
      assign(key, driver, envir = jdbcDrivers)
    }
  } else {
    driver <- RJDBC::JDBC(driverClass, classPath, identifier.quote)
    assign(key, driver, envir = jdbcDrivers)
  }
  driver
}

#' @title
#' connect
#'
#' @description
#' \code{connect} creates a connection to a database server .There are four ways to call this
#' function:
#' \itemize{
#'   \item \code{connect(dbms, user, domain, password, server, port, schema, extraSettings,
#'         oracleDriver = "thin")}
#'   \item \code{connect(connectionDetails)}
#'   \item \code{connect(dbms, connectionString)}
#'   \item \code{connect(dbms, connectionString, user, password)}
#' }
#'
#'
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
#' conn <- connect(dbms = "mysql",
#'                 server = "localhost",
#'                 user = "root",
#'                 password = "xxx",
#'                 schema = "cdm_v4")
#' dbGetQuery(conn, "SELECT COUNT(*) FROM person")
#' dbDisconnect(conn)
#'
#' conn <- connect(dbms = "sql server", server = "RNDUSRDHIT06.jnj.com", schema = "Vocabulary")
#' dbGetQuery(conn, "SELECT COUNT(*) FROM concept")
#' dbDisconnect(conn)
#'
#' conn <- connect(dbms = "oracle",
#'                 server = "127.0.0.1/xe",
#'                 user = "system",
#'                 password = "xxx",
#'                 schema = "test")
#' dbGetQuery(conn, "SELECT COUNT(*) FROM test_table")
#' dbDisconnect(conn)
#'
#' conn <- connect(dbms = "postgresql",
#'                 connectionString = "jdbc:postgresql://127.0.0.1:5432/cmd_database")
#' dbGetQuery(conn, "SELECT COUNT(*) FROM person")
#' dbDisconnect(conn)
#'
#' }
#' @export
connect <- function(connectionDetails,
                    dbms,
                    user,
                    domain,
                    password,
                    server,
                    port,
                    schema,
                    extraSettings,
                    oracleDriver = "thin",
                    connectionString,
                    pathToDriver) {
  if (!missing(connectionDetails) && !is.null(connectionDetails)) {
    connection <- connect(dbms = connectionDetails$dbms,
                          user = connectionDetails$user,
                          domain = connectionDetails$domain,
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
  if (dbms == "mysql") {
    writeLines("Connecting using MySQL driver")
    pathToJar <- system.file("java",
                             "mysql-connector-java-5.1.30-bin.jar",
                             package = "DatabaseConnector")
    driver <- jdbcSingleton("com.mysql.jdbc.Driver", pathToJar, identifier.quote = "`")
    if (missing(connectionString) || is.null(connectionString)) {
      if (missing(port) || is.null(port))
        port <- "3306"
      connectionString <- paste("jdbc:mysql://",
                                server,
                                ":",
                                port,
                                "/?useCursorFetch=true",
                                sep = "")
      if (!missing(extraSettings) && !is.null(extraSettings))
        connectionString <- paste(connectionString, "&", extraSettings, sep = "")
    }
    if (missing(user) || is.null(user)) {
      connection <- RJDBC::dbConnect(driver, connectionString)
    } else {
      connection <- RJDBC::dbConnect(driver, connectionString, user, password)
    }
    if (!missing(schema) && !is.null(schema)) {
      RJDBC::dbSendUpdate(connection, paste("USE", schema))
    }
    attr(connection, "dbms") <- dbms
    return(connection)
  }
  if (dbms == "sql server") {
    if (missing(user) || is.null(user)) {
      # Using Windows integrated security
      writeLines("Connecting using SQL Server driver using Windows integrated security")
      pathToJar <- system.file("java", "sqljdbc4.jar", package = "DatabaseConnector")
      driver <- jdbcSingleton("com.microsoft.sqlserver.jdbc.SQLServerDriver", pathToJar)
      if (missing(connectionString) || is.null(connectionString)) {
        connectionString <- paste("jdbc:sqlserver://", server, ";integratedSecurity=true", sep = "")
        if (!missing(port) && !is.null(port))
          connectionString <- paste(connectionString, ";port=", port, sep = "")
        if (!missing(extraSettings) && !is.null(extraSettings))
          connectionString <- paste(connectionString, ";", extraSettings, sep = "")
      }
      connection <- RJDBC::dbConnect(driver, connectionString)
    } else {
      # Using regular user authentication
      writeLines("Connecting using SQL Server driver")
      if (grepl("/", user) | grepl("\\\\", user))
        stop("User name appears to contain the domain, but this should be specified using the domain parameter")
      if (!missing(domain) && !is.null(domain)) {
        # I have been unable to get Microsoft's JDBC driver to connect when a domain needs to be specified,
        # so using JTDS driver instead: (Note, JTDS has issues with dates, which it converts to VARCHAR), see
        # https://sourceforge.net/p/jtds/bugs/679/
        pathToJar <- system.file("java", "jtds-1.3.1.jar", package = "DatabaseConnector")
        driver <- jdbcSingleton("net.sourceforge.jtds.jdbc.Driver", pathToJar)
        writeLines("Warning: Using JTDS driver because a domain is specified. This may lead to problems. Try using integrated security instead.")
        if (missing(connectionString) || is.null(connectionString)) {
          if (!missing(port) && !is.null(port))
          server <- paste(server, port, sep = ":")
          connectionString <- paste("jdbc:jtds:sqlserver://", server, ";domain=", domain, sep = "")
          if (!missing(extraSettings) && !is.null(extraSettings))
          connectionString <- paste(connectionString, ";", extraSettings, sep = "")
        }
        connection <- RJDBC::dbConnect(driver, connectionString, user, password)
      } else {
        pathToJar <- system.file("java", "sqljdbc4.jar", package = "DatabaseConnector")
        driver <- jdbcSingleton("com.microsoft.sqlserver.jdbc.SQLServerDriver", pathToJar)
        if (missing(connectionString) || is.null(connectionString)) {
          connectionString <- paste("jdbc:sqlserver://", server, sep = "")
          if (!missing(port) && !is.null(port))
          connectionString <- paste(connectionString, ";port=", port, sep = "")
          if (!missing(extraSettings) && !is.null(extraSettings))
          connectionString <- paste(connectionString, ";", extraSettings, sep = "")
        }
        connection <- RJDBC::dbConnect(driver, connectionString, user, password)
      }
    }
    if (!missing(schema) && !is.null(schema)) {
      database <- strsplit(schema, "\\.")[[1]][1]
      RJDBC::dbSendUpdate(connection, paste("USE", database))
    }
    attr(connection, "dbms") <- dbms
    return(connection)
  }
  if (dbms == "pdw") {
    writeLines("Connecting using SQL Server driver")
    pathToJar <- system.file("java", "sqljdbc4.jar", package = "DatabaseConnector")
    driver <- jdbcSingleton("com.microsoft.sqlserver.jdbc.SQLServerDriver", pathToJar)
    if (missing(user) || is.null(user)) {
      # Using Windows integrated security
      if (missing(connectionString) || is.null(connectionString)) {
        connectionString <- paste("jdbc:sqlserver://", server, ";integratedSecurity=true", sep = "")
        if (!missing(port) && !is.null(port))
          connectionString <- paste(connectionString, ";port=", port, sep = "")
        if (!missing(extraSettings) && !is.null(extraSettings))
          connectionString <- paste(connectionString, ";", extraSettings, sep = "")
      }
      connection <- RJDBC::dbConnect(driver, connectionString)
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
      connection <- RJDBC::dbConnect(driver, connectionString, user, password)
    }
    if (!missing(schema) && !is.null(schema)) {
      database <- strsplit(schema, "\\.")[[1]][1]
      RJDBC::dbSendUpdate(connection, paste("USE", database))
    }
    attr(connection, "dbms") <- dbms
    return(connection)
  }
  if (dbms == "oracle") {
    writeLines("Connecting using Oracle driver")
    pathToJar <- system.file("java", "ojdbc6.jar", package = "DatabaseConnector")
    driver <- jdbcSingleton("oracle.jdbc.driver.OracleDriver", pathToJar)
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
        result <- class(try(connection <- dbConnectUsingProperties(driver,
                                                                   connectionString,
                                                                   user = user,
                                                                   password = password,
                                                                   oracle.jdbc.mapDateToTimestamp = "false"), silent = FALSE))[1]

        # Try using TNSName instead:
        if (result == "try-error") {
          writeLines("- Trying using TNSName")
          connectionString <- paste0("jdbc:oracle:thin:@", server)
          connection <- dbConnectUsingProperties(driver,
                                                 connectionString,
                                                 user = user,
                                                 password = password,
                                                 oracle.jdbc.mapDateToTimestamp = "false")
        }
      }
      if (oracleDriver == "oci") {
        writeLines("- using OCI to connect")
        connectionString <- paste0("jdbc:oracle:oci8:@", server)
        connection <- dbConnectUsingProperties(driver,
                                               connectionString,
                                               user = user,
                                               password = password,
                                               oracle.jdbc.mapDateToTimestamp = "false")
      }
    } else {
      # User has provided the connection string:
      if (missing(user) || is.null(user)) {
        connection <- dbConnectUsingProperties(driver,
                                               connectionString,
                                               oracle.jdbc.mapDateToTimestamp = "false")
      } else {
        connection <- dbConnectUsingProperties(driver,
                                               connectionString,
                                               user = user,
                                               password = password,
                                               oracle.jdbc.mapDateToTimestamp = "false")
      }
    }
    if (!missing(schema) && !is.null(schema)) {
      RJDBC::dbSendUpdate(connection, paste("ALTER SESSION SET current_schema = ", schema))
    }
    attr(connection, "dbms") <- dbms
    return(connection)
  }
  if (dbms == "postgresql") {
    writeLines("Connecting using PostgreSQL driver")
    pathToJar <- system.file("java", "postgresql-9.3-1101.jdbc4.jar", package = "DatabaseConnector")
    driver <- jdbcSingleton("org.postgresql.Driver", pathToJar, identifier.quote = "`")
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
      connection <- RJDBC::dbConnect(driver, connectionString)
    } else {
      connection <- RJDBC::dbConnect(driver, connectionString, user, password)
    }
    if (!missing(schema) && !is.null(schema))
      RJDBC::dbSendUpdate(connection, paste("SET search_path TO ", schema))
    attr(connection, "dbms") <- dbms
    return(connection)
  }
  if (dbms == "redshift") {
    writeLines("Connecting using Redshift driver")
    pathToJar <- system.file("java", "RedshiftJDBC4-1.1.17.1017.jar", package = "DatabaseConnector")
    driver <- jdbcSingleton("com.amazon.redshift.jdbc4.Driver", pathToJar, identifier.quote = "`")
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
      connection <- RJDBC::dbConnect(driver, connectionString)
    } else {
      connection <- RJDBC::dbConnect(driver, connectionString, user, password)
    }
    if (!missing(schema) && !is.null(schema))
      RJDBC::dbSendUpdate(connection, paste("SET search_path TO ", schema))
    attr(connection, "dbms") <- dbms
    return(connection)
  }
  if (dbms == "netezza") {
    writeLines("Connecting using Netezza driver")
    if (missing(pathToDriver) || is.null(pathToDriver)) {
      stop(paste("Error: pathToDriver not set but is required for Netezza Please download the Netezza JDBC driver, then add the argument ",
                 "'pathToDriver', pointing to the local path to directory containing Netezza JDBC JAR file"))
    }
    pathToJar <- file.path(pathToDriver, "nzjdbc.jar")
    if (!file.exists(pathToJar)) {
      stop(paste0("Error: Cannot find nzjdbc.jar in ", pathToJar))
    }
    driver <- jdbcSingleton("org.netezza.Driver", pathToJar, identifier.quote = "`")
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
      connection <- RJDBC::dbConnect(driver, connectionString)
    } else {
      connection <- RJDBC::dbConnect(driver, connectionString, user, password)
    }
    if (!missing(schema) && !is.null(schema)) {
      RJDBC::dbSendUpdate(connection, paste("SET schema TO ", schema))
    }
    attr(connection, "dbms") <- dbms
    return(connection)
  }
  if (dbms == "impala") {
    writeLines("Connecting using Impala driver")
    if (missing(pathToDriver) || is.null(pathToDriver)) {
      stop(paste("Error: pathToDriver not set but is required for Impala. Please download the Impala JDBC driver, then add the argument ",
        "'pathToDriver', pointing to the local path to directory containing the Impala JDBC JAR files"))
    }
    driverClasspath <- paste(list.files(pathToDriver, "\\.jar$", full.names = TRUE), collapse=":")
    if (nchar(driverClasspath) == 0) {
      stop(paste0("Error: no JAR files found in '",pathToDriver,"'. Please check 'pathToDriver' setting."))
    }
    driver <- jdbcSingleton("com.cloudera.impala.jdbc4.Driver", driverClasspath, identifier.quote = "`")
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
      connection <- RJDBC::dbConnect(driver, connectionString)
    } else {
      connection <- RJDBC::dbConnect(driver, connectionString, user, password)
    }
    if (!missing(schema) && !is.null(schema)) {
      RJDBC::dbSendUpdate(connection, paste("USE", schema))
    }
    attr(connection, "dbms") <- dbms
    return(connection)
  }
}

dbConnectUsingProperties <- function(drv, url, ...) {
  properties <- list(...)
  p <- rJava::.jnew("java/util/Properties")
  for (i in 1:length(properties)) {
    rJava::.jcall(p,
                  "Ljava/lang/Object;",
                  "setProperty",
                  names(properties)[i],
                  as.character(properties[[i]])[1])
  }
  jc <- rJava::.jcall(drv@jdrv, "Ljava/sql/Connection;", "connect", as.character(url)[1], p)
  if (rJava::is.jnull(jc)) {
    x <- rJava::.jgetEx(TRUE)
    if (rJava::is.jnull(x)) {
      stop("Unable to connect JDBC to ", url)
    } else {
      stop("Unable to connect JDBC to ", url, " (", rJava::.jcall(x, "S", "getMessage"), ")")
    }
  }
  connection <- new("JDBCConnection", jc = jc, identifier.quote = drv@identifier.quote)
  return(connection)
}
