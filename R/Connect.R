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

# Connection helper functions --------------------------------------------------
checkIfDbmsIsSupported <- function(dbms) {
  supportedDbmss <- c(
    "oracle",
    "postgresql",
    "redshift",
    "sql server",
    "pdw",
    "netezza",
    "bigquery",
    # "sqlite",
    # "sqlite extended",
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
  
  drv <- switch (dbms,
    "oracle"          = Oracle(pathToDriver),
    "postgresql"      = PostgreSQL(pathToDriver),
    "redshift"        = Redshift(pathToDriver),
    "sql server"      = SqlServer(pathToDriver),
    "pdw"             = Pdw(pathToDriver),
    "netezza"         = Netezza(pathToDriver),
    "bigquery"        = BigQuery(pathToDriver),
    # "impala"          = Impala(pathToDriver),
    # "hive"            = Hive(pathToDriver),
    # "sqlite"          = SQLite(extended_types = FALSE),
    # "sqlite extended" = SQLite(extended_types = TRUE),
    "spark"           = Spark(pathToDriver),
  )
  
   dbConnect(drv = drv, 
             user = user, 
             port = port, 
             password = password, 
             extraSettings = extraSettings, 
             connectionString = connectionString)
}

# JDBC connection backend ------------------------------------------------------

generateRandomString <- function(length = 20) {
  return(paste(sample(c(letters, 0:9), length, TRUE), collapse = ""))
}

setPathToDll <- function() {
  pathToDll <- Sys.getenv("PATH_TO_AUTH_DLL")
  if (pathToDll != "") {
    inform(paste("Looking for authentication DLL in path specified in PATH_TO_AUTH_DLL:", pathToDll))
    rJava::J("org.ohdsi.databaseConnector.Authentication")$addPathToJavaLibrary(pathToDll)
  }
}

connectUsingJdbcDriver <- function(jdbcDriver,
                                   url,
                                   identifierQuote = '"',
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
  
  connectionClass <- switch (dbms,
                             "oracle"     = "Oracle",
                             "postgresql" = "PostgreSQL",
                             "redshift"   = "Redshift",
                             "sql server" = "Microsoft SQL Server",
                             "pdw"        = "Microsoft SQL Server",
                             "netezza"    = "Netezza",
                             "bigquery"   = "BigQuery",
                             # "impala"
                             # "hive"
                             "spark"      = "Spark",
                             "Unknown"    = "DatabaseConnectorJdbcConnection")

  connection <- new(connectionClass,
                    jConnection = jConnection,
                    identifierQuote = identifierQuote,
                    stringQuote = stringQuote,
                    dbms = dbms,
                    uuid = generateRandomString()
  )
  registerWithRStudio(connection)
  return(connection)
}

# dbConnect methods ------------------------------------------------------------

#' Create a connection to a DBMS
#'
#' @description
#' Connect to a database. This function is synonymous with the \code{\link{connect}} function. except
#' a dummy driver needs to be specified
#'
#' @param drv   The result of the \code{link{DatabaseConnectorDriver}} function
#' @param ...   Other parameters. These are the same as expected by the \code{\link{connect}} function.
#'
#' @return
#' Returns a DatabaseConnectorConnection object that can be used with most of the other functions in
#' this package.
#'
#' @examples
#' \dontrun{
#' conn <- dbConnect(DatabaseConnectorDriver(),
#'   dbms = "postgresql",
#'   server = "localhost/ohdsi",
#'   user = "joe",
#'   password = "secret"
#' )
#' querySql(conn, "SELECT * FROM cdm_synpuf.person;")
#' dbDisconnect(conn)
#' }
#'
#' @export
setMethod("dbConnect", "DatabaseConnectorDriver", function(drv, ...) {
  NextMethod("dbConnect", drv, ...)
})

## dbConnect methods for internal (jdbc) drivers -------------------------------

#' @template DbmsDetails
#' @rdname dbConnect
#' @export
setMethod("dbConnect", "SqlServerDriver", function(drv, user, port, password, extraSettings, connectionString) {
  dbms <- "sql server"
  jarPath <- findPathToJar("^mssql-jdbc.*.jar$|^sqljdbc.*\\.jar$", drv@pathToDriver)
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
                                         connectionString = connectionString,
                                         user = user,
                                         password = password,
                                         dbms = dbms
    )
  }
  attr(connection, "dbms") <- dbms
  return(connection)
})

setMethod("dbConnect", "PdwDriver", function(drv, user, port, password, extraSettings, connectionString) {
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
})


setMethod("dbConnect", "OracleDriver", function(drv, user, port, password, extraSettings, connectionString) {
  jarPath <- findPathToJar("^ojdbc.*\\.jar$", pathToDriver)
  driver <- getJbcDriverSingleton("oracle.jdbc.driver.OracleDriver", jarPath)
  dbms <- "oracle"
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
})

setMethod("dbConnect", "PostgreSQLDriver", function(drv, user, port, password, extraSettings, connectionString) {
  jarPath <- findPathToJar("^postgresql-.*\\.jar$", pathToDriver)
  driver <- getJbcDriverSingleton("org.postgresql.Driver", jarPath)
  dbms <- "postgresql"
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
                                         dbms = dbms,
                                         identifierQuote = '"',
                                         stringQuote = '"'
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
})

setMethod("dbConnnect", "RedshiftDriver", function(drv, user, port, password, extraSettings, connectionString) {
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
})

setMethod("dbConnect", "NetezzaDriver", function(drv, user, port, password, extraSettings, connectionString) {
  inform("Connecting using Netezza driver")
  jarPath <- findPathToJar("^nzjdbc\\.jar$", pathToDriver)
  driver <- getJbcDriverSingleton("org.netezza.Driver", jarPath)
  dbms <- "netezza"
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
})

setMethod("dbConnect", "ImpalaDriver", function(drv, user, port, password, extraSettings, connectionString) {
  inform("Connecting using Impala driver")
  jarPath <- findPathToJar("^ImpalaJDBC42\\.jar$", pathToDriver)
  driver <- getJbcDriverSingleton("com.cloudera.impala.jdbc.Driver", jarPath)
  dbms <- "impala"
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
})

setMethod("dbConnect", "HiveDriver", function(drv, user, port, password, extraSettings, connectionString) {
  inform("Connecting using Hive driver")
  jarPath <- findPathToJar("^hive-jdbc-standalone\\.jar$", pathToDriver)
  driver <- getJbcDriverSingleton("org.apache.hive.jdbc.HiveDriver", jarPath)
  dbms <- "hive"
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
})

setMethod("dbConnect", "BigQueryDriver", function(drv, user, port, password, extraSettings, connectionString) {
  inform("Connecting using BigQuery driver")
  
  files <- list.files(path = pathToDriver, full.names = TRUE)
  for (jar in files) {
    rJava::.jaddClassPath(jar)
  }
  
  jarPath <- findPathToJar("^GoogleBigQueryJDBC42\\.jar$", pathToDriver)
  driver <- getJbcDriverSingleton("com.simba.googlebigquery.jdbc42.Driver", jarPath)
  dbms <- "big query"
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
})

setMethod("dbConnect", "SparkDriver", function(drv, user, port, password, extraSettings, connectionString) {
  inform("Connecting using Spark driver")
  jarPath <- findPathToJar("^SparkJDBC42\\.jar$", pathToDriver)
  driver <- getJbcDriverSingleton("com.simba.spark.jdbc.Driver", jarPath)
  dbms <- "spark"
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
})

## dbConnect methods for external drivers---------------------------------------

setMethod("dbConnect", "RSQLiteDriver", function(drv, server) {
  ensure_installed("RSQLite")
  inform("Connecting using SQLite driver")
  
  dbiConnection <- DBI::dbConnect(RSQLite::SQLite(), dbname = server, extended_types = drv@extended_types)
  # This stores the connection as a slot. Alternatively we could also extend the dbi connection object.
  # Not sure how to handle sqlite
  connection <- new("DatabaseConnectorRSQLiteConnection",
                    server = server,
                    connection = dbiConnection,
                    identifierQuote = "'",
                    stringQuote = "'",
                    dbms = ifelse(drv@extended_types, "sqlite extended", "sqlite"),
                    uuid = generateRandomString()
  )
  attr(connection, "dbms") <- dbms
  registerWithRStudio(connection)
  return(connection)
})

# disconnect helper functions --------------------------------------------------
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
  dbDisconnect(connection)
}


# dbDisconnect methods ---------------------------------------------------------

#' @inherit
#' DBI::dbDisconnect title description params details references return seealso
#' @export
setMethod("dbDisconnect", "DatabaseConnectorJdbcConnection", function(connection) {
  if (rJava::is.jnull(connection@jConnection)) {
    warn("Connection is already closed")
  } else {
    unregisterWithRStudio(connection)
  }
  rJava::.jcall(connection@jConnection, "V", "close")
  invisible(TRUE)
})


# I'm not sure if this is needed.

#' @inherit
#' DBI::dbDisconnect title description params details references return seealso
#' @export
setMethod("dbDisconnect", "DatabaseConnectorConnection", function(connection) {
  DBI::dbDisconnect(connection)
  unregisterWithRStudio(connection)
  invisible(TRUE)
})

