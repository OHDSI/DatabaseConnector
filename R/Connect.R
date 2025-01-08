# @file Connect.R
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

checkIfDbmsIsSupported <- function(dbms) {
  supportedDbmss <- c(
    "oracle",
    "hive",
    "postgresql",
    "redshift",
    "sql server",
    "pdw",
    "netezza",
    "impala",
    "bigquery",
    "sqlite",
    "sqlite extended",
    "spark",
    "snowflake",
    "synapse",
    "duckdb"
  )
  deprecated <- c(
    "hive",
    "impala",
    "netezza",
    "pdw"
  )
  if (!dbms %in% supportedDbmss) {
    abort(sprintf(
      "DBMS '%s' not supported. Please use one of these values: '%s'",
      dbms,
      paste(supportedDbmss, collapse = "', '")
    ))
  }
  if (dbms %in% deprecated) {
    warn(sprintf(
      paste(c("DBMS '%s' has been deprecated. Current functionality is provided as is.",
            "No futher support will be provided.",
            "Please consider switching to a different database platform."),
            collapse = " "),
      dbms),
      .frequency = "regularly",
      .frequency_id = "deprecated_dbms"
    )
  }
}

checkDetailValidation <- function(connectionDetails, name) {
  tryCatch(
    invisible(connectionDetails[[name]]()),
    error = function(e) {
      abort(
        sprintf(
          paste(
            "Unable to evaluate the '%s' argument of the connection details.",
            "Most likely this is because the connection is being established",
            "in a separate R thread that has no access to variables in the main",
            "thread. This problem will not occur when using",
            "a secure approach to credentials such as keyring. See",
            "?createConnectionDetails for more information."
          ),
          name
        )
      )
    }
  )
}

assertDetailsCanBeValidated <- function(connectionDetails) {
  checkDetailValidation(connectionDetails, "server")
  checkDetailValidation(connectionDetails, "port")
  checkDetailValidation(connectionDetails, "user")
  checkDetailValidation(connectionDetails, "password")
  checkDetailValidation(connectionDetails, "connectionString")
}

#' @title
#' createConnectionDetails
#'
#' @description
#' Creates a list containing all details needed to connect to a
#' database. There are three ways to call this function:
#' 
#' - `createConnectionDetails(dbms, user, password, server, port, extraSettings, oracleDriver, pathToDriver)`
#' - `createConnectionDetails(dbms, connectionString, pathToDriver)`
#' - `createConnectionDetails(dbms, connectionString, user, password, pathToDriver)`
#'
#' @usage
#' NULL
#'
#' @template Dbms
#' @template DefaultConnectionDetails
#'
#' @details
#' This function creates a list containing all details needed to connect to a database. The list can
#' then be used in the [connect()] function.
#'
#' It is highly recommended to use a secure approach to storing credentials, so not to have your
#' credentials in plain text in your R scripts. The examples demonstrate how to use the
#' `keyring` package.
#'
#' @return
#' A list with all the details needed to connect to a database.
#'
#' @examples
#' \dontrun{
#' # Needs to be done only once on a machine. Credentials will then be stored in
#' # the operating system's secure credential manager:
#' keyring::key_set_with_value("server", password = "localhost/postgres")
#' keyring::key_set_with_value("user", password = "root")
#' keyring::key_set_with_value("password", password = "secret")
#'
#' # Create connection details using keyring. Note: the connection details will
#' # not store the credentials themselves, but the reference to get the credentials.
#' connectionDetails <- createConnectionDetails(
#'   dbms = "postgresql",
#'   server = keyring::key_get("server"),
#'   user = keyring::key_get("user"),
#'   password = keyring::key_get("password"),
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

  class(result) <- c("ConnectionDetails", "DefaultConnectionDetails")
  return(result)
}


#' Create DBI connection details
#' 
#' @description 
#' For advanced users only. This function will allow `DatabaseConnector` to wrap any DBI driver. Using a driver that 
#' `DatabaseConnector` hasn't been tested with may give unpredictable performance. Use at your own risk. No
#' support will be provided. 
#'
#' @template Dbms
#' @param drv   An object that inherits from DBIDriver, or an existing DBIConnection object
#'              (in order to clone an existing connection).
#' @param ...   authentication arguments needed by the DBMS instance; these typically
#'              include user, password, host, port, dbname, etc. For details see the appropriate DBIDriver
#'
#' @return
#' A list with all the details needed to connect to a database.
#'
#' @export
createDbiConnectionDetails <- function(dbms, drv, ...) {
  result <- list(...)
  result$dbms <- dbms
  result$drv <- drv
  class(result) <- c("ConnectionDetails", "DbiConnectionDetails")
  return(result)
}

#' @title
#' connect
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
#' connectionDetails <- createConnectionDetails(
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
  if (missing(connectionDetails) || is.null(connectionDetails)) {
    # warn("Use of dbms, server, etc. when calling connect() is deprecated. Use connectionDetails instead.")
    connectionDetails <- createConnectionDetails(
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
  jarPath <- findPathToJar("^[Rr]edshift.*\\.jar$", connectionDetails$pathToDriver)
  if (grepl("RedshiftJDBC42", jarPath) || grepl("redshift-jdbc42", jarPath)) {
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

connectNetezza <- function(connectionDetails) {
  inform("Connecting using Netezza driver")
  jarPath <- findPathToJar("^nzjdbc\\.jar$", connectionDetails$pathToDriver)
  driver <- getJbcDriverSingleton("org.netezza.Driver", jarPath)
  if (is.null(connectionDetails$connectionString()) || connectionDetails$connectionString() == "") {
    if (!grepl("/", connectionDetails$server())) {
      abort("Error: database name not included in server string but is required for Redshift Please specify server as <host>/<database>")
    }
    parts <- unlist(strsplit(connectionDetails$server(), "/"))
    host <- parts[1]
    database <- parts[2]
    if (is.null(connectionDetails$port())) {
      port <- "5480"
    } else {
      port <- connectionDetails$port()
    }
    connectionString <- paste0("jdbc:netezza://", host, ":", port, "/", database)
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
  return(connection)
}

connectImpala <- function(connectionDetails) {
  inform("Connecting using Impala driver")
  jarPath <- findPathToJar("^ImpalaJDBC42\\.jar$", connectionDetails$pathToDriver)
  driver <- getJbcDriverSingleton("com.cloudera.impala.jdbc.Driver", jarPath)
  if (is.null(connectionDetails$connectionString()) || connectionDetails$connectionString() == "") {
    if (is.null(connectionDetails$port())) {
      port <- "21050"
    } else {
      port <- connectionDetails$port()
    }
    connectionString <- paste0("jdbc:impala://", connectionDetails$server(), ":", port)
    if (!is.null(connectionDetails$extraSettings)) {
      connectionString <- paste(connectionString, connectionDetails$extraSettings, sep = ";")
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
  return(connection)
}

connectHive <- function(connectionDetails) {
  inform("Connecting using Hive driver")
  jarPath <- findPathToJar("^hive-jdbc-([.0-9]+-)*standalone\\.jar$", connectionDetails$pathToDriver)
  driver <- getJbcDriverSingleton("org.apache.hive.jdbc.HiveDriver", jarPath)

  if (is.null(connectionDetails$connectionString()) || connectionDetails$connectionString() == "") {
    connectionString <- paste0("jdbc:hive2://", connectionDetails$server(), ":", connectionDetails$port(), "/")
    if (!is.null(connectionDetails$extraSettings)) {
      connectionString <- paste(connectionString, connectionDetails$extraSettings, sep = ";")
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

connectSparkUsingOdbc <- function(connectionDetails) {
  inform("Connecting using Spark ODBC driver")
  ensure_installed("odbc")
  dbiConnectionDetails <- createDbiConnectionDetails(
    dbms = connectionDetails$dbms,
    drv = odbc::odbc(),
    Driver = "Simba Spark ODBC Driver",
    Host = connectionDetails$server(),
    uid = connectionDetails$user(),
    pwd = connectionDetails$password(),
    Port = connectionDetails$port()
    # UseNativeQuery=1
  )
  if (!is.null(connectionDetails$extraSettings)) {
    dbiConnectionDetails <- append(
      dbiConnectionDetails,
      connectionDetails$extraSettings
    )
  }
  connection <- connectUsingDbi(dbiConnectionDetails)
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
      "CLIENT_TIMESTAMP_TYPE_MAPPING"="TIMESTAMP_NTZ",
      "QUOTED_IDENTIFIERS_IGNORE_CASE"="TRUE"
    )
  }
  return(connection)
}

connectSqlite <- function(connectionDetails) {
  inform("Connecting using SQLite driver")
  ensure_installed("RSQLite")
  connection <- connectUsingDbi(
    createDbiConnectionDetails(
      dbms = connectionDetails$dbms,
      drv = RSQLite::SQLite(),
      dbname = connectionDetails$server(),
      extended_types = (connectionDetails$dbms == "sqlite extended")
    )
  )
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
  class <- getClassDef("DatabaseConnectorJdbcConnection", where = class_cache, inherits = FALSE)
  if (is.null(class) || methods::isVirtualClass(class)) {
    setClass("DatabaseConnectorJdbcConnection",
             contains = "DatabaseConnectorConnection", 
             slots = list(jConnection = "jobjRef"),
             where = class_cache)
  }
  connection <- new("DatabaseConnectorJdbcConnection",
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

connectDuckdb <- function(connectionDetails) {
  inform("Connecting using DuckDB driver")
  ensure_installed("duckdb")
  connection <- connectUsingDbi(
    createDbiConnectionDetails(
      dbms = connectionDetails$dbms,
      drv = duckdb::duckdb(),
      dbdir = connectionDetails$server(),
      bigint = "integer64"
    )
  )
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
  if (connection@dbms == "duckdb") {
    DBI::dbDisconnect(connection@dbiConnection, shutdown = TRUE)
  } else {
    DBI::dbDisconnect(connection@dbiConnection)
  }
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

#' Get the database platform from a connection
#'
#' The SqlRender package provides functions that translate SQL from OHDSI-SQL to
#' a target SQL dialect. These function need the name of the database platform to
#' translate to. The `dbms` function returns the dbms for any DBI
#' connection that can be passed along to SqlRender translation functions (see example).
#'
#' @template Connection
#'
#' @return The name of the database (dbms) used by SqlRender
#' @export
#'
#' @examples
#' library(DatabaseConnector)
#' con <- connect(dbms = "sqlite", server = ":memory:")
#' dbms(con)
#' #> [1] "sqlite"
#' SqlRender::translate("DATEADD(d, 365, dateColumn)", targetDialect = dbms(con))
#' #> "CAST(STRFTIME('%s', DATETIME(dateColumn, 'unixepoch', (365)||' days')) AS REAL)"
#' disconnect(con)
dbms <- function(connection) {
  if (is(connection, "Pool")) {
    connection <- pool::poolCheckout(connection)
    on.exit(pool::poolReturn(connection))
  }

  if (!inherits(connection, "DBIConnection")) abort("connection must be a DBIConnection")

  if (!is.null(attr(connection, "dbms"))) {
    return(attr(connection, "dbms"))
  }

  switch(class(connection),
    "Microsoft SQL Server" = "sql server",
    "PqConnection" = "postgresql",
    "RedshiftConnection" = "redshift",
    "BigQueryConnection" = "bigquery",
    "SQLiteConnection" = "sqlite",
    "duckdb_connection" = "duckdb"
    # add mappings from various DBI connection classes to SqlRender dbms here
  )
}
