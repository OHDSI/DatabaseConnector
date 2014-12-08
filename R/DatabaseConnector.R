# @file DatabaseConnector.R
#
# Copyright 2014 Observational Health Data Sciences and Informatics
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
#
# @author Observational Health Data Sciences and Informatics
# @author Martijn Schuemie
# @author Marc Suchard

.onLoad <- function(libname, pkgname){
  jdbcDrivers <<- new.env()
}


#' @title createConnectionDetails
#'
#' @description
#' \code{createConnectionDetails} creates a list containing all details needed to connect to a database.
#' 
#' @param dbms              The type of DBMS running on the server. Valid values are
#' \itemize{
#'   \item{"mysql" for MySQL}
#'   \item{"oracle" for Oracle}
#'   \item{"postgresql" for PostgreSQL}
#'   \item{"redshift" for Amazon Redshift}   
#'   \item{"sql server" for Microsoft SQL Server}
#' } 
#' @param user				The user name used to access the server.
#' @param password		The password for that user
#' @param server			The name of the server
#' @param port				(optional) The port on the server to connect to
#' @param schema			(optional) The name of the schema to connect to
#'
#' @details
#' This function creates a list containing all details needed to connect to a database. The list can then be used in the 
#' \code{connect} function. 
#' 
#' Depending on the DBMS, the function arguments have slightly different interpretations:
#' 
#' MySQL:
#' \itemize{
#'   \item \code{user}. The user name used to access the server
#'   \item \code{password}. The password for that user
#'   \item \code{server}. The host name of the server
#'   \item \code{port}. Specifies the port on the server (default = 3306)
#'   \item \code{schema}. The database containing the tables
#' }
#' 
#' Oracle:
#' \itemize{
#'   \item \code{user}. The user name used to access the server
#'   \item \code{password}. The password for that user
#'   \item \code{server}. This field contains the SID, or host and servicename or SID: '<sid>', '<host>/<sid>', '<host>/<service name>'
#'   \item \code{port}. Specifies the port on the server (default = 1521)
#'   \item \code{schema}. This field contains the schema (i.e. 'user' in Oracle terms) containing the tables
#' }
#' Microsoft SQL Server:
#' \itemize{
#'   \item \code{user}. The user used to log in to the server. If the user is not specified, Windows Integrated Security will be used, which requires the SQL Server JDBC drivers to be installed (see details below). Optionally, the domain can be specified as <domain>/<user> (e.g. 'MyDomain/Joe')
#'   \item \code{password}. The password used to log on to the server
#'   \item \code{server}. This field contains the host name of the server
#'   \item \code{port}. Not used for SQL Server
#'   \item \code{schema}. The database containing the tables
#' }
#' 
#' PostgreSQL:
#' \itemize{
#'   \item \code{user}. The user used to log in to the server 
#'   \item \code{password}. The password used to log on to the server
#'   \item \code{server}. This field contains the host name of the server and the database holding the relevant schemas: <host>/<database>
#'   \item \code{port}. Specifies the port on the server (default = 5432)
#'   \item \code{schema}. The schema containing the tables. 
#' }
#' Redshift:
#' \itemize{
#'   \item \code{user}. The user used to log in to the server 
#'   \item \code{password}. The password used to log on to the server
#'   \item \code{server}. This field contains the host name of the server and the database holding the relevant schemas: <host>/<database>
#'   \item \code{port}. Specifies the port on the server (default = 5432)
#'   \item \code{schema}. The schema containing the tables. 
#'}
#' 
#' Netezza:
#' \itemize{
#'   \item \code{user}. The user used to log in to the server 
#'   \item \code{password}. The password used to log on to the server
#'   \item \code{server}. This field contains the host name of the server and the database holding the relevant schemas: <host>/<database>
#'   \item \code{port}. Specifies the port on the server (default = 5480)
#'   \item \code{schema}. The schema containing the tables. 
#' }
#' To be able to use Windows authentication for SQL Server, you have to install the JDBC driver. Download the .exe from 
#' \href{http://www.microsoft.com/en-us/download/details.aspx?displaylang=en&id=11774}{Microsoft} and run it, thereby extracting its 
#' contents to a folder. In the extracted folder you will find the file sqljdbc_4.0/enu/auth/x64/sqljdbc_auth.dll (64-bits) or 
#' sqljdbc_4.0/enu/auth/x86/sqljdbc_auth.dll (32-bits), which needs to be moved to location on the system path, for example 
#' to c:/windows/system32.
#' 
#' In order to enable Netezza support, place your Netezza jdbc driver at \code{inst/java/nzjdbc.jar} in this package.
#' 
#' @return              
#' A list with all the details needed to connect to a database.
#' @examples \dontrun{
#'   connectionDetails <- createConnectionDetails(dbms="mysql", server="localhost",user="root",password="blah",schema="cdm_v4")
#'   conn <- connect(connectionDetails)
#'   dbGetQuery(conn,"SELECT COUNT(*) FROM person")
#'   dbDisconnect(conn)
#' }
#' @export
createConnectionDetails <- function(dbms = "sql server", user, password, server, port, schema){
  result <- c(list(as.character(match.call()[[1]])),lapply(as.list(match.call())[-1],function(x) eval(x,envir=sys.frame(-3))))
  class(result) <- "connectionDetails"
  return(result)
}

#' @title connect
#'
#' @description
#' \code{connect} creates a connection to a database server.
#'
#' @usage 
#' connect(dbms = "sql server", user, password, server, port, schema)
#' connect(connectionDetails)
#' 
#' @param dbms              The type of DBMS running on the server. Valid values are
#' \itemize{
#'   \item{"mysql" for MySQL}
#'   \item{"oracle" for Oracle}
#'   \item{"postgresql" for PostgreSQL}
#'   \item{"redshift" for Amazon Redshift}
#'   \item{"netezza" for Netezza}
#'   \item{"sql server" for Microsoft SQL Server}
#' } 
#' @param user  			The user name used to access the server.
#' @param password		The password for that user
#' @param server			The name of the server
#' @param port				(optional) The port on the server to connect to
#' @param schema			(optional) The name of the schema to connect to
#' @param connectionDetails  an object of class \code{connectionDetails}
#'
#' @details
#' This function creates a list containing all details needed to connect to a database. The list can then be used in the 
#' \code{connect} function. 
#' 
#' Depending on the DBMS, the function arguments have slightly different interpretations:
#' 
#' MySQL:
#' \itemize{
#'   \item \code{user}. The user name used to access the server
#'   \item \code{password}. The password for that user
#'   \item \code{server}. The host name of the server
#'   \item \code{port}. Specifies the port on the server (default = 3306)
#'   \item \code{schema}. The database containing the tables
#' }
#' 
#' Oracle:
#' \itemize{
#'   \item \code{user}. The user name used to access the server
#'   \item \code{password}. The password for that user
#'   \item \code{server}. This field contains the SID, or host and servicename or SID: '<sid>', '<host>/<sid>', '<host>/<service name>'
#'   \item \code{port}. Specifies the port on the server (default = 1521)
#'   \item \code{schema}. This field contains the schema (i.e. 'user' in Oracle terms) containing the tables
#' }
#' Microsoft SQL Server:
#' \itemize{
#'   \item \code{user}. The user used to log in to the server. If the user is not specified, Windows Integrated Security will be used, which requires the SQL Server JDBC drivers to be installed (see details below). Optionally, the domain can be specified as <domain>/<user> (e.g. 'MyDomain/Joe')
#'   \item \code{password}. The password used to log on to the server
#'   \item \code{server}. This field contains the host name of the server
#'   \item \code{port}. Not used for SQL Server
#'   \item \code{schema}. The database containing the tables
#' }
#' 
#' PostgreSQL:
#' \itemize{
#'   \item \code{user}. The user used to log in to the server 
#'   \item \code{password}. The password used to log on to the server
#'   \item \code{server}. This field contains the host name of the server and the database holding the relevant schemas: <host>/<database>
#'   \item \code{port}. Specifies the port on the server (default = 5432)
#'   \item \code{schema}. The schema containing the tables. 
#' }
#' Redshift:
#' \itemize{
#'   \item \code{user}. The user used to log in to the server 
#'   \item \code{password}. The password used to log on to the server
#'   \item \code{server}. This field contains the host name of the server and the database holding the relevant schemas: <host>/<database>
#'   \item \code{port}. Specifies the port on the server (default = 5432)
#'   \item \code{schema}. The schema containing the tables. 
#'}
#' 
#' Netezza:
#' \itemize{
#'   \item \code{user}. The user used to log in to the server 
#'   \item \code{password}. The password used to log on to the server
#'   \item \code{server}. This field contains the host name of the server and the database holding the relevant schemas: <host>/<database>
#'   \item \code{port}. Specifies the port on the server (default = 5480)
#'   \item \code{schema}. The schema containing the tables. 
#' }
#' To be able to use Windows authentication for SQL Server, you have to install the JDBC driver. Download the .exe from 
#' \href{http://www.microsoft.com/en-us/download/details.aspx?displaylang=en&id=11774}{Microsoft} and run it, thereby extracting its 
#' contents to a folder. In the extracted folder you will find the file sqljdbc_4.0/enu/auth/x64/sqljdbc_auth.dll (64-bits) or 
#' sqljdbc_4.0/enu/auth/x86/sqljdbc_auth.dll (32-bits), which needs to be moved to location on the system path, for example 
#' to c:/windows/system32.
#' 
#' In order to enable Netezza support, place your Netezza jdbc driver at \code{inst/java/nzjdbc.jar} in this package.
#' 
#' @return              
#' An object that extends \code{DBIConnection} in a database-specific manner. This object is used to direct commands to the database engine. 
#' 
#' @examples \dontrun{
#'   conn <- connect(dbms="mysql", server="localhost",user="root",password="xxx",schema="cdm_v4")
#'   dbGetQuery(conn,"SELECT COUNT(*) FROM person")
#'   dbDisconnect(conn)
#' 
#'   conn <- connect(dbms="sql server", server="RNDUSRDHIT06.jnj.com",schema="Vocabulary")
#'   dbGetQuery(conn,"SELECT COUNT(*) FROM concept")
#'   dbDisconnect(conn)
#' 
#'   conn <- connect(dbms="oracle", server="127.0.0.1/xe",user="system",password="xxx",schema="test")
#'   dbGetQuery(conn,"SELECT COUNT(*) FROM test_table")
#'   dbDisconnect(conn)
#' }
#' @export
connect <- function(...){
  UseMethod("connect") 
}

# Singleton pattern to ensire driver is instantiated only once
jdbcSingleton <- function(driverClass = "", classPath = "", identifier.quote = NA){
  key <- paste(driverClass,classPath)
  if (key %in% ls(jdbcDrivers)){
    driver <- get(key,jdbcDrivers)
    if (is.jnull(driver@jdrv)){
      driver <- JDBC(driverClass, classPath, identifier.quote)
      assign(key,driver,envir = jdbcDrivers)
    }
  } else {
    driver <- JDBC(driverClass, classPath, identifier.quote)
    assign(key,driver,envir = jdbcDrivers)    
  }
  driver
}

#' @export
connect.default <- function(dbms = "sql server", user, password, server, port, schema){
  if (dbms == "mysql"){
    writeLines("Connecting using MySQL driver")
    if (missing(port)|| is.null(port))
      port = "3306"
    pathToJar <- system.file("java", "mysql-connector-java-5.1.30-bin.jar", package="DatabaseConnector")
    driver <- jdbcSingleton("com.mysql.jdbc.Driver", pathToJar, identifier.quote="`")
    connection <- dbConnect(driver, paste("jdbc:mysql://",server,":",port,"/?useCursorFetch=true",sep=""), user, password)
    if (!missing(schema) && !is.null(schema))
      dbSendUpdate(connection,paste("USE",schema))
    attr(connection,"dbms") <- dbms
    return(connection)
  }	
  if (dbms == "sql server"){
    if (missing(user) || is.null(user)) { # Using Windows integrated security
      writeLines("Connecting using SQL Server driver using Windows integrated security")
      pathToJar <- system.file("java", "sqljdbc4.jar", package="DatabaseConnector")
      driver <- jdbcSingleton("com.microsoft.sqlserver.jdbc.SQLServerDriver", pathToJar)
      connection <- dbConnect(driver, paste("jdbc:sqlserver://",server,";integratedSecurity=true",sep=""))
    } else { # Using regular user authentication
      writeLines("Connecting using SQL Server driver")
      pathToJar <- system.file("java", "jtds-1.2.7.jar", package="DatabaseConnector")
      driver <- jdbcSingleton("net.sourceforge.jtds.jdbc.Driver", pathToJar)
      if (grepl("/",user)){
        parts <-  unlist(strsplit(user,"/"))
        connection <- dbConnect(driver, paste("jdbc:jtds:sqlserver://",server,";domain=",parts[1],sep=""), parts[2], password)
      } else {
        connection <- dbConnect(driver, paste("jdbc:jtds:sqlserver://",server,sep=""), user, password)
      }
    }
    if (!missing(schema) && !is.null(schema))
      dbSendUpdate(connection,paste("USE",schema))
    attr(connection,"dbms") <- dbms
    return(connection)
  }
  if (dbms == "oracle"){
    writeLines("Connecting using Oracle driver")
    pathToJar <- system.file("java", "ojdbc6.jar", package="DatabaseConnector")
    driver <- jdbcSingleton("oracle.jdbc.driver.OracleDriver", pathToJar)
    
    # First THIN driver:
    if (missing(port)|| is.null(port))
      port = "1521"
    host = "127.0.0.1"	
    sid = server
    if (grepl("/",server)){
      parts <-  unlist(strsplit(server,"/"))
      host = parts[1]
      sid = parts[2]
    }
    result <- class(try(connection <- dbConnect(driver, paste("jdbc:oracle:thin:@",host,":",port,":",sid ,sep=""), user, password),silent=TRUE))[1]
    
    
    # Next try OCI driver:
    if (result =="try-error")
      connection <- dbConnect(driver, paste("jdbc:oracle:oci8:@",server,sep=""), user, password)    
    
    if (!missing(schema) && !is.null(schema))
      dbSendUpdate(connection,paste("ALTER SESSION SET current_schema = ",schema))
    attr(connection,"dbms") <- dbms
    return(connection)
  }
  if (dbms == "postgresql"){
    writeLines("Connecting using PostgreSQL driver")
    if (!grepl("/",server))
      stop("Error: database name not included in server string but is required for PostgreSQL. Please specify server as <host>/<database>")
    parts <-  unlist(strsplit(server,"/"))
    host = parts[1]
    database = parts[2]
    if (missing(port)|| is.null(port))
      port = "5432"
    pathToJar <- system.file("java", "postgresql-9.3-1101.jdbc4.jar", package="DatabaseConnector")
    driver <- jdbcSingleton("org.postgresql.Driver", pathToJar, identifier.quote="`")
    connection <- dbConnect(driver, paste("jdbc:postgresql://",host,":",port,"/",database,sep=""), user, password)
    if (!missing(schema) && !is.null(schema))
      dbSendUpdate(connection,paste("SET search_path TO ",schema))
    attr(connection,"dbms") <- dbms
    return(connection)
  }	
  if (dbms == "redshift"){
    writeLines("Connecting using Redshift driver")
    # Redshift uses old version of the PostgreSQL JDBC driver
    if (!grepl("/",server))
      stop("Error: database name not included in server string but is required for Redshift Please specify server as <host>/<database>")
    parts <-  unlist(strsplit(server,"/"))
    host = parts[1]
    database = parts[2]
    if (missing(port)|| is.null(port))
      port = "5432"
    pathToJar <- system.file("java", "postgresql-8.4-704.jdbc4.jar", package="DatabaseConnector")
    driver <- jdbcSingleton("org.postgresql.Driver", pathToJar, identifier.quote="`")
    connection <- dbConnect(driver, paste("jdbc:postgresql://",host,":",port,"/",database,sep=""), user, password)
    if (!missing(schema) && !is.null(schema))
      dbSendUpdate(connection,paste("SET search_path TO ",schema))
    attr(connection,"dbms") <- dbms
    return(connection)
  }	
  if (dbms == "netezza"){
    writeLines("Connecting using Netezza driver")
    if (!grepl("/",server))
      stop("Error: database name not included in server string but is required for Netezza. Please specify server as <host>/<database>")
    parts <-  unlist(strsplit(server,"/"))
    host = parts[1]
    database = parts[2]
    if (missing(port)|| is.null(port))
      port = "5480"
    pathToJar <- system.file("java", "nzjdbc.jar", package="DatabaseConnector")
    driver <- jdbcSingleton("org.netezza.Driver", pathToJar, identifier.quote="`")
    connection <- dbConnect(driver, paste("jdbc:netezza://",host,":",port,"/",database,sep=""), user, password)
    if (!missing(schema) && !is.null(schema))
      dbSendUpdate(connection,paste("SET search_path TO ",schema))
    attr(connection,"dbms") <- dbms
    return(connection)
  }	
}

#' @export
connect.connectionDetails <- function(connectionDetails){
  dbms = connectionDetails$dbms
  user = connectionDetails$user
  password = connectionDetails$password
  server = connectionDetails$server
  port = connectionDetails$port
  schema = connectionDetails$schema
  connect(dbms, user, password, server, port, schema)
}
