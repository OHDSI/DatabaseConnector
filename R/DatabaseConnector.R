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

#' @title connect
#'
#' @description
#' \code{connect} creates a connection to a database server.
#'
#' @details
#' This function loads the appropriate database driver, which is included in the library, and connects to the database server.
#' 
#' @param dbms              The type of DBMS running on the server. Valid values are
#' \itemize{
#'   \item{"mysql" for MySQL}
#'   \item{"oracle" for Oracle}
#'   \item{"postgresql" for PostgreSQL}
#'   \item{"sql server" for Microsoft SQL Server}
#' } 
#' @param user				The user name used to access the server. For Microsoft SQL Server, this parameter can be omitted to 
#' use Windows integrated security, which will require installation of the JDBC driver for SQL Server. When not using Windows integrated 
#' security, the domain can be specified using <mydomain>/<user>.
#' @param password			The password for that user
#' @param server			The name or IP-address of the server
#' @param schema			(optional) The name of the schema to connect to
#' @return              
#' An object that extends \code{DBIConnection} in a database-specific manner. This object is used to direct commands to the database engine. 
#' @examples
#' conn <- connect(dbms="mysql", server="localhost",user="root",password="xxx",schema="cdm_v4")
#' dbGetQuery(conn,"SELECT COUNT(*) FROM person")
#' dbDisconnect(conn)
#' 
#' conn <- connect(dbms="sql server", server="RNDUSRDHIT06.jnj.com",schema="Vocabulary")
#' dbGetQuery(conn,"SELECT COUNT(*) FROM concept")
#' dbDisconnect(conn)
#' 
#' conn <- connect(dbms="oracle", server="127.0.0.1:1521/xe",user="system",password="xxx",schema="test")
#' dbGetQuery(conn,"SELECT COUNT(*) FROM test_table")
#' dbDisconnect(conn)
#' @export
connect <- function(dbms = "mysql", user, password, server, port, schema){
	if (dbms == "mysql"){
		print("Connecting using MySQL driver")
		pathToJar <- system.file("java", "mysql-connector-java-5.1.18-bin.jar", package="DatabaseConnector")
		driver <- JDBC("com.mysql.jdbc.Driver", pathToJar, identifier.quote="`")
		connection <- dbConnect(driver, paste("jdbc:mysql://",server,sep=""), user, password)
		if (!missing(schema))
			dbSendUpdate(connection,paste("USE",schema))
		return(connection)
	}	
	if (dbms == "sql server"){
		if (missing(user)) { # Using Windows integrated security
			print("Connecting using SQL Server driver using Windows integrated security")
			pathToJar <- system.file("java", "sqljdbc4.jar", package="DatabaseConnector")
			driver <- JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver", pathToJar)
			connection <- dbConnect(driver, paste("jdbc:sqlserver://",server,";integratedSecurity=true",sep=""))
		} else { # Using regular user authentication
			print("Connecting using SQL Server driver")
			pathToJar <- system.file("java", "jtds-1.3.0.jar", package="DatabaseConnector")
			driver <- JDBC("net.sourceforge.jtds.jdbc.Driver", pathToJar)
			if (grepl("/",user)){
				parts <-  unlist(strsplit(user,"/"))
				connection <- dbConnect(driver, paste("jdbc:jtds:sqlserver://",server,";domain=",parts[1],sep=""), parts[2], password)
			} else {
				connection <- dbConnect(driver, paste("jdbc:jtds:sqlserver://",server,sep=""), user, password)
			}
		}
		if (!missing(schema))
			dbSendUpdate(connection,paste("USE",schema))
		return(connection)
	}
	if (dbms == "oracle"){
		print("Connecting using Oracle driver")
		pathToJar <- system.file("java", "ojdbc6.jar", package="DatabaseConnector")
		driver <- JDBC("oracle.jdbc.driver.OracleDriver", pathToJar)
		
		# First try OCI driver:
		result <- class(try(connection <- dbConnect(driver, paste("jdbc:oracle:oci8:@",server,sep=""), user, password),silent=TRUE))[1]
		
		# Next, try THIN driver:
		if (result =="try-error"){
			if (missing(port))
				port = "1521"
			host = "127.0.0.1"	
			sid = server
			if (grepl("/",server)){
				parts <-  unlist(strsplit(server,"/"))
				host = parts[1]
				sid = parts[2]
				
				if (grepl(":",host)){
					parts <-  unlist(strsplit(host,":"))
					host = parts[1]
					port = parts[2]
				}
			}
			print(paste("jdbc:oracle:thin:@",server,":",port,":",sid ,sep=""))
			connection <- dbConnect(driver, paste("jdbc:oracle:thin:@",host,":",port,":",sid ,sep=""), user, password)
		}
		if (!missing(schema))
			dbSendUpdate(connection,paste("ALTER SESSION SET current_schema = ",schema))
		return(connection)
	}
}

#' @title createConnectionDetails
#'
#' @description
#' \code{createConnectionDetails} creates a list containing all details needed to connect to a database.
#'
#' @details
#' This function creates a list containing all details needed to connect to a database. The list can then be used in the 
#' \code{connectUsingConnectionDetails} function. 
#' 
#' @param dbms              The type of DBMS running on the server. Valid values are
#' \itemize{
#'   \item{"mysql" for MySQL}
#'   \item{"oracle" for Oracle}
#'   \item{"postgresql" for PostgreSQL}
#'   \item{"sql server" for Microsoft SQL Server}
#' } 
#' @param user				The user name used to access the server. For Microsoft SQL Server, this parameter can be omitted to 
#' use Windows integrated security, which will require installation of the JDBC driver for SQL Server. When not using Windows integrated 
#' security, the domain can be specified using <mydomain>/<user>.
#' @param password			The password for that user
#' @param server			The name or IP-address of the server
#' @param schema			(optional) The name of the schema to connect to
#' @return              
#' A list with all the details needed to connect to a database.
#' @examples
#' connectionDetails <- createConnectionDetails(dbms="mysql", server="localhost",user="root",password="F1r3starter",schema="cdm_v4")
#' conn <- connectUsingConnectionDetails(connectionDetails)
#' dbGetQuery(conn,"SELECT COUNT(*) FROM person")
#' dbDisconnect(conn)
#' @export
createConnectionDetails <- function(dbms = "mysql", user, password, server, port, schema){
	return(as.list(match.call()))
}

#' @title connectUsingConnectionDetails
#'
#' @description
#' \code{connectUsingConnectionDetails} connects to a database given the provided details. 
#'
#' @details
#' This function connects to a database given the provided details. The details should be in a list created by
#' \code{createConnectionDetails}.
#' 
#' @param connectionDetails              a list created by the \code{createConnectionDetails} function.
#' @return              
#' An object that extends \code{DBIConnection} in a database-specific manner. This object is used to direct commands to the database engine. 
#' @examples
#' connectionDetails <- createConnectionDetails(dbms="mysql", server="localhost",user="root",password="xxx",schema="cdm_v4")
#' conn <- connectUsingConnectionDetails(connectionDetails)
#' dbGetQuery(conn,"SELECT COUNT(*) FROM person")
#' dbDisconnect(conn)
#' @export
connectUsingConnectionDetails <- function(connectionDetails){
	do.call("connect",connectionDetails)
}

