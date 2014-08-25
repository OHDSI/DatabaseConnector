# @file HelperFunctions.R
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

#' Retrieve data from server as ffdf object.
#'
#' @description
#' This allows very large data sets to be retrieved without running out of memory.
#' 
#' @param connection	The connection to the database server.
#' @param query		    The SQL statement to retrieve the data
#' @param batchSize		The number of rows that will be retrieved at a time from the server. A larger 
#' batchSize means less calls to the server so better performance, but too large a batchSize could
#' lead to out-of-memory errors.
#'
#' @details
#' Retrieves data from the database server and stores it in an ffdf object. This allows very large
#' data sets to be retrieved without running out of memory.
#' 
#' @return              
#' A ffdf object containing the data. If there are 0 rows, a regular data frame is returned instead (ffdf cannot have 0 rows)
#' 
#' @examples \dontrun{
#'   connectionDetails <- createConnectionDetails(dbms="mysql", server="localhost",user="root",password="blah",schema="cdm_v4")
#'   conn <- connect(connectionDetails)
#'   dbGetQuery.ffdf(conn,"SELECT * FROM person")
#'   dbDisconnect(conn)
#' }
#' @export
dbGetQuery.ffdf <- function (connection, query = "", batchSize = 100000){
  #Create resultset:
  .jcall("java/lang/System",,"gc")
  .jcall(connection@jc,"V",method="setAutoCommit",FALSE)
  s <- .jcall(connection@jc, "Ljava/sql/Statement;", "createStatement")
  .jcall(s,"V",method="setFetchSize",as.integer(batchSize))
  r <- .jcall(s, "Ljava/sql/ResultSet;", "executeQuery",as.character(query)[1])
  md <- .jcall(r, "Ljava/sql/ResultSetMetaData;", "getMetaData", check=FALSE)
  resultSet <- new("JDBCResult", jr=r, md=md, stat=s, pull=.jnull())
  
  exitFunction <- function(){
    dbClearResult(resultSet)
    .jcall(connection@jc,"V",method="setAutoCommit",TRUE)
  }
  
  on.exit(exitFunction())
  
  #Fetch data in batches:
  data <- NULL
  n <- batchSize
  while (n == batchSize){
    batch <- fetch(resultSet, batchSize)
    n <- nrow(batch)
    if (is.null(data)){
      charCols <- sapply(batch,class)
      charCols <- names(charCols[charCols == "character"])
      
      for(charCol in charCols)
        batch[[charCol]] <- factor(batch[[charCol]]) 
      
      if (n == 0)
        data <- batch #ffdf cannot contain 0 rows, so return data.frame instead
      else
        data <- as.ffdf(batch)    
    } else {
      for(charCol in charCols)
        batch[[charCol]] <- factor(batch[[charCol]]) 
      
      data <- ffdfappend(data,batch)
    }
  }
  data
}

#' Retrieve data from server using batchwise loading.
#'
#' @description
#' This function loads the data from the server in batches. Should be used when loading large sets from a Postgres database to
#' prevent the \code{no description because toString() failed} error.
#' 
#' @param connection  The connection to the database server.
#' @param query		    The SQL statement to retrieve the data
#' @param batchSize		The number of rows that will be retrieved at a time from the server. A larger 
#' batchSize means less calls to the server so better performance, but too large a batchSize could
#' lead to out-of-memory errors.
#'
#' @details
#' Retrieves data from the database server and stores it in a data frame.
#' 
#' @return              
#' A data frame containing the data retrieved from the server
#' 
#' @examples \dontrun{
#'   connectionDetails <- createConnectionDetails(dbms="mysql", server="localhost",user="root",password="blah",schema="cdm_v4")
#'   conn <- connect(connectionDetails)
#'   dbGetQueryBatchWise(conn,"SELECT * FROM person")
#'   dbDisconnect(conn)
#' }
#' @export
dbGetQueryBatchWise <- function (connection, query = "", batchSize = 100000){
  #Create resultset:
  .jcall("java/lang/System",,"gc")
  .jcall(connection@jc,"V",method="setAutoCommit",FALSE)
  s <- .jcall(connection@jc, "Ljava/sql/Statement;", "createStatement")
  .jcall(s,"V",method="setFetchSize",as.integer(batchSize))
  r <- .jcall(s, "Ljava/sql/ResultSet;", "executeQuery",as.character(query)[1])
  md <- .jcall(r, "Ljava/sql/ResultSetMetaData;", "getMetaData", check=FALSE)
  resultSet <- new("JDBCResult", jr=r, md=md, stat=s, pull=.jnull())
  
  exitFunction <- function(){
    dbClearResult(resultSet)
    .jcall(connection@jc,"V",method="setAutoCommit",TRUE)
  }
  
  on.exit(exitFunction())
  
  #Fetch data in batches:
  data <- NULL
  n <- batchSize
  while (n == batchSize){
    batch <- fetch(resultSet, batchSize)
    n <- nrow(batch)
    if (is.null(data)){
        data <- batch 
    } else {
        data <- rbind(data,batch)
    }
  }
  data
}

#' Execute SQL code
#'
#' @description
#' This function executes SQL consisting of one or more statements.
#' 
#' @param connection  The connection to the database server.
#' @param dbms  	    The type of DBMS running on the server. Valid values are
#' \itemize{
#'   \item{"mysql" for MySQL}
#'   \item{"oracle" for Oracle}
#'   \item{"postgresql" for PostgreSQL}
#'   \item{"redshift" for Amazon Redshift}   
#'   \item{"sql server" for Microsoft SQL Server}
#'   }
#' @param sql		The SQL to be executed
#' @param profile     When true, each separate statement is written to file prior to sending to the server, and the time taken
#' to execute a statement is displayed.
#' @param progressBar  When true, a progress bar is shown based on the statements in the SQL code.
#' @param reportOverallTime  When true, the function will display the overall time taken to execute all statements.
#'
#' @details
#' This function splits the SQL in separate statements and sends it to the server for execution. If an error occurs during
#' SQL execution, this error is written to a file to facilitate debugging. Optionally, a progress bar is shown and the total
#' time taken to execute the SQL is displayed. Optionally, each separate SQL statement is written to file, and the execution
#' time per statement is shown to aid in detecting performance issues.
#' 
#' 
#' @examples \dontrun{
#'   connectionDetails <- createConnectionDetails(dbms="mysql", server="localhost",user="root",password="blah",schema="cdm_v4")
#'   conn <- connect(connectionDetails)
#'   executeSql(conn,connectionDetails$dbms,"CREATE TABLE x (k INT); CREATE TABLE y (k INT);")
#'   dbDisconnect(conn)
#' }
#' @export
executeSql <- function(connection, dbms, sql, profile = FALSE, progressBar = TRUE, reportOverallTime = TRUE){
  if (profile)
    progressBar = FALSE
  sqlStatements = splitSql(sql)
  if (progressBar)
    pb <- txtProgressBar(style=3)
  start <- Sys.time()
  for (i in 1:length(sqlStatements)){
    sqlStatement <- sqlStatements[i]
    if (profile){
      sink(paste("statement_",i,".sql",sep=""))
      cat(sqlStatement)
      sink()
    }
    tryCatch ({   
      startQuery <- Sys.time()
      
      #Horrible hack for Redshift, which doesn't support DROP TABLE IF EXIST (or anything similar):
      if (dbms == "redshift" & grepl("DROP TABLE IF EXISTS",sqlStatement)){
        nameStart = regexpr("DROP TABLE IF EXISTS", sqlStatement) + nchar("DROP TABLE IF EXISTS") + 1
        tableName = tolower(gsub("(^ +)|( +$)", "", substr(sqlStatement,nameStart,nchar(sqlStatement))))
        tableCount = dbGetQuery(connection,paste("SELECT COUNT(*) FROM pg_table_def WHERE tablename = '",tableName,"'",sep=""))
        if (tableCount != 0)
          dbSendUpdate(connection, paste("DROP TABLE",tableName))
      } else
        dbSendUpdate(connection, sqlStatement)
      
      if (profile){
        delta <- Sys.time() - startQuery
        writeLines(paste("Statement ",i,"took", delta, attr(delta,"units")))
      }
    } , error = function(err) {
      writeLines(paste("Error executing SQL:",err))
      
      #Write error report:
      filename <- paste(getwd(),"/errorReport.txt",sep="")
      sink(filename)
      error <<- err
      cat("DBMS:\n")
      cat(dbms)
      cat("\n\n")
      cat("Error:\n")
      cat(err$message)
      cat("\n\n")
      cat("SQL:\n")
      cat(sqlStatement)
      sink()
      
      writeLines(paste("An error report has been created at ", filename))
      break
    })
    if (progressBar)
      setTxtProgressBar(pb, i/length(sqlStatements))
  }
  if (progressBar)
    close(pb)
  if (reportOverallTime) {
    delta <- Sys.time() - start
    writeLines(paste("Analysis took", signif(delta,3), attr(delta,"units")))
  }
}

#' Send SQL query
#'
#' @description
#' This function sends SQL to the server, and returns the results.
#' 
#' @param connection  The connection to the database server.
#' @param dbms        The type of DBMS running on the server. Valid values are
#' \itemize{
#'   \item{"mysql" for MySQL}
#'   \item{"oracle" for Oracle}
#'   \item{"postgresql" for PostgreSQL}
#'   \item{"redshift" for Amazon Redshift}   
#'   \item{"sql server" for Microsoft SQL Server}
#'   }
#' @param sql		The SQL to be send.
#'
#' @details
#' This function sends the SQL to the server and retrieves the results. If an error occurs during
#' SQL execution, this error is written to a file to facilitate debugging. 
#' 
#' 
#' @examples \dontrun{
#'   connectionDetails <- createConnectionDetails(dbms="mysql", server="localhost",user="root",password="blah",schema="cdm_v4")
#'   conn <- connect(connectionDetails)
#'   count <- querySql(conn,connectionDetails$dbms,"SELECT COUNT(*) FROM person")
#'   dbDisconnect(conn)
#' }
#' @export
querySql <- function(conn, dbms, sql){
  tryCatch ({   
    .jcall("java/lang/System",,"gc") #Calling garbage collection prevents crashes
    
    if (dbms == "postgresql" | dbms == "redshift"){ #Use dbGetQueryBatchWise to prevent Java out of heap
      result <- dbGetQueryBatchWise(conn, sql)
      colnames(result) <- toupper(colnames(result))
      return(result)
    } else {
      result <- dbGetQuery(conn, sql)
      colnames(result) <- toupper(colnames(result))
      return(result)
    }
    
  } , error = function(err) {
    writeLines(paste("Error executing SQL:",err))
    
    #Write error report:
    filename <- paste(getwd(),"/errorReport.txt",sep="")
    sink(filename)
    error <<- err
    cat("DBMS:\n")
    cat(dbms)
    cat("\n\n")
    cat("Error:\n")
    cat(err$message)
    cat("\n\n")
    cat("SQL:\n")
    cat(sql)
    sink()
    
    writeLines(paste("An error report has been created at ", filename))
    break
  })
}



