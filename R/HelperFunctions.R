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

