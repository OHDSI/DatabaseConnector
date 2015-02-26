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
#'   library("ffbase")
#'   connectionDetails <- createConnectionDetails(dbms="mysql", server="localhost",user="root",password="blah",schema="cdm_v4")
#'   conn <- connect(connectionDetails)
#'   dbGetQuery.ffdf(conn,"SELECT * FROM person")
#'   dbDisconnect(conn)
#' }
#' @export
dbGetQuery.ffdf <- function (connection, query = "", batchSize = 500000){
  #Create resultset:
  rJava::.jcall("java/lang/System",,"gc")
  
  # Have to set autocommit to FALSE for PostgreSQL, or else it will ignore setFetchSize
  # (Note: reason for this is that PostgreSQL doesn't want the data set you're getting to change during fetch)
  rJava::.jcall(connection@jc,"V",method="setAutoCommit",FALSE)
  
  type_forward_only <- rJava::.jfield("java/sql/ResultSet","I","TYPE_FORWARD_ONLY")
  concur_read_only <- rJava::.jfield("java/sql/ResultSet","I","CONCUR_READ_ONLY")
  s <- rJava::.jcall(connection@jc, "Ljava/sql/Statement;", "createStatement",type_forward_only,concur_read_only)
  
  # Have to call setFetchSize on Statement object for PostgreSQL (RJDBC only calls it on ResultSet)
  rJava::.jcall(s,"V",method="setFetchSize",as.integer(2048))
  
  r <- rJava::.jcall(s, "Ljava/sql/ResultSet;", "executeQuery",as.character(query)[1])
  md <- rJava::.jcall(r, "Ljava/sql/ResultSetMetaData;", "getMetaData", check=FALSE)
  resultSet <- new("JDBCResult", jr=r, md=md, stat=s, pull=rJava::.jnull())
  
  on.exit(RJDBC::dbClearResult(resultSet))
  # Don't forget to return autocommit to TRUE
  on.exit(rJava::.jcall(connection@jc,"V",method="setAutoCommit",TRUE), add = TRUE)
  
  #Fetch data in batches:
  data <- NULL
  n <- batchSize
  while (n == batchSize){
    batch <- RJDBC::fetch(resultSet, batchSize)
    n <- nrow(batch)
    if (is.null(data)){
      charCols <- sapply(batch,class)
      charCols <- names(charCols[charCols == "character"])
      
      for(charCol in charCols)
        batch[[charCol]] <- factor(batch[[charCol]]) 
      
      if (n == 0){
        data <- batch #ffdf cannot contain 0 rows, so return data.frame instead
        warning("Data has zero rows, returning an empty data frame")
      } else
        data <- ff::as.ffdf(batch)    
    } else if (n != 0){
      for(charCol in charCols)
        batch[[charCol]] <- factor(batch[[charCol]]) 
      
      data <- ffbase::ffdfappend(data,batch)
    }
  }
  return(data)
}

#' Retrieve data from server using PostgreSQL specific commands.
#'
#' @description
#' This function is tailored to retrieve large datasets from a PostgreSQL database.
#' Specifically, it temporarily disables auto commit and calls \code{setFetchSize} 
#' on the Statement object. Without these settings, all rows would be fetched
#' from the server, resulting in out-of-memory errors.
#' 
#' @param connection  The connection to the database server.
#' @param query		    The SQL statement to retrieve the data
#'
#' @details
#' Retrieves data from the database server and stores it in a data frame.
#' 
#' @return              
#' A data frame containing the data retrieved from the server
#' 
#' @examples \dontrun{
#'   connectionDetails <- createConnectionDetails(dbms="postgresql", server="localhost/ohdsi",user="postgres",password="blah",schema="cdm_v4")
#'   conn <- connect(connectionDetails)
#'   dbGetQueryPostgreSql(conn,"SELECT * FROM person")
#'   dbDisconnect(conn)
#' }
#' @export
dbGetQueryPostgreSql <- function (connection, query = ""){
  #Create resultset:
  rJava::.jcall("java/lang/System",,"gc")
  
  # Have to set autocommit to FALSE for PostgreSQL, or else it will ignore setFetchSize
  # (Note: reason for this is that PostgreSQL doesn't want the data set you're getting to change during fetch)
  rJava::.jcall(connection@jc,"V",method="setAutoCommit",FALSE)
  
  type_forward_only <- rJava::.jfield("java/sql/ResultSet","I","TYPE_FORWARD_ONLY")
  concur_read_only <- rJava::.jfield("java/sql/ResultSet","I","CONCUR_READ_ONLY")
  s <- rJava::.jcall(connection@jc, "Ljava/sql/Statement;", "createStatement",type_forward_only,concur_read_only)
  
  # Have to call setFetchSize on Statement object for PostgreSQL (RJDBC only calls it on ResultSet)
  rJava::.jcall(s,"V",method="setFetchSize",as.integer(2048))
  
  r <- rJava::.jcall(s, "Ljava/sql/ResultSet;", "executeQuery",as.character(query)[1])
  md <- rJava::.jcall(r, "Ljava/sql/ResultSetMetaData;", "getMetaData", check=FALSE)
  resultSet <- new("JDBCResult", jr=r, md=md, stat=s, pull=rJava::.jnull())
  
  on.exit(RJDBC::dbClearResult(resultSet))
  # Don't forget to return autocommit to TRUE
  on.exit(rJava::.jcall(connection@jc,"V",method="setAutoCommit",TRUE), add = TRUE)
  
  data <- RJDBC::fetch(resultSet, -1)
  return(data)
}

#' Execute SQL code
#'
#' @description
#' This function executes SQL consisting of one or more statements.
#' 
#' @param connection  The connection to the database server.
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
#'   connectionDetails <- createConnectionDetails(dbms="mysql", 
#'                                                server="localhost",
#'                                                user="root",
#'                                                password="blah",
#'                                                schema="cdm_v4")
#'   conn <- connect(connectionDetails)
#'   executeSql(conn,"CREATE TABLE x (k INT); CREATE TABLE y (k INT);")
#'   dbDisconnect(conn)
#' }
#' @export
executeSql <- function(connection, sql, profile = FALSE, progressBar = TRUE, reportOverallTime = TRUE){
  if (profile)
    progressBar = FALSE
  sqlStatements = SqlRender::splitSql(sql)
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
      if (attr(connection,"dbms") == "redshift" & grepl("DROP TABLE IF EXISTS",sqlStatement)){
        nameStart = regexpr("DROP TABLE IF EXISTS", sqlStatement) + nchar("DROP TABLE IF EXISTS") + 1
        tableName = tolower(gsub("(^ +)|( +$)", "", substr(sqlStatement,nameStart,nchar(sqlStatement))))
        tableCount = dbGetQuery(connection,paste("SELECT COUNT(*) FROM pg_table_def WHERE tablename = '",tableName,"'",sep=""))
        if (tableCount != 0)
          RJDBC::dbSendUpdate(connection, paste("DROP TABLE",tableName))
      } else
        RJDBC::dbSendUpdate(connection, sqlStatement)
      
      if (profile){
        delta <- Sys.time() - startQuery
        writeLines(paste("Statement ",i,"took", delta, attr(delta,"units")))
      }
    } , error = function(err) {
      writeLines(paste("Error executing SQL:",err))
      
      #Write error report:
      filename <- paste(getwd(),"/errorReport.txt",sep="")
      sink(filename)
      cat("DBMS:\n")
      cat(attr(connection,"dbms"))
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
#' @param sql		The SQL to be send.
#'
#' @details
#' This function sends the SQL to the server and retrieves the results. If an error occurs during
#' SQL execution, this error is written to a file to facilitate debugging. 
#' 
#' @return A data frame.
#' 
#' @examples \dontrun{
#'   connectionDetails <- createConnectionDetails(dbms="mysql", 
#'                                                server="localhost",
#'                                                user="root",
#'                                                password="blah",
#'                                                schema="cdm_v4")
#'   conn <- connect(connectionDetails)
#'   count <- querySql(conn,"SELECT COUNT(*) FROM person")
#'   dbDisconnect(conn)
#' }
#' @export
querySql <- function(connection, sql){
  tryCatch ({   
    rJava::.jcall("java/lang/System",,"gc") #Calling garbage collection prevents crashes
    
    result <- dbGetQueryPostgreSql(connection, sql)
    colnames(result) <- toupper(colnames(result))
    return(result)
  } , error = function(err) {
    writeLines(paste("Error executing SQL:",err))
    
    #Write error report:
    filename <- paste(getwd(),"/errorReport.txt",sep="")
    sink(filename)
    cat("DBMS:\n")
    cat(attr(connection,"dbms"))
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

#' Send SQL query
#'
#' @description
#' This function sends SQL to the server, and returns the results in an ffdf object.
#' 
#' @param connection  The connection to the database server.
#' @param sql  	The SQL to be send.
#'
#' @details
#' This function sends the SQL to the server and retrieves the results. If an error occurs during
#' SQL execution, this error is written to a file to facilitate debugging. 
#' 
#' @return An ffdf object. 
#' 
#' @examples \dontrun{
#'   library(ffbase)
#'   connectionDetails <- createConnectionDetails(dbms="mysql", 
#'                                                server="localhost",
#'                                                user="root",
#'                                                password="blah",
#'                                                schema="cdm_v4")#'   conn <- connect(connectionDetails)
#'   count <- querySql.ffdf(conn,"SELECT COUNT(*) FROM person")
#'   dbDisconnect(conn)
#' }
#' @export
querySql.ffdf <- function(connection, sql){
  tryCatch ({   
    result <- dbGetQuery.ffdf(connection, sql)
    colnames(result) <- toupper(colnames(result))
    return(result)
  } , error = function(err) {
    writeLines(paste("Error executing SQL:",err))
    
    #Write error report:
    filename <- paste(getwd(),"/errorReport.txt",sep="")
    sink(filename)
    cat("DBMS:\n")
    cat(attr(connection,"dbms"))
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

.sql.qescape <- function(s, identifier=FALSE, quote="\"") {
  s <- as.character(s)
  if (identifier) {
    vid <- grep("^[A-Za-z]+([A-Za-z0-9_]*)$",s)
    if (length(s[-vid])) {
      if (is.na(quote)) stop("The JDBC connection doesn't support quoted identifiers, but table/column name contains characters that must be quoted (",paste(s[-vid],collapse=','),")")
      s[-vid] <- .sql.qescape(s[-vid], FALSE, quote)
    }
    return(s)
  }
  if (is.na(quote)) quote <- ''
  s <- gsub("\\\\","\\\\\\\\",s)
  if (nchar(quote)) s <- gsub(paste("\\",quote,sep=''),paste("\\\\\\",quote,sep=''),s,perl=TRUE)
  paste(quote,s,quote,sep='')
}

#' Insert a table on the server
#'
#' @description
#' This function sends the data in a data frame to a table on the server. Either a new table is 
#' created, or the data is appended to an existing table.
#' 
#' @param connection  The connection to the database server.
#' @param tableName  	The name of the table where the data should be inserted.
#' @param data        The data frame containing the data to be inserted.
#' @param dropTableIfExists   Drop the table if the table already exists before writing?
#' @param createTable      Create a new table? If false, will append to existing table.
#'
#' @details
#' This function sends the data in a data frame to a table on the server. Either a new table is 
#' created, or the data is appended to an existing table.
#' 
#' @examples \dontrun{
#'   connectionDetails <- createConnectionDetails(dbms="mysql", 
#'                                                server="localhost",
#'                                                user="root",
#'                                                password="blah",
#'                                                schema="cdm_v4")
#'   conn <- connect(connectionDetails)
#'   data <- data.frame(x = c(1,2,3), y = c("a","b","c"))
#'   dbInsertTable(conn,"my_table",data)
#'   dbDisconnect(conn)
#' }
#' @export
dbInsertTable <- function(connection, tableName, data, dropTableIfExists = TRUE, createTable = TRUE) {
  if (dropTableIfExists)
    createTable = TRUE
  if (is.vector(data) && !is.list(data)) data <- data.frame(x=data)
  if (length(data) < 1) stop("data must have at least one column")
  if (is.null(names(data))) names(data) <- paste("V",1:length(data),sep='')
  if (length(data[[1]])>0) {
    if (!is.data.frame(data)) data <- as.data.frame(data, row.names=1:length(data[[1]]))
  } else {
    if (!is.data.frame(data)) data <- as.data.frame(data)
  }

  if (dropTableIfExists) {
    sql <- "IF OBJECT_ID('@tableName', 'U') IS NOT NULL  DROP TABLE @tableName;"
    sql <- SqlRender::renderSql(sql,  tableName = tableName)$sql
    sql <- SqlRender::translateSql(sql, targetDialect = attr(connection,"dbms"))$sql
    executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)  
  }
  
  def = function(obj) {
    if (is.integer(obj)) "INTEGER"
    else if (is.numeric(obj)) "FLOAT"
    else "VARCHAR(255)"
  }
  fts <- sapply(data, def)
  fdef <- paste(.sql.qescape(names(data), TRUE, connection@identifier.quote),fts,collapse=',')
  qname <- .sql.qescape(tableName, TRUE, connection@identifier.quote)
  if (createTable) {
    sql <- paste("CREATE TABLE ",qname," (",fdef,")",sep= '')
    sql <- SqlRender::translateSql(sql,targetDialect=attr(connection,"dbms"))$sql
    executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  }
  esc <- function(str){
    paste("'",gsub("'","''",str),"'",sep="")
  }
  varNames <- paste(.sql.qescape(names(data), TRUE, connection@identifier.quote),collapse=',')
  batchSize <- 1000
  for (start in seq(1,nrow(data),by=batchSize)){
    end = min(start+batchSize-1,nrow(data))
    valueString <- paste(apply(sapply(data[start:end,],esc),MARGIN=1,FUN = paste,collapse=","),collapse="),(")
    sql <- paste("INSERT INTO ",qname," (",varNames,") VALUES (",valueString,")",sep= '')
    sql <- SqlRender::translateSql(sql,targetDialect=attr(connection,"dbms"))$sql
    dbSendUpdate(connection,sql)
  }   
}


