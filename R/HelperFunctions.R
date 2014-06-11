recodeLevels.factor <- function(x, lev){
  m <- match(levels(x), lev)
  a <- attributes(x)
  a$levels <- lev
  attributes(x) <- NULL
  x <- m[x]
  attributes(x) <- a
  x
}

appendToFfdf <- function(data, batch){
  for (i in 1 : ncol(batch)){
    if (class(batch[[i]]) == "factor"){
      lev <- unique(c(levels(data[[i]]), levels(batch[[i]])))
      levels(data[[i]]) <- lev
      batch[[i]] <- recodeLevels(batch[[i]], lev)
    }
  }
  nBatch <- nrow(batch)
  nData <- nrow(data)
  nrow(data) <- nData + nBatch
  index <- hi(nData + 1L, nData + nBatch)
  data[index, ] <- batch
  data
}

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
#' A ffdf object containing the data.
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
  .jcall(conn@jc,"V",method="setAutoCommit",FALSE)
  s <- .jcall(conn@jc, "Ljava/sql/Statement;", "createStatement")
  .jcall(s,"V",method="setFetchSize",as.integer(batchSize))
  r <- .jcall(s, "Ljava/sql/ResultSet;", "executeQuery",as.character(query)[1])
  md <- .jcall(r, "Ljava/sql/ResultSetMetaData;", "getMetaData", check=FALSE)
  resultSet <- new("JDBCResult", jr=r, md=md, stat=s, pull=.jnull())
  
  on.exit(dbClearResult(resultSet))
  
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
      
      data <- as.ffdf(convertCharacterToFactor(batch))
    } else {
      for(charCol in charCols)
        batch[[charCol]] <- factor(batch[[charCol]]) 
      
      data <- appendToFfdf(data,batch)
    }
  }
  data
}
