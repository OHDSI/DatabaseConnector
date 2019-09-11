library(DatabaseConnector)
options(fftempdir = "s:/fftemp")

# Test PDW with integrated security ----------------------------------------------
connectionDetails <- createConnectionDetails(dbms = "pdw",
                                             server = Sys.getenv("PDW_SERVER"),
                                             port = Sys.getenv("PDW_PORT"),
                                             schema = "CDM_Truven_MDCR_V415")
conn <- connect(connectionDetails)
conn2 <- connect(connectionDetails)
disconnect(conn)
disconnect(conn2)
x <- querySql.ffdf(conn, "SELECT * FROM observation_period WHERE person_id = -999")
getTableNames(conn, "CDM_Truven_MDCR_V415.dbo")


# DBI compatability
dbIsValid(conn)
dbListTables(conn, database = "CDM_Truven_MDCR_V415", schema = "dbo")
dbListFields(conn, database = "CDM_Truven_MDCR_V415", schema = "dbo", name = "vocabulary")
dbExistsTable(conn, database = "CDM_Truven_MDCR_V415", schema = "dbo", name = "vocabulary")
res <- dbSendQuery(conn, "SELECT * FROM CDM_Truven_MDCR_V415.dbo.vocabulary")
dbColumnInfo(res)
dbGetRowCount(res)
dbHasCompleted(res)
dbFetch(res)
dbGetRowCount(res)
dbHasCompleted(res)
dbGetStatement(res)
dbClearResult(res)
dbQuoteIdentifier(conn, "test results")
dbQuoteString(conn, "one two three")
dbGetQuery(conn, "SELECT * FROM CDM_Truven_MDCR_V415.dbo.vocabulary")

res <- dbSendStatement(conn, "CREATE TABLE #temp(x int);")
dbHasCompleted(res)
dbGetRowsAffected(res)
dbGetStatement(res)
dbClearResult(res)
dbListFields(conn, "#temp")
dbRemoveTable(conn, "#temp")

dbExecute(conn, "CREATE TABLE #temp(x int);")
dbExistsTable(conn, name = "temp")
dbRemoveTable(conn, "#temp")

data <- data.frame(name = c("john", "mary"), age = c(35, 26))
dbWriteTable(conn, "#temp", data, temporary = TRUE)
dbGetQuery(conn, "SELECT * FROM #temp")
dbReadTable(conn, "#temp")
dbRemoveTable(conn, "#temp")

dbCreateTable(conn, "#temp", data, temporary = TRUE)
dbAppendTable(conn, "#temp", data)
dbGetQuery(conn, "SELECT * FROM #temp")
dbRemoveTable(conn, "#temp")


dbDisconnect(conn)


# Test Oracle ---------------------------------------------------
connectionDetails <- createConnectionDetails(dbms = "oracle",
                                             server = "xe",
                                             user = "system",
                                             password = Sys.getenv("pwOracle"),
                                             schema = "cdm_synpuf")
conn <- connect(connectionDetails)
querySql(conn, "SELECT COUNT(*) FROM person")

dbIsValid(conn)
dbListTables(conn, schema = "cdm_synpuf")
disconnect(conn)


# Test PostgreSQL --------------------------------------------------------
connectionDetails <- createConnectionDetails(dbms = "postgresql",
                                             server = "localhost/ohdsi",
                                             user = "postgres",
                                             password = Sys.getenv("pwPostgres"),
                                             schema = "cdm_synpuf")
conn <- connect(connectionDetails)
querySql(conn, "SELECT COUNT(*) FROM person")
getTableNames(conn, "cdm_synpuf")
disconnect(conn)

# DBI compatability
dbCanConnect(DatabaseConnectorDriver(), connectionDetails)

conn <- dbConnect(DatabaseConnectorDriver(), connectionDetails)
dbIsReadOnly(conn)
dbIsValid(conn)
dbListTables(conn, schema = "cdm_synpuf")
dbListFields(conn, schema = "cdm_synpuf", name = "vocabulary")
dbExistsTable(conn, schema = "cdm_synpuf", name = "vocabulary")
res <- dbSendQuery(conn, "SELECT * FROM cdm_synpuf.vocabulary")
dbColumnInfo(res)
dbGetRowCount(res)
dbHasCompleted(res)
dbFetch(res)
dbGetRowCount(res)
dbHasCompleted(res)
dbGetStatement(res)
dbClearResult(res)
dbQuoteIdentifier(conn, "test results")
dbQuoteString(conn, "one two three")
dbGetQuery(conn, "SELECT * FROM cdm_synpuf.vocabulary")

res <- dbSendStatement(conn, "CREATE TEMP TABLE temp(x int);")
dbHasCompleted(res)
dbGetRowsAffected(res)
dbGetStatement(res)
dbClearResult(res)
dbListFields(conn, "temp")
dbRemoveTable(conn, "temp")

dbExecute(conn, "CREATE TEMP TABLE temp(x int);")
dbExistsTable(conn, name = "temp")


dbRemoveTable(conn, "temp")

data <- data.frame(name = c("john", "mary"), age = c(35, 26))
dbWriteTable(conn, "temp", data, temporary = TRUE)
dbGetQuery(conn, "SELECT * FROM temp")
dbReadTable(conn, "temp")
dbRemoveTable(conn, "temp")

dbCreateTable(conn, "temp", data, temporary = TRUE)
dbAppendTable(conn, "temp", data)
dbGetQuery(conn, "SELECT * FROM temp")
dbRemoveTable(conn, "temp")

dbDisconnect(conn)


# Test Redshift -----------------------
dbms <- "redshift"
user <- Sys.getenv("redShiftUser")
pw <- Sys.getenv("redShiftPassword")
connectionString <- Sys.getenv("jmdcRedShiftConnectionString")
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = dbms,
                                                                connectionString = connectionString,
                                                                user = user,
                                                                password = pw)

conn <- connect(connectionDetails)
# querySql(conn, "SELECT COUNT(*) FROM person")
getTableNames(conn, "cdm")
renderTranslateExecuteSql(conn, "CREATE TABLE #test (x INT); INSERT INTO #test (x) SELECT 1; TRUNCATE TABLE #test; DROP TABLE #test;")

person <- querySql.ffdf(conn, "SELECT * FROM person")
data <- data.frame(id = c(1, 2, 3),
                   date = as.Date(c("2000-01-01", "2001-01-31", "2004-12-31")),
                   text = c("asdf", "asdf", "asdf"))
system.time(
insertTable(connection = conn,
            tableName = "#test",
            data = data,
            dropTableIfExists = TRUE,
            createTable = TRUE,
            tempTable = TRUE)
)
d2 <- querySql(conn, "SELECT * FROM #test;")
str(d2)

options(fftempdir = "s:/fftemp")
d2 <- querySql.ffdf(conn, "SELECT * FROM test")
d2
disconnect(conn)

### Tests for dbInsertTable ###
day.start <- "1900/01/01"
day.end <- "2012/12/31"
dayseq <- seq.Date(as.Date(day.start), as.Date(day.end), by = "day")
makeRandomStrings <- function(n = 1, lenght = 12) {
  randomString <- c(1:n)
  for (i in 1:n) randomString[i] <- paste(sample(c(0:9, letters, LETTERS), lenght, replace = TRUE),
                                          collapse = "")
  return(randomString)
}
data <- data.frame(start_date = dayseq,
                   person_id = as.integer(round(runif(length(dayseq), 1, 1e+07))),
                   value = runif(length(dayseq)),
                   id = makeRandomStrings(length(dayseq)))
str(data)
tableName <- "#temp"
data <- data[1:10, ]
connection <- connect(connectionDetails)
insertTable(connection, tableName, data, dropTableIfExists = TRUE)

d <- querySql(connection, "SELECT * FROM #temp")
d <- querySql.ffdf(connection, "SELECT * FROM #temp")

library(ffbase)
data <- as.ffdf(data)

disconnect(connection)

# Test RDS -----------------------
connectionDetails <- createConnectionDetails(dbms = "sql server",
                                             server = Sys.getenv("RDS_SERVER"),
                                             user = Sys.getenv("RDS_USER"),
                                             password = Sys.getenv("RDS_PW"))
conn <- connect(connectionDetails)
querySql(conn, "SELECT COUNT(*) FROM testing.cdm_testing_jmdc.person")
disconnect(conn)


# Test insert table performance on PostgreSQL -----------------------------
connectionDetails <- createConnectionDetails(dbms = "postgresql",
                                             server = "localhost/ohdsi",
                                             user = "postgres",
                                             password = Sys.getenv("pwPostgres"),
                                             schema = "cdm_synpuf")
conn <- connect(connectionDetails)
set.seed(1)
day.start <- "1900/01/01"
day.end <- "2012/12/31"
dayseq <- seq.Date(as.Date(day.start), as.Date(day.end), by = "day")
makeRandomStrings <- function(n = 1, lenght = 12) {
  randomString <- c(1:n)
  for (i in 1:n) randomString[i] <- paste(sample(c(0:9, letters, LETTERS), lenght, replace = TRUE),
                                          collapse = "")
  return(randomString)
}
data <- data.frame(start_date = dayseq,
                   person_id = as.integer(round(runif(length(dayseq), 1, 1e+07))),
                   value = runif(length(dayseq)),
                   id = makeRandomStrings(length(dayseq)))

data$start_date[4] <- NA
data$person_id[5] <- NA
data$value[2] <- NA
data$id[3] <- NA

system.time(
  insertTable(connection = conn,
              tableName = "scratch.insert_test",
              data = data,
              dropTableIfExists = TRUE,
              createTable = TRUE,
              tempTable = FALSE,
              progressBar = TRUE)
)
# Without batched insert: 
#user  system elapsed 
#62.36    0.46   65.38 
#user  system elapsed 
# 59.13    0.11   61.70 
# user  system elapsed 
#60.10    0.21   64.14 

# With batched insert:
# user  system elapsed 
# 4.28    0.29    3.74 
# user  system elapsed 
# 1.50    0.11    2.83 

# user  system elapsed 
# 0.98    0.10    2.76 

insertTable(connection = conn,
            tableName = "scratch.insert_test",
            data = data[1, ],
            dropTableIfExists = TRUE,
            createTable = TRUE,
            tempTable = FALSE)

disconnect(conn)

# Test bulk import -----------------------------------------------
connectionDetails <- createConnectionDetails(dbms = "pdw",
                                             server = Sys.getenv("PDW_SERVER"),
                                             port = Sys.getenv("PDW_PORT"),
                                             schema = "scratch.dbo")
conn <- connect(connectionDetails)
set.seed(1)
day.start <- "1900/01/01"
day.end <- "2012/12/31"
dayseq <- seq.Date(as.Date(day.start), as.Date(day.end), by = "day")
makeRandomStrings <- function(n = 1, lenght = 12) {
  randomString <- c(1:n)
  for (i in 1:n) randomString[i] <- paste(sample(c(0:9, letters, LETTERS), lenght, replace = TRUE),
                                          collapse = "")
  return(randomString)
}
data <- data.frame(start_date = dayseq,
                   person_id = as.integer(round(runif(length(dayseq), 1, 1e+07))),
                   value = runif(length(dayseq)),
                   id = makeRandomStrings(length(dayseq)))

system.time(
  insertTable(connection = conn,
              tableName = "scratch.dbo.insert_test",
              data = data,
              dropTableIfExists = TRUE,
              createTable = TRUE,
              tempTable = FALSE,
              progressBar = TRUE,
              useMppBulkLoad = TRUE)
)
executeSql(conn, "DROP TABLE scratch.dbo.insert_test;")
disconnect(conn)

# Test RedShift ------------------------------------------
dbms <- "redshift"
user <- Sys.getenv("jmdcRedShiftUser")
pw <- Sys.getenv("jmdcRedShiftPassword")
connectionString <- Sys.getenv("jmdcRedShiftConnectionString")
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = dbms,
                                                                connectionString = connectionString,
                                                                user = user,
                                                                password = pw)
conn <- connect(connectionDetails)
getTableNames(conn, "cdm")
disconnect(conn)


# Test compression -----------------------------------
setwd("c:/temp/")
zipFile <- "c:/temp/testData.zip"
fileToZip <- "c:/temp/testData.csv"
rows <- 1e8
x <- data.frame(a = runif(rows), b = sample(letters, rows, replace = TRUE))  
write.csv(x, fileToZip)
createZipFile(zipFile = zipFile,
              files = fileToZip)

files <- "vignetteFeatureExtraction"
zipFile <- "data.zip"
rootFolder <- "c:/temp"
createZipFile(zipFile = "data.zip", files = "vignetteFeatureExtraction")


files <- c("AppStore.log", "AppStoreInstallLogs.txt")



# Test insert table performance on RedShift -----------------------------
dbms <- "redshift"
user <- Sys.getenv("jmdcRedShiftUser")
pw <- Sys.getenv("jmdcRedShiftPassword")
connectionString <- Sys.getenv("jmdcRedShiftConnectionString")
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = dbms,
                                                                connectionString = connectionString,
                                                                user = user,
                                                                password = pw)
conn <- connect(connectionDetails)
set.seed(1)
day.start <- "1900/01/01"
day.end <- "2012/12/31"
dayseq <- seq.Date(as.Date(day.start), as.Date(day.end), by = "day")
makeRandomStrings <- function(n = 1, lenght = 12) {
  randomString <- c(1:n)
  for (i in 1:n) randomString[i] <- paste(sample(c(0:9, letters, LETTERS), lenght, replace = TRUE),
                                          collapse = "")
  return(randomString)
}
data <- data.frame(start_date = dayseq,
                   person_id = as.integer(round(runif(length(dayseq), 1, 1e+07))),
                   value = runif(length(dayseq)),
                   id = makeRandomStrings(length(dayseq)))

data$start_date[4] <- NA
data$person_id[5] <- NA
data$value[2] <- NA
data$id[3] <- NA

data <- data[1:10000, ]

system.time(
  insertTable(connection = conn,
              tableName = "scratch_mschuemi.insert_test",
              data = data,
              dropTableIfExists = TRUE,
              createTable = TRUE,
              tempTable = FALSE,
              progressBar = TRUE)
)

# 100 rows:
# Using default:
# user  system elapsed 
# 1.45    0.36  265.66 

# 2nd time using default:
# user  system elapsed 
# 0.34    0.06  213.82 

# Using CTAS 
# user  system elapsed 
# 0.49    0.05   32.93 

#1000 rows:
# Using CTAS:
# user  system elapsed 
# 1.40    0.08  149.74 

# 10000 rows:
# user  system elapsed 
# 4.72    0.36 1703.59 

data2 <- querySql(conn, "SELECT * FROM scratch_mschuemi.insert_test;")

disconnect(conn)
