library(DatabaseConnector)
options(fftempdir = "s:/fftemp")

# Test PDW with integrated security ----------------------------------------------
connectionDetails <- createConnectionDetails(dbms = "pdw",
                                             server = Sys.getenv("PDW_SERVER"),
                                             port = Sys.getenv("PDW_PORT"),
                                             schema = "CDM_Truven_MDCR_V415")
conn <- connect(connectionDetails)
x <- querySql.ffdf(conn, "SELECT * FROM observation_period WHERE person_id = -999")
getTableNames(conn, "CDM_Truven_MDCR_V415.dbo")
disconnect(conn)


# Test Oracle ---------------------------------------------------
connectionDetails <- createConnectionDetails(dbms = "oracle",
                                             server = "xe",
                                             user = "system",
                                             password = Sys.getenv("pwOracle"),
                                             schema = "cdm_synpuf")
conn <- connect(connectionDetails)
querySql(conn, "SELECT COUNT(*) FROM person")

dbListTables(conn, schema = "CDM_SYNPUF")
dbListTables(conn, schema = "cdm_synpuf")
dbGetQuery(conn, "SELECT * FROM cdm_synpuf.vocabulary")

dbDisconnect(conn)

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

# Test Redshift -----------------------
connectionDetails <- createConnectionDetails(dbms = "redshift",
                                             server = paste0(Sys.getenv("REDSHIFT_SERVER"), "/jmdc"),
                                             user = Sys.getenv("REDSHIFT_USER"),
                                             password = Sys.getenv("REDSHIFT_PW"),
                                             extraSettings = "ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory")
conn <- connect(connectionDetails)
querySql(conn, "SELECT COUNT(*) FROM person")
getTableNames(conn, "cdm")
executeSql(conn, "CREATE TABLE scratch.test (x INT)")

person <- querySql.ffdf(conn, "SELECT * FROM person")
data <- data.frame(id = c(1, 2, 3),
                   date = as.Date(c("2000-01-01", "2001-01-31", "2004-12-31")),
                   text = c("asdf", "asdf", "asdf"))
insertTable(connection = conn,
            tableName = "test",
            data = data,
            dropTableIfExists = TRUE,
            createTable = TRUE,
            tempTable = TRUE)
d2 <- querySql(conn, "SELECT * FROM test")
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
connectionDetails <- createConnectionDetails(dbms = "sql server",
                                             server = "RNDUSRDHIT06.jnj.com",
                                             schema = "cdm_hcup")
connection <- connect(connectionDetails)
dbInsertTable(connection, tableName, data, dropTableIfExists = TRUE)

d <- querySql(connection, "SELECT * FROM #temp")
d <- querySql.ffdf(connection, "SELECT * FROM #temp")

library(ffbase)
data <- as.ffdf(data)

dbDisconnect(connection)

# Test RDS -----------------------
connectionDetails <- createConnectionDetails(dbms = "sql server",
                                             server = Sys.getenv("RDS_SERVER"),
                                             user = Sys.getenv("RDS_USER"),
                                             password = Sys.getenv("RDS_PW"))
conn <- connect(connectionDetails)
querySql(conn, "SELECT COUNT(*) FROM person")
disconnect(conn)

