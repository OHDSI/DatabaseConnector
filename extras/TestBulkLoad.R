# Currently only have PDW and PostgreSQL set up
library(DatabaseConnector)

# Generate some data to upload
set.seed(0)
day.start <- "1960/01/01"
day.end <- "2000/12/31"
time.start <- as.POSIXct("2018-11-12 09:04:07 CET")
dayseq <- seq.Date(as.Date(day.start), as.Date(day.end), by = "week")
timeSeq <- time.start + (1:length(dayseq)) * 60 * 60 * 24
makeRandomStrings <- function(n = 1, lenght = 12) {
  randomString <- c(1:n)
  for (i in 1:n) randomString[i] <- paste(sample(c(0:9, letters, LETTERS), lenght, replace = TRUE),
                                          collapse = "")
  return(randomString)
}
bigInts <- bit64::runif64(length(dayseq))
data <- data.frame(startDate = dayseq,
                   someDatetime = timeSeq,
                   personId = as.integer(round(runif(length(dayseq), 1, 1e+07))),
                   value = runif(length(dayseq)),
                   id = makeRandomStrings(length(dayseq)),
                   bigInts = bigInts,
                   stringsAsFactors = FALSE)

data$startDate[4] <- NA
data$someDatetime[6] <- NA
data$personId[5] <- NA
data$value[2] <- NA
data$id[3] <- NA
data$bigInts[7] <- NA
data$bigInts[8] <- 3.3043e+10

# PDW ------------------------------------------------------------------------------
Sys.setenv(DWLOADER_PATH = "C:/Program Files/Microsoft SQL Server Parallel Data Warehouse/100/DWLoader.exe")
connectionDetails <- createConnectionDetails(dbms = "pdw",
                                             server = keyring::key_get("pdwServer"),
                                             port = keyring::key_get("pdwPort"))
connection <- connect(connectionDetails)
system.time(
  insertTable(connection = connection,
              tableName = "scratch.dbo.insert_test",
              data = data,
              dropTableIfExists = TRUE,
              createTable = TRUE,
              tempTable = FALSE,
              progressBar = TRUE,
              camelCaseToSnakeCase = TRUE,
              bulkLoad = TRUE)
)
data2 <- querySql(connection, "SELECT * FROM scratch.dbo.insert_test;", snakeCaseToCamelCase = TRUE)

data <- data[order(data$id), ]
data2 <- data2[order(data2$id), ]
row.names(data) <- NULL
row.names(data2) <- NULL
all.equal(data, data2)

renderTranslateExecuteSql(connection, "DROP TABLE scratch.dbo.insert_test;")
disconnect(connection)

# PostgreSQL ------------------------------------------------------------------------------
Sys.setenv(POSTGRES_PATH = "C:/Program Files/PostgreSQL/11/bin")
connectionDetails <- createConnectionDetails(dbms = "postgresql",
                                             server = "localhost/ohdsi",
                                             user = "postgres",
                                             password = Sys.getenv("pwPostgres"))
connection <- connect(connectionDetails)
system.time(
  insertTable(connection = connection,
              tableName = "scratch.insert_test",
              data = data,
              dropTableIfExists = TRUE,
              createTable = TRUE,
              tempTable = FALSE,
              progressBar = TRUE,
              camelCaseToSnakeCase = TRUE,
              bulkLoad = TRUE)
)
data2 <- querySql(connection, "SELECT * FROM scratch.insert_test;", snakeCaseToCamelCase = TRUE)

all.equal(data, data2)

renderTranslateExecuteSql(connection, "DROP TABLE scratch.insert_test;")
disconnect(connection)
