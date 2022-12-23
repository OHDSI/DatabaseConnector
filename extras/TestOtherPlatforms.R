# Tests for platforms not available to GitHub Actions
library(DatabaseConnector)
library(testthat)

# Open and close connection -----------------------------------------------

# BigQuery
details <- createConnectionDetails(dbms = "bigquery",
                                   connectionString = keyring::key_get("bigQueryConnString"),
                                   user = "",
                                   password = "")
connection <- connect(details)
expect_true(inherits(connection, "DatabaseConnectorConnection"))
expect_true(disconnect(connection))

# Fetch results -------------------------------------------------------------

# BigQuery
connection <- connect(dbms = "bigquery",
                      connectionString = keyring::key_get("bigQueryConnString"),
                      user = "",
                      password = "")
cdmDatabaseSchema <- "synpuf_2m"
sql <- "SELECT COUNT(*) AS row_count FROM @cdm_database_schema.vocabulary"
renderedSql <- SqlRender::render(sql, cdm_database_schema = cdmDatabaseSchema)

# Fetch data.frame:
count <- querySql(connection, renderedSql)
expect_equal(count[1, 1], 96)
count <- renderTranslateQuerySql(connection, sql, cdm_database_schema = cdmDatabaseSchema)
expect_equal(count[1, 1], 96)

# Fetch Andromeda:
andromeda <- Andromeda::andromeda()
querySqlToAndromeda(connection, renderedSql, andromeda = andromeda, andromedaTableName = "test", snakeCaseToCamelCase = TRUE)
expect_equivalent(dplyr::collect(andromeda$test)$rowCount[1], 96)
renderTranslateQuerySqlToAndromeda(connection, sql, cdm_database_schema = cdmDatabaseSchema, andromeda = andromeda, andromedaTableName = "test2", snakeCaseToCamelCase = TRUE)
expect_equivalent(dplyr::collect(andromeda$test2)$rowCount[1], 96)

disconnect(connection)


# Get table names ----------------------------------------------------------------------

# BigQuery
details <- createConnectionDetails(dbms = "bigquery",
                                   connectionString = keyring::key_get("bigQueryConnString"),
                                   user = "",
                                   password = "")
connection <- connect(details)
tables <- getTableNames(connection, "synpuf_2m")
expect_true("PERSON" %in% tables)
disconnect(connection)

# insertTable ---------------------------------------------------------------------------------
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
data <- data.frame(start_date = dayseq,
                   some_datetime = timeSeq,
                   person_id = as.integer(round(runif(length(dayseq), 1, 1e+07))),
                   value = runif(length(dayseq)),
                   id = makeRandomStrings(length(dayseq)),
                   big_ints = bigInts,
                   stringsAsFactors = FALSE)

data$start_date[4] <- NA
data$some_datetime[6] <- NA
data$person_id[5] <- NA
data$value[2] <- NA
data$id[3] <- NA
data$big_ints[7] <- NA
data$big_ints[8] <- 3.3043e+10

# BigQuery
details <- createConnectionDetails(dbms = "bigquery",
                                   connectionString = keyring::key_get("bigQueryConnString"),
                                   user = "",
                                   password = "")
connection <- connect(details)
insertTable(connection = connection,
            tableName = "synpuf_2m_results.temp",
            data = data,
            createTable = TRUE,
            tempTable = FALSE,
            tempEmulationSchema = "synpuf_2m_results")

# Check data on server is same as local
data2 <- querySql(connection, "SELECT * FROM synpuf_2m_results.temp", integer64AsNumeric = FALSE)
names(data2) <- tolower(names(data2))
data <- data[order(data$person_id), ]
data2 <- data2[order(data2$person_id), ]
row.names(data) <- NULL
row.names(data2) <- NULL
expect_equal(data[order(data$big_ints), ], data2[order(data2$big_ints), ])

# Check data types
res <- dbSendQuery(connection, "SELECT * FROM synpuf_2m_results.temp")
columnInfo <- dbColumnInfo(res)
dbClearResult(res)
expect_equal(as.character(columnInfo$field.type), c("DATE", "DATETIME", "INT64", "FLOAT64", "STRING", "INT64"))

executeSql(connection, "DROP TABLE synpuf_2m_results.temp;")

disconnect(connection)


# Test dropEmulatedTempTables ----------------------------------------------
# BigQuery
details <- createConnectionDetails(dbms = "bigquery",
                                   connectionString = keyring::key_get("bigQueryConnString"),
                                   user = "",
                                   password = "")
connection <- connect(details)
insertTable(connection = connection,
            tableName = "temp",
            data = cars,
            createTable = TRUE,
            tempTable = TRUE,
            tempEmulationSchema = "synpuf_2m_results")

droppedTables <- dropEmulatedTempTables(connection = connection, tempEmulationSchema = "synpuf_2m_results")
expect_equal(droppedTables, sprintf("%s.%stemp", tempEmulationSchema, SqlRender::getTempTablePrefix()))
disconnect(connection)


# Testhash computation ----------------------------------------------
# BigQuery
details <- createConnectionDetails(dbms = "bigquery",
                                   connectionString = keyring::key_get("bigQueryConnString"),
                                   user = "",
                                   password = "")
connection <- connect(details)
hash <- computeDataHash(connection = connection,
                        databaseSchema = "synpuf_2m")
expect_true(is.character(hash))
disconnect(connection)

# Test dbplyr ------------------------------------------------------------------
source("tests/testthat/dbplyrTestFunction.R")
# options("DEBUG_DATABASECONNECTOR_DBPLYR" = TRUE)
# BigQuery
connectionDetails <- createConnectionDetails(dbms = "bigquery",
                                             connectionString = keyring::key_get("bigQueryConnString"),
                                             user = "",
                                             password = "")
cdmDatabaseSchema <- "synpuf_2m"
options(sqlRenderTempEmulationSchema = "synpuf_2m_results")
testDbplyrFunctions(connectionDetails, cdmDatabaseSchema)


# Spark
connectionDetails <- createConnectionDetails(dbms = "spark",
                                             connectionString = keyring::key_get("sparkConnectionString"),
                                             user = keyring::key_get("sparkUser"),
                                             password = keyring::key_get("sparkPassword"))
cdmDatabaseSchema <- "eunomia"
options(sqlRenderTempEmulationSchema = "eunomia")
testDbplyrFunctions(connectionDetails, cdmDatabaseSchema)

# Spark via ODBC
connectionDetails <- createConnectionDetails(dbms = "spark",
                                             server = keyring::key_get("sparkServer"),
                                             port = keyring::key_get("sparkPort"),
                                             user = keyring::key_get("sparkUser"),
                                             password = keyring::key_get("sparkPassword"))



# connectionDetails <- DatabaseConnector:::createDbiConnectionDetails(
#   dbms = "spark",
#   drv = odbc::odbc(),
#   Driver = "Simba Spark ODBC Driver",
#   Host = keyring::key_get("sparkServer"),
#   uid = keyring::key_get("sparkUser"),
#   pwd = keyring::key_get("sparkPassword"),
#   Port = keyring::key_get("sparkPort")
# )

connection <- connect(connectionDetails)
insertTable(
  connection = connection,
  databaseSchema = "eunomia",
  tableName = "cars",
  data = cars,
  dropTableIfExists = TRUE,
  createTable = TRUE
)

executeSql(connection, "DROP TABLE eunomia.cars;")

DBI::dbDisconnect(connection)

hash <- computeDataHash(connection = connection,
                        databaseSchema = "eunomia")

expect_true(is.character(hash))
disconnect(connection)


getTableNames(connection, "eunomia")

# Use pure ODBC:
db <- DBI::dbConnect(odbc::odbc(),
                     Driver = "Simba Spark ODBC Driver",
                     Host = keyring::key_get("sparkServer"),
                     uid = keyring::key_get("sparkUser"),
                     pwd = keyring::key_get("sparkPassword"),
                     Port = keyring::key_get("sparkPort"))

DBI::dbExecute(db, "DROP TABLE cars;")
DBI::dbGetQuery(db, "SELECT * FROM eunomia.achilles_results LIMIT 1;")
DBI::dbExecute(db, "CREATE TABLE eunomia.test(x INT);")
DBI::dbExecute(db, "DROP TABLE eunomia.test;")

DBI::dbExecute(db, "USE eunomia;")
DBI::dbExecute(db, "DROP TABLE \"test\";")
DBI::dbListTables(db, schema = "eunomia")
DBI::dbGetQuery(db, "SELECT * FROM eunomia.test2 LIMIT 1;")

DBI::dbDisconnect(db)

# RPostgres
connectionDetails <- DatabaseConnector:::createDbiConnectionDetails(
  dbms = "postgresql",
  drv = RPostgres::Postgres(),
  dbname = strsplit(Sys.getenv("CDM5_POSTGRESQL_SERVER"), "/")[[1]][2],
  host = strsplit(Sys.getenv("CDM5_POSTGRESQL_SERVER"), "/")[[1]][1], 
  user = Sys.getenv("CDM5_POSTGRESQL_USER"), 
  password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD"))
)
connection <- connect(connectionDetails)

insertTable(
  connection = connection,
  # databaseSchema = "ohdsi",
  tableName = "#cars",
  data = cars,
  dropTableIfExists = TRUE,
  createTable = TRUE,
  tempTable = TRUE
)
dbGetQuery(connection, "SELECT TOP 5 * FROM #cars;")

dbExecute(connection, "DROP TABLE #cars;")

disconnect(connection)

