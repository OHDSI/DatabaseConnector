# Tests for platforms not available to Travis
library(DatabaseConnector)
library(testthat)

# Download drivers -----------------------------------------------

downloadJdbcDrivers("pdw")
downloadJdbcDrivers("redshift")

# Open and close connection -----------------------------------------------

# PDW
details <- createConnectionDetails(dbms = "pdw",
                                   server = keyring::key_get("pdwServer"),
                                   port = keyring::key_get("pdwPort"))
connection <- connect(details)
expect_true(inherits(connection, "DatabaseConnectorConnection"))
expect_true(disconnect(connection))

# RedShift
details <- DatabaseConnector::createConnectionDetails(dbms = "redshift",
                                                      connectionString = keyring::key_get("redShiftConnectionStringCcae"),
                                                      user = keyring::key_get("redShiftUserName"),
                                                      password = keyring::key_get("redShiftPassword"))
connection <- connect(details)
expect_true(inherits(connection, "DatabaseConnectorConnection"))
expect_true(disconnect(connection))

# BigQuery
details <- createConnectionDetails(dbms = "bigquery",
                                   connectionString = keyring::key_get("bigQueryConnString"),
                                   user = "",
                                   password = "")
connection <- connect(details)
expect_true(inherits(connection, "DatabaseConnectorConnection"))
expect_true(disconnect(connection))

# Fetch results -------------------------------------------------------------

# PDW
connection <- connect(dbms = "pdw",
                      server = keyring::key_get("pdwServer"),
                      port = keyring::key_get("pdwPort"))
cdmDatabaseSchema <- "CDM_IBM_CCAE_V1247.dbo"
sql <- "SELECT COUNT(*) AS row_count FROM @cdm_database_schema.vocabulary"
renderedSql <- SqlRender::render(sql, cdm_database_schema = cdmDatabaseSchema)

# Fetch data.frame:
count <- querySql(connection, renderedSql)
expect_equal(count[1, 1], 148)
count <- renderTranslateQuerySql(connection, sql, cdm_database_schema = cdmDatabaseSchema)
expect_equal(count[1, 1], 148)

# Fetch Andromeda:
andromeda <- Andromeda::andromeda()
querySqlToAndromeda(connection, renderedSql, andromeda = andromeda, andromedaTableName = "test", snakeCaseToCamelCase = TRUE)
expect_equivalent(dplyr::collect(andromeda$test)$rowCount[1], 148)
renderTranslateQuerySqlToAndromeda(connection, sql, cdm_database_schema = cdmDatabaseSchema, andromeda = andromeda, andromedaTableName = "test2", snakeCaseToCamelCase = TRUE)
expect_equivalent(dplyr::collect(andromeda$test2)$rowCount[1], 148)

disconnect(connection)


# RedShift
connection <- connect(dbms = "redshift",
                      connectionString = keyring::key_get("redShiftConnectionStringCcae"),
                      user = keyring::key_get("redShiftUserName"),
                      password = keyring::key_get("redShiftPassword"))
cdmDatabaseSchema <- "cdm"
sql <- "SELECT COUNT(*) AS row_count FROM @cdm_database_schema.vocabulary"
renderedSql <- SqlRender::render(sql, cdm_database_schema = cdmDatabaseSchema)

# Fetch data.frame:
count <- querySql(connection, renderedSql)
expect_equal(count[1, 1], 164)
count <- renderTranslateQuerySql(connection, sql, cdm_database_schema = cdmDatabaseSchema)
expect_equal(count[1, 1], 164)

# Fetch Andromeda:
andromeda <- Andromeda::andromeda()
querySqlToAndromeda(connection, renderedSql, andromeda = andromeda, andromedaTableName = "test", snakeCaseToCamelCase = TRUE)
expect_equivalent(dplyr::collect(andromeda$test)$rowCount[1], 164)
renderTranslateQuerySqlToAndromeda(connection, sql, cdm_database_schema = cdmDatabaseSchema, andromeda = andromeda, andromedaTableName = "test2", snakeCaseToCamelCase = TRUE)
expect_equivalent(dplyr::collect(andromeda$test2)$rowCount[1], 164)

disconnect(connection)


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
# PDW
details <- createConnectionDetails(dbms = "pdw",
                                   server = keyring::key_get("pdwServer"),
                                   port = keyring::key_get("pdwPort"))
connection <- connect(details)
tables <- getTableNames(connection, "CDM_IBM_CCAE_V1247")
expect_true("PERSON" %in% tables)
disconnect(connection)

# RedShift
details <- createConnectionDetails(dbms = "redshift",
                                   connectionString = keyring::key_get("redShiftConnectionStringCcae"),
                                   user = keyring::key_get("redShiftUserName"),
                                   password = keyring::key_get("redShiftPassword"))
connection <- connect(details)
tables <- getTableNames(connection, "cdm")
expect_true("PERSON" %in% tables)
disconnect(connection)

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

# PDW
details <- createConnectionDetails(dbms = "pdw",
                                   server = keyring::key_get("pdwServer"),
                                   port = keyring::key_get("pdwPort"))
connection <- connect(details)
insertTable(connection = connection,
            tableName = "#temp",
            data = data,
            createTable = TRUE,
            tempTable = TRUE)

# Check data on server is same as local
data2 <- querySql(connection, "SELECT * FROM #temp", integer64AsNumeric = FALSE)
names(data2) <- tolower(names(data2))
expect_equivalent(data[order(data$big_ints), ], data2[order(data2$big_ints), ])


# Check data types
res <- dbSendQuery(connection, "SELECT * FROM #temp")
columnInfo <- dbColumnInfo(res)
dbClearResult(res)
expect_equal(as.character(columnInfo$field.type), c("date", "datetime2", "int", "float", "varchar", "bigint"))

disconnect(connection)


# RedShift
details <- createConnectionDetails(dbms = "redshift",
                                   connectionString = keyring::key_get("redShiftConnectionStringCcae"),
                                   user = keyring::key_get("redShiftUserName"),
                                   password = keyring::key_get("redShiftPassword"))
connection <- connect(details)
insertTable(connection = connection,
            tableName = "#temp",
            data = data,
            createTable = TRUE,
            tempTable = TRUE)

# Check data on server is same as local
data2 <- querySql(connection, "SELECT * FROM #temp", integer64AsNumeric = FALSE)
names(data2) <- tolower(names(data2))
data <- data[order(data$person_id), ]
data2 <- data2[order(data2$person_id), ]
expect_equivalent(data[order(data$big_ints), ], data2[order(data2$big_ints), ])

# Check data types
res <- dbSendQuery(connection, "SELECT * FROM #temp")
columnInfo <- dbColumnInfo(res)
dbClearResult(res)
expect_equal(as.character(columnInfo$field.type), c("date", "timestamp", "int4", "float8", "varchar", "int8"))

disconnect(connection)


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
