# Tests for platforms not available to GitHub Actions
library(DatabaseConnector)
library(testthat)

# Set connection details -------------------------------------------------------

# BigQuery
connectionDetailsBigQuery <- createConnectionDetails(
  dbms = "bigquery",
  connectionString = keyring::key_get("bigQueryConnString"),
  user = "",
  password = ""
)
cdmDatabaseSchemaBigQuery <- "synpuf_2m"
scratchDatabaseSchemaBigQuery <- "synpuf_2m_results"

# Azure
connectionDetailsAzure <- createConnectionDetails(
  dbms = "sql server",
  connectionString = keyring::key_get("azureConnectionString"),
  user = keyring::key_get("azureUser"),
  password = keyring::key_get("azurePassword")
)
cdmDatabaseSchemaAzure <- "[sql-synthea-1M].cdm_synthea_1M"
scratchDatabaseSchemaAzure <- "[sql-synthea-1M].mschuemie"

# Spark
# connectionDetailsSparkJdbc <- createConnectionDetails(
#   dbms = "spark",
#   connectionString = keyring::key_get("sparkConnectionString"),
#   user = keyring::key_get("sparkUser"),
#   password = keyring::key_get("sparkPassword")
# )
# connectionDetailsSparkOdbc <- createConnectionDetails(
#   dbms = "spark",
#   server = keyring::key_get("sparkServer"),
#   port = keyring::key_get("sparkPort"),
#   user = keyring::key_get("sparkUser"),
#   password = keyring::key_get("sparkPassword")
# )
# cdmDatabaseSchemaSpark <- "eunomia"
# scratchDatabaseSchemaSpark <- "eunomia"

# DataBricks
connectionDetailsDataBricksJdbc <- createConnectionDetails(
  dbms = "spark",
  connectionString = keyring::key_get("dataBricksConnectionString"),
  user = "token", keyring::key_get("dataBricksUser"),
  password = keyring::key_get("dataBricksPassword")
)
connectionDetailsDataBricksOdbc <- createConnectionDetails(
  dbms = "spark",
  server = keyring::key_get("dataBricksServer"),
  port = keyring::key_get("dataBricksPort"),
  user = keyring::key_get("dataBricksUser"),
  password = keyring::key_get("dataBricksPassword"),
  extraSettings = list(
    HTTPPath = keyring::key_get("dataBricksHttpPath"),
    SSL = 1,
    ThriftTransport = 2,
    AuthMech = 3
    )
)
cdmDatabaseSchemaDataBricks <- "eunomia"
scratchDatabaseSchemaDataBricks <- "scratch"

# Snowflake
connectionDetailsSnowflake <- createConnectionDetails(
  dbms = "snowflake",
  connectionString = keyring::key_get("snowflakeConnectionString"),
  user = keyring::key_get("snowflakeUser"),
  password = keyring::key_get("snowflakePassword")
)
cdmDatabaseSchemaSnowflake <- "ohdsi.eunomia"
scratchDatabaseSchemaSnowflake <- "ohdsi.scratch"

# Open and close connection -----------------------------------------------

# BigQuery
connection <- connect(connectionDetailsBigQuery)
expect_true(inherits(connection, "DatabaseConnectorConnection"))
expect_true(disconnect(connection))

# Azure
connection <- connect(connectionDetailsAzure)
expect_true(inherits(connection, "DatabaseConnectorConnection"))
expect_true(disconnect(connection))

# DataBricks
connection <- connect(connectionDetailsDataBricksJdbc)
expect_true(inherits(connection, "DatabaseConnectorConnection"))
expect_true(disconnect(connection))

connection <- connect(connectionDetailsDataBricksOdbc)
expect_true(inherits(connection, "DatabaseConnectorConnection"))
expect_true(disconnect(connection))

# Snowflake
connection <- connect(connectionDetailsSnowflake)
expect_true(inherits(connection, "DatabaseConnectorConnection"))
expect_true(disconnect(connection))


# Fetch results -------------------------------------------------------------
sql <- "SELECT COUNT(*) AS row_count FROM @cdm_database_schema.vocabulary"

# BigQuery
connection <- connect(connectionDetailsBigQuery)
renderedSql <- SqlRender::render(sql, cdm_database_schema = cdmDatabaseSchemaBigQuery)

# Fetch data.frame:
count <- querySql(connection, renderedSql)
expect_equal(count[1, 1], 96)
count <- renderTranslateQuerySql(connection, sql, cdm_database_schema = cdmDatabaseSchemaBigQuery)
expect_equal(count[1, 1], 96)

# Fetch Andromeda:
andromeda <- Andromeda::andromeda()
querySqlToAndromeda(connection, renderedSql, andromeda = andromeda, andromedaTableName = "test", snakeCaseToCamelCase = TRUE)
expect_equivalent(dplyr::collect(andromeda$test)$rowCount[1], 96)
renderTranslateQuerySqlToAndromeda(connection, sql, cdm_database_schema = cdmDatabaseSchemaBigQuery, andromeda = andromeda, andromedaTableName = "test2", snakeCaseToCamelCase = TRUE)
expect_equivalent(dplyr::collect(andromeda$test2)$rowCount[1], 96)

disconnect(connection)


# Azure
connection <- connect(connectionDetailsAzure)
renderedSql <- SqlRender::render(sql, cdm_database_schema = cdmDatabaseSchemaAzure)

# Fetch data.frame:
count <- querySql(connection, renderedSql)
expect_equal(count[1, 1], 63)
count <- renderTranslateQuerySql(connection, sql, cdm_database_schema = cdmDatabaseSchemaAzure)
expect_equal(count[1, 1], 63)

# Fetch Andromeda:
andromeda <- Andromeda::andromeda()
querySqlToAndromeda(connection, renderedSql, andromeda = andromeda, andromedaTableName = "test", snakeCaseToCamelCase = TRUE)
expect_equivalent(dplyr::collect(andromeda$test)$rowCount[1], 63)
renderTranslateQuerySqlToAndromeda(connection, sql, cdm_database_schema = cdmDatabaseSchemaAzure, andromeda = andromeda, andromedaTableName = "test2", snakeCaseToCamelCase = TRUE)
expect_equivalent(dplyr::collect(andromeda$test2)$rowCount[1], 63)

disconnect(connection)


# DataBricks JDBC
connection <- connect(connectionDetailsDataBricksJdbc)
renderedSql <- SqlRender::render(sql, cdm_database_schema = cdmDatabaseSchemaDataBricks)

# Fetch data.frame:
count <- querySql(connection, renderedSql)
expect_equal(count[1, 1], 125)
count <- renderTranslateQuerySql(connection, sql, cdm_database_schema = cdmDatabaseSchemaDataBricks)
expect_equal(count[1, 1], 125)

# Fetch Andromeda:
andromeda <- Andromeda::andromeda()
querySqlToAndromeda(connection, renderedSql, andromeda = andromeda, andromedaTableName = "test", snakeCaseToCamelCase = TRUE)
expect_equivalent(dplyr::collect(andromeda$test)$rowCount[1], 125)
renderTranslateQuerySqlToAndromeda(connection, sql, cdm_database_schema = cdmDatabaseSchemaDataBricks, andromeda = andromeda, andromedaTableName = "test2", snakeCaseToCamelCase = TRUE)
expect_equivalent(dplyr::collect(andromeda$test2)$rowCount[1], 125)

disconnect(connection)


# DataBricks ODBC
connection <- connect(connectionDetailsDataBricksOdbc)
renderedSql <- SqlRender::render(sql, cdm_database_schema = cdmDatabaseSchemaDataBricks)

# Fetch data.frame:
count <- querySql(connection, renderedSql)
expect_equal(count[1, 1], 125)
count <- renderTranslateQuerySql(connection, sql, cdm_database_schema = cdmDatabaseSchemaDataBricks)
expect_equal(count[1, 1], 125)

# Fetch Andromeda:
andromeda <- Andromeda::andromeda()
querySqlToAndromeda(connection, renderedSql, andromeda = andromeda, andromedaTableName = "test", snakeCaseToCamelCase = TRUE)
expect_equivalent(dplyr::collect(andromeda$test)$rowCount[1], 125)
renderTranslateQuerySqlToAndromeda(connection, sql, cdm_database_schema = cdmDatabaseSchemaDataBricks, andromeda = andromeda, andromedaTableName = "test2", snakeCaseToCamelCase = TRUE)
expect_equivalent(dplyr::collect(andromeda$test2)$rowCount[1], 125)

disconnect(connection)


# Snowflake
connection <- connect(connectionDetailsSnowflake)
renderedSql <- SqlRender::render(sql, cdm_database_schema = cdmDatabaseSchemaSnowflake)

# Fetch data.frame:
count <- querySql(connection, renderedSql)
expect_equal(count[1, 1], 125)
count <- renderTranslateQuerySql(connection, sql, cdm_database_schema = cdmDatabaseSchemaSnowflake)
expect_equal(count[1, 1], 125)

# Fetch Andromeda:
andromeda <- Andromeda::andromeda()
querySqlToAndromeda(connection, renderedSql, andromeda = andromeda, andromedaTableName = "test", snakeCaseToCamelCase = TRUE)
expect_equivalent(dplyr::collect(andromeda$test)$rowCount[1], 125)
renderTranslateQuerySqlToAndromeda(connection, sql, cdm_database_schema = cdmDatabaseSchemaSnowflake, andromeda = andromeda, andromedaTableName = "test2", snakeCaseToCamelCase = TRUE)
expect_equivalent(dplyr::collect(andromeda$test2)$rowCount[1], 125)

disconnect(connection)


# Get table names ----------------------------------------------------------------------

# BigQuery
connection <- connect(connectionDetailsBigQuery)
tables <- getTableNames(connection, cdmDatabaseSchemaBigQuery)
expect_true("person" %in% tables)
disconnect(connection)

# Azure
connection <- connect(connectionDetailsAzure)
tables <- getTableNames(connection, cdmDatabaseSchemaAzure)
expect_true("person" %in% tables)
disconnect(connection)

# DataBricks
connection <- connect(connectionDetailsDataBricksJdbc)
tables <- getTableNames(connection, cdmDatabaseSchemaDataBricks)
expect_true("person" %in% tables)
disconnect(connection)

connection <- connect(connectionDetailsDataBricksOdbc)
tables <- getTableNames(connection, cdmDatabaseSchemaDataBricks)
expect_true("person" %in% tables)
disconnect(connection)

# Snowflake
connection <- connect(connectionDetailsSnowflake)
tables <- getTableNames(connection, cdmDatabaseSchemaSnowflake)
expect_true("person" %in% tables)
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
connection <- connect(connectionDetailsBigQuery)
insertTable(connection = connection,
            tableName = paste(scratchDatabaseSchemaBigQuery, "insert_test", sep= "."),
            data = data,
            createTable = TRUE,
            tempTable = FALSE,
            tempEmulationSchema = scratchDatabaseSchemaBigQuery)

# Check data on server is same as local
data2 <- renderTranslateQuerySql(
  connection = connection, 
  sql = "SELECT * FROM @scratch_database_schema.insert_test", 
  scratch_database_schema = scratchDatabaseSchemaBigQuery,
  integer64AsNumeric = FALSE)
names(data2) <- tolower(names(data2))
data <- data[order(data$person_id), ]
data2 <- data2[order(data2$person_id), ]
row.names(data) <- NULL
row.names(data2) <- NULL
expect_equal(data[order(data$big_ints), ], data2[order(data2$big_ints), ])

# Check data types
res <- dbSendQuery(connection, SqlRender::render("SELECT * FROM @scratch_database_schema.insert_test", scratch_database_schema = scratchDatabaseSchemaBigQuery))
columnInfo <- dbColumnInfo(res)
dbClearResult(res)
expect_equal(as.character(columnInfo$field.type), c("DATE", "DATETIME", "INT64", "FLOAT64", "STRING", "INT64"))

executeSql(connection, SqlRender::render("DROP TABLE @scratch_database_schema.insert_test", scratch_database_schema = scratchDatabaseSchemaBigQuery))

disconnect(connection)


# Azure
connection <- connect(connectionDetailsAzure)

insertTable(connection = connection,
            tableName = paste(scratchDatabaseSchemaAzure, "insert_test", sep= "."),
            data = data,
            createTable = TRUE,
            tempTable = FALSE)

# Check data on server is same as local
data2 <- renderTranslateQuerySql(
  connection = connection, 
  sql = "SELECT * FROM @scratch_database_schema.insert_test", 
  scratch_database_schema = scratchDatabaseSchemaAzure,
  integer64AsNumeric = FALSE)
names(data2) <- tolower(names(data2))
data <- data[order(data$person_id), ]
data2 <- data2[order(data2$person_id), ]
row.names(data) <- NULL
row.names(data2) <- NULL
expect_equal(data[order(data$big_ints), ], data2[order(data2$big_ints), ])

# Check data types
res <- dbSendQuery(connection, SqlRender::render("SELECT * FROM @scratch_database_schema.insert_test", scratch_database_schema = scratchDatabaseSchemaAzure))
columnInfo <- dbColumnInfo(res)
dbClearResult(res)
expect_equal(as.character(columnInfo$field.type), c("date", "datetime2", "int", "float", "varchar", "bigint"))

executeSql(connection, SqlRender::render("DROP TABLE @scratch_database_schema.insert_test", scratch_database_schema = scratchDatabaseSchemaAzure))

disconnect(connection)


# DataBricks JDBC
connection <- connect(connectionDetailsDataBricksJdbc)
insertTable(connection = connection,
            databaseSchema = scratchDatabaseSchemaDataBricks,
            tableName ="insert_test",
            data = data,
            createTable = TRUE,
            tempTable = FALSE)

# Check data on server is same as local
data2 <- renderTranslateQuerySql(
  connection = connection, 
  sql = "SELECT * FROM @scratch_database_schema.insert_test", 
  scratch_database_schema = scratchDatabaseSchemaDataBricks,
  integer64AsNumeric = FALSE)
names(data2) <- tolower(names(data2))
data <- data[order(data$person_id), ]
data2 <- data2[order(data2$person_id), ]
row.names(data) <- NULL
row.names(data2) <- NULL
expect_equal(data, data2, tolerance = 1e7)

# Check data types
res <- dbSendQuery(connection, SqlRender::render("SELECT * FROM @scratch_database_schema.insert_test", scratch_database_schema = scratchDatabaseSchemaDataBricks))
columnInfo <- dbColumnInfo(res)
dbClearResult(res)
expect_equal(as.character(columnInfo$field.type), c("DATE", "TIMESTAMP", "INT", "FLOAT", "STRING", "BIGINT"))

executeSql(connection, SqlRender::render("DROP TABLE @scratch_database_schema.insert_test", scratch_database_schema = scratchDatabaseSchemaDataBricks))

disconnect(connection)


# DataBricks ODBC
connection <- connect(connectionDetailsDataBricksOdbc)
insertTable(connection = connection,
            databaseSchema = scratchDatabaseSchemaDataBricks,
            tableName = "insert_test",
            data = data,
            createTable = TRUE,
            tempTable = FALSE)

# Check data on server is same as local
data2 <- renderTranslateQuerySql(
  connection = connection, 
  sql = "SELECT * FROM @scratch_database_schema.insert_test", 
  scratch_database_schema = scratchDatabaseSchemaDataBricks,
  integer64AsNumeric = FALSE)
names(data2) <- tolower(names(data2))
data <- data[order(data$person_id), ]
data2 <- data2[order(data2$person_id), ]
row.names(data) <- NULL
row.names(data2) <- NULL
attr(data2$some_datetime, "tzone") <- NULL
expect_equal(data, data2)

# Check data types
res <- dbSendQuery(connection, SqlRender::render("SELECT * FROM @scratch_database_schema.insert_test", scratch_database_schema = scratchDatabaseSchemaDataBricks))
columnInfo <- dbColumnInfo(res)
dbClearResult(res)
expect_equal(columnInfo$type, c("91", "93", "4", "8", "12" ,"12"))

executeSql(connection, SqlRender::render("DROP TABLE @scratch_database_schema.insert_test", scratch_database_schema = scratchDatabaseSchemaDataBricks))

disconnect(connection)


# Snowflake
connection <- connect(connectionDetailsSnowflake)

insertTable(connection = connection,
            tableName = paste(scratchDatabaseSchemaSnowflake, "insert_test", sep= "."),
            data = data,
            createTable = TRUE,
            tempTable = FALSE)

# Check data on server is same as local
data2 <- renderTranslateQuerySql(
  connection = connection, 
  sql = "SELECT * FROM @scratch_database_schema.insert_test", 
  scratch_database_schema = scratchDatabaseSchemaSnowflake,
  integer64AsNumeric = FALSE)
names(data2) <- tolower(names(data2))
data <- data[order(data$value), ]
data2 <- data2[order(data2$value), ]
row.names(data) <- NULL
row.names(data2) <- NULL
expect_equal(data, data2) 

# Check data types
res <- dbSendQuery(connection, SqlRender::render("SELECT * FROM @scratch_database_schema.insert_test", scratch_database_schema = scratchDatabaseSchemaSnowflake))
columnInfo <- dbColumnInfo(res)
dbClearResult(res)
expect_equal(as.character(columnInfo$field.type), c("DATE", "TIMESTAMPNTZ", "NUMBER", "DOUBLE", "VARCHAR", "NUMBER"))

executeSql(connection, SqlRender::render("DROP TABLE @scratch_database_schema.insert_test", scratch_database_schema = scratchDatabaseSchemaSnowflake))

disconnect(connection)

# Test dropEmulatedTempTables ----------------------------------------------

# BigQuery
connection <- connect(connectionDetailsBigQuery)
insertTable(connection = connection,
            tableName = "temp",
            data = cars,
            createTable = TRUE,
            tempTable = TRUE,
            tempEmulationSchema = scratchDatabaseSchemaBigQuery)

droppedTables <- dropEmulatedTempTables(connection = connection, tempEmulationSchema = scratchDatabaseSchemaBigQuery)
expect_equal(droppedTables, sprintf("%s.%stemp", scratchDatabaseSchemaBigQuery, SqlRender::getTempTablePrefix()))
disconnect(connection)

# DataBricks JDBC
connection <- connect(connectionDetailsDataBricksJdbc)
insertTable(connection = connection,
            tableName = "temp",
            data = cars,
            createTable = TRUE,
            tempTable = TRUE,
            tempEmulationSchema = scratchDatabaseSchemaDataBricks)

droppedTables <- dropEmulatedTempTables(connection = connection, tempEmulationSchema = scratchDatabaseSchemaDataBricks)
expect_equal(droppedTables, sprintf("%s.%stemp", scratchDatabaseSchemaDataBricks, SqlRender::getTempTablePrefix()))
disconnect(connection)

# DataBricks ODBC
connection <- connect(connectionDetailsDataBricksOdbc)
insertTable(connection = connection,
            tableName = "temp",
            data = cars,
            createTable = TRUE,
            tempTable = TRUE,
            tempEmulationSchema = scratchDatabaseSchemaDataBricks)

droppedTables <- dropEmulatedTempTables(connection = connection, tempEmulationSchema = scratchDatabaseSchemaDataBricks)
expect_equal(droppedTables, sprintf("%s.%stemp", scratchDatabaseSchemaDataBricks, SqlRender::getTempTablePrefix()))
disconnect(connection)

# Snowflake
connection <- connect(connectionDetailsSnowflake)
insertTable(connection = connection,
            tableName = "temp",
            data = cars,
            createTable = TRUE,
            tempTable = TRUE,
            tempEmulationSchema = scratchDatabaseSchemaSnowflake)

droppedTables <- dropEmulatedTempTables(connection = connection, tempEmulationSchema = scratchDatabaseSchemaSnowflake)
expect_equal(droppedTables, sprintf("%s.%stemp", scratchDatabaseSchemaSnowflake, SqlRender::getTempTablePrefix()))
disconnect(connection)


# Test hash computation ----------------------------------------------

# BigQuery
connection <- connect(connectionDetailsBigQuery)
hash <- computeDataHash(connection = connection,
                        databaseSchema = cdmDatabaseSchemaBigQuery)
expect_true(is.character(hash))
disconnect(connection)

# Azure
connection <- connect(connectionDetailsAzure)
hash <- computeDataHash(connection = connection,
                        databaseSchema = cdmDatabaseSchemaAzure)
expect_true(is.character(hash))
disconnect(connection)

# DataBricks
connection <- connect(connectionDetailsDataBricksJdbc)
hash <- computeDataHash(connection = connection,
                        databaseSchema = cdmDatabaseSchemaDataBricks)
expect_true(is.character(hash))
disconnect(connection)

connection <- connect(connectionDetailsDataBricksOdbc)
hash <- computeDataHash(connection = connection,
                        databaseSchema = cdmDatabaseSchemaDataBricks)
expect_true(is.character(hash))
disconnect(connection)

# Snowflake
connection <- connect(connectionDetailsSnowflake)
hash <- computeDataHash(connection = connection,
                        databaseSchema = cdmDatabaseSchemaSnowflake)
expect_true(is.character(hash))
disconnect(connection)

# Test dbplyr ------------------------------------------------------------------

source("tests/testthat/dbplyrTestFunction.R")
# options("DEBUG_DATABASECONNECTOR_DBPLYR" = TRUE)
# BigQuery
options(sqlRenderTempEmulationSchema = scratchDatabaseSchemaBigQuery)
testDbplyrFunctions(connectionDetails = connectionDetailsBigQuery, 
                    cdmDatabaseSchema = cdmDatabaseSchemaBigQuery)

# Azure
testDbplyrFunctions(connectionDetails = connectionDetailsAzure, 
                    cdmDatabaseSchema = cdmDatabaseSchemaAzure)

# DataBricks
options(sqlRenderTempEmulationSchema = scratchDatabaseSchemaDataBricks)
testDbplyrFunctions(connectionDetails = connectionDetailsDataBricksJdbc, 
                    cdmDatabaseSchema = cdmDatabaseSchemaDataBricks)

testDbplyrFunctions(connectionDetails = connectionDetailsDataBricksOdbc, 
                    cdmDatabaseSchema = cdmDatabaseSchemaDataBricks)

# Snowflake
options(sqlRenderTempEmulationSchema = scratchDatabaseSchemaSnowflake)
testDbplyrFunctions(connectionDetails = connectionDetailsSnowflake, 
                    cdmDatabaseSchema = cdmDatabaseSchemaSnowflake)


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

# DataQualityDashboard ---------------------------------------------------------

DataQualityDashboard::executeDqChecks(
  connectionDetails = connectionDetailsBigQuery,
  cdmDatabaseSchema = cdmDatabaseSchemaBigQuery,
  resultsDatabaseSchema = scratchDatabaseSchemaBigQuery,
  cdmSourceName = "Synpuf",
  outputFolder = "d:/temp/dqd",
  outputFile = "d:/temp/dqd/output.txt",
  writeToTable = FALSE
)






# Random stuff -----------------------------------------------------------------

connection <- connect(connectionDetailsBigQuery)

querySql(connection, "SELECT * FROM synpuf_2m.cdm_source;")

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
                     Host = keyring::key_get("dataBricksServer"),
                     uid = keyring::key_get("dataBricksUser"),
                     pwd = keyring::key_get("dataBricksPassword"),
                     UseNativeQuery = 1,
                     HTTPPath = keyring::key_get("dataBricksHttpPath"),
                     SSL = 1,
                     ThriftTransport = 2,
                     AuthMech = 3,
                     Port = keyring::key_get("dataBricksPort"))

db <- DBI::dbConnect(odbc::odbc(),
                     Driver = "Simba Spark ODBC Driver",
                     Host = keyring::key_get("sparkServer"),
                     uid = keyring::key_get("sparkUser"),
                     pwd = keyring::key_get("sparkPassword"),
                     UseNativeQuery=1,
                     Port = keyring::key_get("sparkPort"))

DBI::dbExecute(db, "DROP TABLE cars;")
DBI::dbGetQuery(db, "SELECT * FROM eunomia.achilles_results LIMIT 1;")
DBI::dbExecute(db, "CREATE TABLE eunomia.test(x INT);")
DBI::dbExecute(db, "DROP TABLE eunomia.test;")

DBI::dbExecute(db, "USE eunomia;")
DBI::dbExecute(db, "DROP TABLE \"test\";")
DBI::dbListTables(db, schema = "eunomia")
DBI::dbGetQuery(db, "SELECT * FROM eunomia.person LIMIT 1;")

DBI::dbExecute(db, "DROP TABLE scratch.test;")
DBI::dbWriteTable(db, DBI::Id(schema = "scratch", table = "test"), cars)



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

library(DatabaseConnector)
connectionDetails <- createConnectionDetails(dbms = "spark",
                                             server = keyring::key_get("sparkServer"),
                                             port = keyring::key_get("sparkPort"),
                                             user = keyring::key_get("sparkUser"),
                                             password = keyring::key_get("sparkPassword"))

connection <- connect(connectionDetails)
dbGetQuery(connection, "SELECT TOP 5 * FROM eunomia.person;")
querySql(connection, "SELECT  FROM eunomia.person;")
disconnect(connection)


# ODBC
db <- DBI::dbConnect(odbc::odbc(),
                     server = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
                     uid = Sys.getenv("CDM5_SQL_SERVER_USER"),
                     pwd = URLdecode(Sys.getenv("CDM5_SQL_SERVER_PASSWORD")),
                     driver = "SQL Server",
                     Trusted_Connection=TRUE)


