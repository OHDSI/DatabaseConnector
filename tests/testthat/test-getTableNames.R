library(testthat)

test_that("Get table names", {
  # Postgresql --------------------------------------------------
  details <- createConnectionDetails(
    dbms = "postgresql",
    user = Sys.getenv("CDM5_POSTGRESQL_USER"),
    password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
    server = Sys.getenv("CDM5_POSTGRESQL_SERVER")
  )
  connection <- connect(details)
  tables <- getTableNames(connection, Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"))
  expect_true("PERSON" %in% tables)
  expect_true(existsTable(connection, Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"), "person"))
  disconnect(connection)

  # SQL Server --------------------------------------------------
  details <- createConnectionDetails(
    dbms = "sql server",
    user = Sys.getenv("CDM5_SQL_SERVER_USER"),
    password = URLdecode(Sys.getenv("CDM5_SQL_SERVER_PASSWORD")),
    server = Sys.getenv("CDM5_SQL_SERVER_SERVER")
  )
  connection <- connect(details)
  tables <- getTableNames(connection, Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA"))
  expect_true("PERSON" %in% tables)
  expect_true(existsTable(connection, Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA"), "person"))
  disconnect(connection)

  # Oracle --------------------------------------------------
  details <- createConnectionDetails(
    dbms = "oracle",
    user = Sys.getenv("CDM5_ORACLE_USER"),
    password = URLdecode(Sys.getenv("CDM5_ORACLE_PASSWORD")),
    server = Sys.getenv("CDM5_ORACLE_SERVER")
  )
  connection <- connect(details)
  tables <- getTableNames(connection, Sys.getenv("CDM5_ORACLE_CDM_SCHEMA"))
  expect_true("PERSON" %in% tables)
  expect_true(existsTable(connection, Sys.getenv("CDM5_ORACLE_CDM_SCHEMA"), "person"))
  disconnect(connection)

  # Sqlite --------------------------------------------------
  dbFile <- tempfile()
  details <- createConnectionDetails(
    dbms = "sqlite",
    server = dbFile
  )
  connection <- connect(details)
  executeSql(connection, "CREATE TABLE person (x INT);")
  tables <- getTableNames(connection, "main")
  expect_true("PERSON" %in% tables)
  expect_true(existsTable(connection, "main", "person"))
  disconnect(connection)
  unlink(dbFile)

  # RedShift --------------------------------------------------
  details <- createConnectionDetails(
    dbms = "redshift",
    user = Sys.getenv("CDM5_REDSHIFT_USER"),
    password = URLdecode(Sys.getenv("CDM5_REDSHIFT_PASSWORD")),
    server = Sys.getenv("CDM5_REDSHIFT_SERVER")
  )
  connection <- connect(details)
  tables <- getTableNames(connection, Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA"))
  expect_true("PERSON" %in% tables)
  expect_true(existsTable(connection, Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA"), "person"))
  disconnect(connection)
})

test_that("Cleaning of database or schema name", {
  expect_equivalent(cleanSchemaName("[test\\name]"), "test\\\\name")
  expect_equivalent(cleanSchemaName("\"test\\name\""), "test\\\\name")
  expect_equivalent(cleanDatabaseName("test\\name"), "test\\name")
})

test_that("Get table names using DBI drivers", {
  skip_if_not(Sys.getenv("RUN_DATABASECONNECTOR_DBI_DRIVER_TESTS") == "TRUE")
  
  # Postgresql with RPostgres driver -----------------------------
  connection <-  con <- DBI::dbConnect(RPostgres::Postgres(),
                                       dbname = Sys.getenv("CDM5_POSTGRESQL_DBNAME"),
                                       host = Sys.getenv("CDM5_POSTGRESQL_HOST"),
                                       user = Sys.getenv("CDM5_POSTGRESQL_USER"),
                                       password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD"))
  tables <- getTableNames(connection, Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"))
  expect_true("PERSON" %in% tables)
  expect_true(existsTable(connection, Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"), "person"))
  DBI::dbDisconnect(connection)
  
  # SQL Server with odbc driver ----------------------------------
  connection <- DBI::dbConnect(odbc::odbc(),
                               Driver   = "ODBC Driver 18 for SQL Server",
                               Server   = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
                               Database = Sys.getenv("CDM5_SQL_SERVER_CDM_DATABASE"),
                               UID      = Sys.getenv("CDM5_SQL_SERVER_USER"),
                               PWD      = Sys.getenv("CDM5_SQL_SERVER_PASSWORD"),
                               TrustServerCertificate = "yes",
                               Port     = 1433)
  tables <- getTableNames(connection, Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA"))
  expect_true("PERSON" %in% tables)
  expect_true(existsTable(connection, Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA"), "person"))
  DBI::dbDisconnect(connection)
  
  # Sqlite --------------------------------------------------
  
  dbFile <- tempfile()
  connection <- DBI::dbConnect(RSQLite::SQLite(), dbname = dbFile)
  DBI::dbExecute(connection, "CREATE TABLE person (x INT);")
  tables <- getTableNames(connection, "main")
  expect_true("PERSON" %in% tables)
  expect_true(existsTable(connection, "main", "person"))
  DBI::dbDisconnect(connection)
  unlink(dbFile)
  
  # duckdb --------------------------------------------------
  
  connection <- DBI::dbConnect(duckdb::duckdb())
  DBI::dbExecute(connection, "CREATE SCHEMA test")
  DBI::dbExecute(connection, "CREATE TABLE test.person (x INT);")
  tables <- getTableNames(connection, "test")
  expect_true("PERSON" %in% tables)
  expect_true(existsTable(connection, "test", "person"))
  DBI::dbDisconnect(connection)
  
  # RedShift --------------------------------------------------
  connection <- DBI::dbConnect(RPostgres::Redshift(),
                               dbname   = Sys.getenv("CDM5_REDSHIFT_DBNAME"),
                               host     = Sys.getenv("CDM5_REDSHIFT_HOST"),
                               port     = Sys.getenv("CDM5_REDSHIFT_PORT"),
                               user     = Sys.getenv("CDM5_REDSHIFT_USER"),
                               password = Sys.getenv("CDM5_REDSHIFT_PASSWORD"))
  
  tables <- getTableNames(connection, Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA"))
  expect_true("PERSON" %in% tables)
  expect_true(existsTable(connection, Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA"), "person"))
  DBI::dbDisconnect(connection)
})
