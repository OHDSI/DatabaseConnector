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
