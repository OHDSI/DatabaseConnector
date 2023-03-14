library(testthat)

test_that("Compute data hash", {
  # Postgresql -----------------------------------------------------------------
  details <- createConnectionDetails(
    dbms = "postgresql",
    user = Sys.getenv("CDM5_POSTGRESQL_USER"),
    password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
    server = Sys.getenv("CDM5_POSTGRESQL_SERVER")
  )
  connection <- connect(details)
  hash <- computeDataHash(connection, Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"))
  expect_true(is.character(hash))

  disconnect(connection)

  # SQL Server -----------------------------------------------------------------
  details <- createConnectionDetails(
    dbms = "sql server",
    user = Sys.getenv("CDM5_SQL_SERVER_USER"),
    password = URLdecode(Sys.getenv("CDM5_SQL_SERVER_PASSWORD")),
    server = Sys.getenv("CDM5_SQL_SERVER_SERVER")
  )
  connection <- connect(details)
  hash <- computeDataHash(connection, Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA"))
  expect_true(is.character(hash))
  
  disconnect(connection)
  
  # Oracle ---------------------------------------------------------------------
  details <- createConnectionDetails(
    dbms = "oracle",
    user = Sys.getenv("CDM5_ORACLE_USER"),
    password = URLdecode(Sys.getenv("CDM5_ORACLE_PASSWORD")),
    server = Sys.getenv("CDM5_ORACLE_SERVER")
  )
  connection <- connect(details)
  hash <- computeDataHash(connection, Sys.getenv("CDM5_ORACLE_CDM_SCHEMA"))
  expect_true(is.character(hash))
  
  disconnect(connection)
  
  # RedShift  ------------------------------------------------------------------
  details <- createConnectionDetails(
    dbms = "redshift",
    user = Sys.getenv("CDM5_REDSHIFT_USER"),
    password = URLdecode(Sys.getenv("CDM5_REDSHIFT_PASSWORD")),
    server = Sys.getenv("CDM5_REDSHIFT_SERVER")
  )
  connection <- connect(details)
  hash <- computeDataHash(connection, Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA"))
  expect_true(is.character(hash))
  
  disconnect(connection)
  
  # RSQLite --------------------------------------------------------------------
  dbFile <- tempfile(fileext = "sqlite")
  details <- createConnectionDetails(
    dbms = "sqlite",
    server = dbFile
  )
  connection <- connect(details)
  DatabaseConnector::insertTable(
    connection = connection,
    databaseSchema = "main",
    tableName = "cars",
    data = cars,
    createTable = TRUE
  )
  DatabaseConnector::insertTable(
    connection = connection,
    databaseSchema = "main",
    tableName = "iris",
    data = iris,
    createTable = TRUE
  )
  hash <- computeDataHash(connection, "main")
  expect_true(is.character(hash))
  
  disconnect(connection)
  unlink(dbFile)
  
  # DuckDB --------------------------------------------------------------------
  dbFile <- tempfile(fileext = "duckdb")
  details <- createConnectionDetails(
    dbms = "duckdb",
    server = dbFile
  )
  connection <- connect(details)
  DatabaseConnector::insertTable(
    connection = connection,
    databaseSchema = "main",
    tableName = "cars",
    data = cars,
    createTable = TRUE
  )
  DatabaseConnector::insertTable(
    connection = connection,
    databaseSchema = "main",
    tableName = "iris",
    data = iris,
    createTable = TRUE
  )
  hash <- computeDataHash(connection, "main")
  expect_true(is.character(hash))
  
  disconnect(connection)
  unlink(dbFile)
})
