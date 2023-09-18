library(testthat)

if (DatabaseConnector:::is_installed("ParallelLogger")) {
  logFileName <- tempfile(fileext = ".txt")
  ParallelLogger::addDefaultFileLogger(logFileName, name = "TEST_LOGGER")
}

test_that("Send updates to server", {
  sql <- "CREATE TABLE #temp (x INT);
    INSERT INTO #temp (x) SELECT 123;
    DELETE FROM #temp WHERE x = 123;
    DROP TABLE #temp;"

  # Postgresql --------------------------------------------------
  details <- createConnectionDetails(
    dbms = "postgresql",
    user = Sys.getenv("CDM5_POSTGRESQL_USER"),
    password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
    server = Sys.getenv("CDM5_POSTGRESQL_SERVER")
  )
  connection <- connect(details)

  expect_equal(renderTranslateExecuteSql(connection, sql), c(0, 1, 1, 0))

  expect_equal(renderTranslateExecuteSql(connection, sql, runAsBatch = TRUE), c(0, 1, 1, 0))

  rowsAffected <- dbSendStatement(connection, sql)
  expect_equal(dbGetRowsAffected(rowsAffected), 2)
  dbClearResult(rowsAffected)
  
  disconnect(connection)

  # SQL Server --------------------------------------------------
  details <- createConnectionDetails(
    dbms = "sql server",
    user = Sys.getenv("CDM5_SQL_SERVER_USER"),
    password = URLdecode(Sys.getenv("CDM5_SQL_SERVER_PASSWORD")),
    server = Sys.getenv("CDM5_SQL_SERVER_SERVER")
  )
  connection <- connect(details)

  expect_equal(renderTranslateExecuteSql(connection, sql), c(0, 1, 1, 0))
  
  expect_equal(renderTranslateExecuteSql(connection, sql, runAsBatch = TRUE), c(0, 1, 1, 0))
  
  rowsAffected <- dbSendStatement(connection, sql)
  expect_equal(dbGetRowsAffected(rowsAffected), 2)
  dbClearResult(rowsAffected)

  disconnect(connection)

  # Oracle --------------------------------------------------
  details <- createConnectionDetails(
    dbms = "oracle",
    user = Sys.getenv("CDM5_ORACLE_USER"),
    password = URLdecode(Sys.getenv("CDM5_ORACLE_PASSWORD")),
    server = Sys.getenv("CDM5_ORACLE_SERVER")
  )
  connection <- connect(details)

  expect_equal(renderTranslateExecuteSql(connection, sql), c(0, 1, 1, 0))
  
  expect_equal(renderTranslateExecuteSql(connection, sql, runAsBatch = TRUE), c(0, 1, 1, 0))
  
  rowsAffected <- dbSendStatement(connection, sql)
  expect_equal(dbGetRowsAffected(rowsAffected), 2)
  dbClearResult(rowsAffected)

  disconnect(connection)

  # RedShift --------------------------------------------------
  details <- createConnectionDetails(
    dbms = "redshift",
    user = Sys.getenv("CDM5_REDSHIFT_USER"),
    password = URLdecode(Sys.getenv("CDM5_REDSHIFT_PASSWORD")),
    server = Sys.getenv("CDM5_REDSHIFT_SERVER")
  )
  connection <- connect(details)

  expect_equal(renderTranslateExecuteSql(connection, sql), c(0, 1, 1, 0))
  
  expect_equal(renderTranslateExecuteSql(connection, sql, runAsBatch = TRUE), c(0, 1, 1, 0))
  
  rowsAffected <- dbSendStatement(connection, sql)
  expect_equal(dbGetRowsAffected(rowsAffected), 2)
  dbClearResult(rowsAffected)

  disconnect(connection)
})

test_that("Logging update times", {
  skip_if_not_installed("ParallelLogger")
  log <- readLines(logFileName)
  statementCount <- sum(grepl("Executing SQL:", log))
  expect_gt(statementCount, 19)
  # writeLines(log)
  ParallelLogger::unregisterLogger("TEST_LOGGER")
  unlink(logFileName)
})
