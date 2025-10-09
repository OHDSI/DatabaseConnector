test_that("muckdbConnect creates a valid connection and executes SQL with DBI S4 dispatch", {
  testthat::skip_if_not_installed("reticulate")
  testthat::skip_if_not(reticulate::py_module_available("sqlglot"))

  conn <- DatabaseConnector::muckdbConnect(platform = "hive", dbDir = ":memory:")
  # Table creation and querying with platform SQL
  DBI::dbSendQuery(conn, "CREATE TABLE myTable AS SELECT 1 AS x")
  result <- DBI::dbGetQuery(conn, "SELECT x FROM myTable")
  testthat::expect_true(is.data.frame(result))
  testthat::expect_true("x" %in% names(result))
  testthat::expect_equal(result$x, 1)

  DBI::dbDisconnect(conn)
})

test_that("connectMuckdb returns a mucked up connection", {
  testthat::skip_if_not_installed("reticulate")
  testthat::skip_if_not(reticulate::py_module_available("sqlglot"))

  connectionDetails <- createConnectionDetails(
    dbms = "muckdb",
    connectionString = "hive",
    server = ":memory:"
  )
  conn <- DatabaseConnector::connect(connectionDetails)
  testthat::expect_true(inherits(conn, "DatabaseConnectorConnection"))

  expect_equal(dbms(conn), "hive")
  # Simple roundtrip
  renderTranslateExecuteSql(conn, "CREATE TABLE myTable AS SELECT 2 AS y")
  result <- renderTranslateQuerySql(conn, "SELECT y FROM myTable")
  testthat::expect_equal(result$y, 2)

  disconnect(conn)
})
