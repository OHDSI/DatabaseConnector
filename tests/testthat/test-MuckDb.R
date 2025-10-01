test_that("muckdbConnect creates a valid connection and executes SQL", {
  testthat::skip_if_not_installed("reticulate")
  testthat::skip_if_not(reticulate::py_module_available("sqlglot"))

  conn <- DatabaseConnector::muckdbConnect(platform = "hive", dbDir = ":memory:")

  # Table creation and querying with platform SQL
  DBI::dbGetQuery(conn, "CREATE TABLE myTable AS SELECT 1 AS x")
  result <- DBI::dbGetQuery(conn, "SELECT x FROM myTable")
  testthat::expect_true(is.data.frame(result))
  testthat::expect_true("x" %in% names(result))
  testthat::expect_equal(result$x, 1)

  DBI::dbDisconnect(conn)
})

test_that("connectMuckdb returns a muckdb connection with ICU extension check", {
  testthat::skip_if_not_installed("reticulate")
  testthat::skip_if_not(reticulate::py_module_available("sqlglot"))

  connectionDetails <- createConnectionDetails(
    dbms = "muckdb",
    connectionString = "hive",
    server = function() ":memory:"
  )
  conn <- DatabaseConnector::connect(connectionDetails)

  testthat::expect_true(inherits(conn, "muckdb"))

  # Simple roundtrip
  DBI::dbGetQuery(conn, "CREATE TABLE myTable AS SELECT 2 AS y")
  result <- DBI::dbGetQuery(conn, "SELECT y FROM myTable")
  testthat::expect_equal(result$y, 2)

  DBI::dbDisconnect(conn)
})
