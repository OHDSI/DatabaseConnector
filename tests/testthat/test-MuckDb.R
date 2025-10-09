test_that("connectMuckdb returns a mucked up connection", {
  testthat::skip_if_not_installed("reticulate")
  testthat::skip_if_not(reticulate::py_module_available("sqlglot"))

  platforms <- c("oracle", "postgresql", "bigquery", "spark")

  for (plat in platforms) {
    connectionDetails <- createConnectionDetails(
      dbms = "muckdb",
      connectionString = plat,
      server = ":memory:"
    )

    conn <- DatabaseConnector::connect(connectionDetails)
    testthat::expect_true(inherits(conn, "DatabaseConnectorConnection"))

    expect_equal(dbms(conn), plat)
    renderTranslateExecuteSql(conn, "CREATE TABLE myTable AS SELECT 2 AS y")
    result <- renderTranslateQuerySql(conn, "SELECT y FROM myTable")
    testthat::expect_equal(result$y, 2)

    expect_error(renderTranslateQuerySql(conn, "SELE foo"))
    expect_error(renderTranslateQuerySql(conn, "SELE foo"))
    expect_error(querySql(conn, "SELE foo"))
    expect_error(executeSql(conn, "SELE foo"))
  }

  disconnect(conn)
})
