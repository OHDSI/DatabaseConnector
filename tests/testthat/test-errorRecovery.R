library(testthat)

isAutoCommitOn <- function(connection) {
  if (!inherits(connection, "DatabaseConnectorJdbcConnection") || dbms(connection) == "spark") {
    return(TRUE)
  }
  return(rJava::.jcall(connection@jConnection, "Z", "getAutoCommit"))
}

expectConnectionIsUsable <- function(connection, errorReportFile) {
  expect_true(isAutoCommitOn(connection))
  # A failing statement must not leave the connection in an aborted transaction:
  expect_error(
    executeSql(
      connection = connection,
      sql = "INSERT INTO non_existing_table (x) VALUES (1);",
      progressBar = FALSE,
      reportOverallTime = FALSE,
      errorReportFile = errorReportFile
    ),
    "Error executing SQL"
  )
  result <- renderTranslateQuerySql(connection, "SELECT 1 AS x;", errorReportFile = errorReportFile)
  expect_equal(result[1, 1], 1)
  expect_true(isAutoCommitOn(connection))
}

for (testServer in testServers) {
  test_that(addDbmsToLabel("Connection is usable after a failed query", testServer), {
    connection <- connect(testServer$connectionDetails)
    on.exit(disconnect(connection))
    errorReportFile <- tempfile(fileext = ".txt")
    on.exit(unlink(errorReportFile), add = TRUE)

    expect_error(
      querySql(connection, "SELECT * FROM non_existing_table;", errorReportFile = errorReportFile),
      "Error executing SQL"
    )
    expectConnectionIsUsable(connection, errorReportFile)

    if (dbms(connection) == "postgresql") {
      # Error raised while fetching a later batch, after the first rows were already retrieved:
      sql <- "SELECT CASE WHEN x = 10000 THEN 1 / (x - x) ELSE x END AS y FROM generate_series(1, 20000) x;"
      expect_error(
        querySql(connection, sql, errorReportFile = errorReportFile),
        "division by zero"
      )
      expectConnectionIsUsable(connection, errorReportFile)
    }
  })

  test_that(addDbmsToLabel("Connection is usable after a failed insert", testServer), {
    if (testServer$connectionDetails$dbms == "bigquery") {
      skip("Avoiding rate limit on BigQuery")
    }
    connection <- connect(testServer$connectionDetails)
    options(sqlRenderTempEmulationSchema = testServer$tempEmulationSchema)
    on.exit(dropEmulatedTempTables(connection))
    on.exit(disconnect(connection), add = TRUE)
    errorReportFile <- tempfile(fileext = ".txt")
    on.exit(unlink(errorReportFile), add = TRUE)

    renderTranslateExecuteSql(
      connection = connection,
      sql = "CREATE TABLE #not_null_test (x INT NOT NULL);",
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
    expect_error(
      insertTable(
        connection = connection,
        tableName = "#not_null_test",
        data = data.frame(x = c(1L, NA)),
        dropTableIfExists = FALSE,
        createTable = FALSE,
        tempTable = TRUE,
        progressBar = FALSE
      )
    )
    expectConnectionIsUsable(connection, errorReportFile)
    result <- renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT COUNT(*) AS row_count FROM #not_null_test;",
      errorReportFile = errorReportFile
    )
    expect_equal(result[1, 1], 0)
    renderTranslateExecuteSql(
      connection = connection,
      sql = "TRUNCATE TABLE #not_null_test; DROP TABLE #not_null_test;",
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
  })
}
