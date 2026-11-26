library(testthat)

for (testServer in testServers) {
  test_that(addDbmsToLabel("Remove table", testServer), {
    connection <- connect(testServer$connectionDetails)
    options(sqlRenderTempEmulationSchema = testServer$tempEmulationSchema)
    on.exit(dropEmulatedTempTables(connection))
    on.exit(disconnect(connection), add = TRUE)
    errorReportFile <- tempfile(fileext = ".txt")
    on.exit(unlink(errorReportFile), add = TRUE)

    insertTable(
      connection = connection,
      tableName = "#remove_test",
      data = data.frame(x = 1:3),
      tempTable = TRUE,
      progressBar = FALSE
    )
    result <- renderTranslateQuerySql(connection, "SELECT COUNT(*) AS row_count FROM #remove_test;")
    expect_equal(result[1, 1], 3)

    expect_true(DBI::dbRemoveTable(connection, "#remove_test"))
    expect_error(
      renderTranslateQuerySql(
        connection = connection,
        sql = "SELECT COUNT(*) AS row_count FROM #remove_test;",
        errorReportFile = errorReportFile
      ),
      "Error executing SQL"
    )

    if (testServer$connectionDetails$dbms %in% c("sqlite", "duckdb")) {
      # Permanent table in a schema:
      insertTable(
        connection = connection,
        databaseSchema = testServer$cdmDatabaseSchema,
        tableName = "remove_test",
        data = data.frame(x = 1:3),
        progressBar = FALSE
      )
      expect_true(existsTable(connection, testServer$cdmDatabaseSchema, "remove_test"))
      expect_true(DBI::dbRemoveTable(connection, "remove_test", databaseSchema = testServer$cdmDatabaseSchema))
      expect_false(existsTable(connection, testServer$cdmDatabaseSchema, "remove_test"))
    }
  })
}
