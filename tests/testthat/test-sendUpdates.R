library(testthat)

# Treating create temp table as separate statement because number of rows affected depends on
# whether we're using temp table emulation:
createSql <- "CREATE TABLE #temp (x INT);"
sql <- "INSERT INTO #temp (x) SELECT 123;
    DELETE FROM #temp WHERE x = 123;
    DROP TABLE #temp;"

for (testServer in testServers) {
  test_that(addDbmsToLabel("Send updates", testServer), {
    connection <- connect(testServer$connectionDetails)
    options(sqlRenderTempEmulationSchema = testServer$tempEmulationSchema)
    on.exit(dropEmulatedTempTables(connection))
    on.exit(disconnect(connection), add = TRUE)
    renderTranslateExecuteSql(connection, createSql)
    expect_equal(renderTranslateExecuteSql(connection, sql), c(1, 1, 0))
    if (testServer$connectionDetails$dbms != "bigquery") {
      # Avoid rate limit error on BigQuery
      renderTranslateExecuteSql(connection, createSql)
      expect_equal(renderTranslateExecuteSql(connection, sql, runAsBatch = TRUE), c(1, 1, 0))
      renderTranslateExecuteSql(connection, createSql)
      rowsAffected <- dbSendStatement(connection, sql)
      expect_equal(dbGetRowsAffected(rowsAffected), 2)
      dbClearResult(rowsAffected)
    }
  })
}
