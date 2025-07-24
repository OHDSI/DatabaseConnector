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
      translatedSql <- SqlRender::translate(sql, dbms(connection))
      statements <- SqlRender::splitSql(translatedSql)
      rowsAffected <- dbSendStatement(connection, statements[1])
      expect_equal(dbGetRowsAffected(rowsAffected), 1)
      rowsAffected <- dbSendStatement(connection, statements[2])
      expect_equal(dbGetRowsAffected(rowsAffected), 1)
      rowsAffected <- dbSendStatement(connection, statements[3])
      expect_equal(dbGetRowsAffected(rowsAffected), 0)
      dbClearResult(rowsAffected)
    }
  })
}
