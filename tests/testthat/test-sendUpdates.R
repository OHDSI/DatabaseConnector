library(testthat)

sql <- "IF OBJECT_ID('tempdb..#temp', 'U') IS NOT NULL DROP TABLE #temp;
    CREATE TABLE #temp (x INT);
    INSERT INTO #temp (x) SELECT 123;
    DELETE FROM #temp WHERE x = 123;
    DROP TABLE #temp;"

for (testServer in testServers) {
  test_that(addDbmsToLabel("Send updates", testServer), {
    connection <- connect(testServer$connectionDetails)
    options(sqlRenderTempEmulationSchema = testServer$tempEmulationSchema)
    on.exit(dropEmulatedTempTables(connection))
    on.exit(disconnect(connection), add = TRUE)
    expect_equal(renderTranslateExecuteSql(connection, sql), c(0, 0, 1, 1, 0))
    expect_equal(renderTranslateExecuteSql(connection, sql, runAsBatch = TRUE), c(0, 0, 1, 1, 0))
    rowsAffected <- dbSendStatement(connection, sql)
    expect_equal(dbGetRowsAffected(rowsAffected), 2)
    dbClearResult(rowsAffected)
  })
}
