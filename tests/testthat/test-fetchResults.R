library(testthat)

if (DatabaseConnector:::is_installed("ParallelLogger")) {
  options(LOG_DATABASECONNECTOR_SQL = TRUE)
  logFileName <- tempfile(fileext = ".txt")
  ParallelLogger::addDefaultFileLogger(logFileName, name = "TEST_LOGGER")
}

for (testServer in testServers) {
  test_that(addDbmsToLabel("Fetch results", testServer), {
   
    connection <- connect(testServer$connectionDetails)
    on.exit(disconnect(connection))
    sql <- "SELECT COUNT(*) AS row_count FROM @cdm_database_schema.vocabulary"
    renderedSql <- SqlRender::render(sql = sql, 
                                     cdm_database_schema = testServer$cdmDatabaseSchema)
    
    # Fetch data.frame:
    count <- querySql(connection, renderedSql)
    expect_gt(count[1, 1], 1)
    count <- renderTranslateQuerySql(connection = connection, 
                                     sql = sql, 
                                     cdm_database_schema = testServer$cdmDatabaseSchema)
    expect_gt(count[1, 1], 1)
    
    # Fetch Andromeda:
    andromeda <- Andromeda::andromeda()
    querySqlToAndromeda(connection = connection, 
                        sql = renderedSql, 
                        andromeda = andromeda, 
                        andromedaTableName = "test", 
                        snakeCaseToCamelCase = TRUE)
    expect_gt(dplyr::collect(andromeda$test)$rowCount[1], 1)
    renderTranslateQuerySqlToAndromeda(connection,
                                       sql,
                                       cdm_database_schema = testServer$cdmDatabaseSchema,
                                       andromeda = andromeda,
                                       andromedaTableName = "test2",
                                       snakeCaseToCamelCase = TRUE
    )
    expect_gt(dplyr::collect(andromeda$test2)$rowCount[1], 1)
    Andromeda::close(andromeda)
    
    skip_if_not(testServer$connectionDetails$dbms %in% c("sqlite", "duckdb"))
    # dbFetch only fetches n rows
    sql <- "SELECT * FROM @cdm_database_schema.vocabulary;"
    sql <- SqlRender::render(sql,
                             cdm_database_schema = testServer$cdmDatabaseSchema)
    resultSet <- dbSendQuery(connection, sql)
    data <- dbFetch(resultSet, n = 1)
    expect_false(dbHasCompleted(resultSet))
    expect_equal(nrow(data), 1)
    dbClearResult(resultSet)
  })
}    

test_that("Logging query times", {
  skip_if_not_installed("ParallelLogger")
  
  queryTimes <- extractQueryTimes(logFileName)
  expect_gt(nrow(queryTimes), 3)
  ParallelLogger::unregisterLogger("TEST_LOGGER")
  unlink(logFileName)
})
