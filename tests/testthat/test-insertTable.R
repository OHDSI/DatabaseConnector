library(testthat)

test_that("Insert temp table into Redshift with S3 credentials", {
  testData <- data.frame(a = c(1, 2, 3), b = c("a", "b", "c"), stringsAsFactors = FALSE) 
  tableName <- "#testData"
  connectionDetails <- createConnectionDetails(dbms = "redshift", 
                                               user = Sys.getenv("cdmUser"), 
                                               password = Sys.getenv("cdmPassword"), 
                                               server = Sys.getenv("cdmServer"),
                                               port = Sys.getenv("cdmServerPort"))
  connection <- connect(connectionDetails)
  
  insertTable(connection = connection, tableName = tableName, data = testData, 
              dropTableIfExists = T, createTable = T, tempTable = F)
  
  renderedSql <- SqlRender::renderSql(sql = "SELECT * FROM @tableName", tableName = tableName)$sql
  translatedSql <- SqlRender::translateSql(sql = renderedSql, targetDialect = "redshift")$sql
  result <- querySql(connection = connection, sql = translatedSql)
  expect_identical(names(testData), tolower(names(result)))
  expect_equal(nrow(result), 3)
  DBI::dbDisconnect(connection)
})