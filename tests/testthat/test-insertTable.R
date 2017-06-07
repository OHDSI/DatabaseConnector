library(testthat)

test_that("Insert temp table into Redshift with S3 credentials", {
  testData <- data.frame(a = c(1, 2, 3), b = c("a", "b", "c"), stringsAsFactors = FALSE) 
  tableName <- "#testData"

  Sys.setenv("AWS_ACCESS_KEY_ID" = URLdecode(Sys.getenv("CDM5_AWS_ACCESS_KEY_ID")),
             "AWS_SECRET_ACCESS_KEY" = URLdecode(Sys.getenv("CDM5_AWS_SECRET_ACCESS_KEY")),
             "AWS_DEFAULT_REGION" = Sys.getenv("CDM5_AWS_ACCESS_KEY_ID"),
             "AWS_BUCKET_NAME" = Sys.getenv("CDM5_AWS_BUCKET_NAME"),
             "AWS_OBJECT_KEY" = Sys.getenv("CDM5_AWS_OBJECT_KEY"),
             "AWS_SSE_TYPE" = Sys.getenv("CDM5_AWS_SSE_TYPE"))
  
  connectionDetails <- createConnectionDetails(dbms = "redshift", 
                                               user = Sys.getenv("CDM5_REDSHIFT_USER"),
                                               password = URLdecode(Sys.getenv("CDM5_REDSHIFT_PASSWORD")),
                                               server = Sys.getenv("CDM5_REDSHIFT_SERVER"))
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