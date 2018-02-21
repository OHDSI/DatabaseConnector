library(testthat)

test_that("PDW bulk load prerequisites not met", {
  if (Sys.getenv("CDM5_PDW_SERVER") != "") {
    Sys.setenv("DWLOADER_PATH" = "")
    connectionDetails <- createConnectionDetails(dbms = "redshift",
                                                 user = Sys.getenv("CDM5_PDW_USER"),
                                                 password = URLdecode(Sys.getenv("CDM5_PDW_PASSWORD")),
                                                 server = Sys.getenv("CDM5_PDW_SERVER"),
                                                 schema = Sys.getenv("CDM5_PDW_CDM_SCHEMA"))
    connection <- connect(connectionDetails)
    data <- data.frame(x = c(1, 2, 3), y = c("a", "b", "c"))
    expect_error(object = insertTable(connection = connection, tableName = "scratch.dbo.test", data = data, 
                                      dropTableIfExists = T, createTable = T, tempTable = F, useMppBulkLoad = T))
  }
})


test_that("Redshift bulk load prerequisites not met", {
  if (Sys.getenv("CDM5_PDW_SERVER") != "") {
    
    Sys.setenv("AWS_ACCESS_KEY_ID" = "",
               "AWS_SECRET_ACCESS_KEY" = "",
               "AWS_DEFAULT_REGION" = "",
               "AWS_BUCKET_NAME" = "",
               "AWS_OBJECT_KEY" = "",
               "AWS_SSE_TYPE" = "")
    connectionDetails <- createConnectionDetails(dbms = "redshift",
                                                 user = Sys.getenv("CDM5_REDSHIFT_USER"),
                                                 password = URLdecode(Sys.getenv("CDM5_REDSHIFT_PASSWORD")),
                                                 server = Sys.getenv("CDM5_REDSHIFT_SERVER"),
                                                 schema = Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA"))
    connection <- connect(connectionDetails)
    data <- data.frame(x = c(1, 2, 3), y = c("a", "b", "c"))
    expect_error(object = insertTable(connection = connection, tableName = "scratch.test", data = data, 
                                      dropTableIfExists = T, createTable = T, tempTable = F, useMppBulkLoad = T))
  }
})
