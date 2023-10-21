library(testthat)

test_that("Open and close duckdb connection", {
  databaseFile <- tempfile()
  connection <- DatabaseConnector::connect(dbms = "duckdb", server = databaseFile)
  expect_s4_class(connection, "DatabaseConnectorDbiConnection")
  
  disconnect(connection)
  unlink(databaseFile)
})

test_that("Open and close duckdb without DATABASECONNECTOR_JAR_FOLDER", {
  withr::with_envvar(
    new=c("DATABASECONNECTOR_JAR_FOLDER"=""),
    {
    databaseFile <- tempfile()
    connection <- DatabaseConnector::connect(dbms = "duckdb", server = databaseFile)
    expect_s4_class(connection, "DatabaseConnectorDbiConnection")
    disconnect(connection)
    unlink(databaseFile)
    }
    
  )
})

test_that("Insert and retrieve dates from duckdb", {
  databaseFile <- tempfile()
  connection <- DatabaseConnector::connect(dbms = "duckdb", server = databaseFile)
  data <- data.frame(startDate = as.Date(c("2000-01-01", "2000-02-01")))
  insertTable(
    connection = connection,
    databaseSchema = "main",
    tableName = "test",
    data = data,
    createTable = TRUE,
    camelCaseToSnakeCase = TRUE
  )
  data2 <- renderTranslateQuerySql(
    connection = connection, 
    sql = "SELECT * FROM main.test;",
    snakeCaseToCamelCase = TRUE)
  expect_equal(data, data2)
  disconnect(connection)
  unlink(databaseFile)
})

test_that("Insert using tibbles and retrieve dates from duckdb", {
  databaseFile <- tempfile()
  connection <- DatabaseConnector::connect(dbms = "duckdb", server = databaseFile)
  
  data <- dplyr::tibble(startDate = as.Date(c("2000-01-01", "2000-02-01")))
  insertTable(
    connection = connection,
    databaseSchema = "main",
    tableName = "test",
    data = data,
    createTable = TRUE,
    camelCaseToSnakeCase = TRUE
  )
  data2 <- renderTranslateQuerySql(
    connection = connection, 
    sql = "SELECT * FROM main.test;",
    snakeCaseToCamelCase = TRUE)
  expect_equal(data, dplyr::as_tibble(data2))
  disconnect(connection)
  unlink(databaseFile)
})
