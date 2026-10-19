library(testthat)

test_that("Spark JDBC connection string keeps Databricks driver properties", {
  connectionString <- paste0(
    "jdbc:databricks://my-org.cloud.databricks.com:443/default;",
    "ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/abcde12345;"
  )

  prepared <- DatabaseConnector:::prepareSparkJdbcConnectionString(connectionString)

  expect_identical(prepared, connectionString)
  expect_false(grepl("UseNativeQuery", prepared, fixed = TRUE))
  expect_true(grepl("AuthMech=3", prepared, fixed = TRUE))
  expect_true(grepl("ssl=1", prepared, fixed = TRUE))
  expect_true(grepl("httpPath=/sql/1.0/warehouses/abcde12345", prepared, fixed = TRUE))
})

test_that("A legacy transportMode property is left for the driver to ignore", {
  connectionString <- paste0(
    "jdbc:databricks://my-org.cloud.databricks.com:443/default;",
    "transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/abcde12345"
  )

  expect_identical(
    DatabaseConnector:::prepareSparkJdbcConnectionString(connectionString),
    connectionString
  )
})

test_that("Spark JDBC connection requires a connection string", {
  expect_error(
    DatabaseConnector:::prepareSparkJdbcConnectionString(NULL),
    "Connection string required"
  )
  expect_error(
    DatabaseConnector:::prepareSparkJdbcConnectionString(""),
    "Connection string required"
  )
})
