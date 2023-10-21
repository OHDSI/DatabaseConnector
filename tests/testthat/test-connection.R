library(testthat)

for (testServer in testServers) {
  test_that(addDbmsToLabel("Open and close connection", testServer), {
    connection <- connect(testServer$connectionDetails)
    expect_true(inherits(connection, "DatabaseConnectorConnection"))
    expect_equal(dbms(connection), testServer$connectionDetails$dbms)
    expect_true(disconnect(connection))
  })
}

for (testServer in testServers) {
  if (!is.null(testServer$connectionDetails2)) {
    test_that(addDbmsToLabel("Open and close connection using secondary details", testServer), {
      connection <- connect(testServer$connectionDetails2)
      expect_true(inherits(connection, "DatabaseConnectorConnection"))
      expect_equal(dbms(connection), testServer$connectionDetails2$dbms)
      expect_true(disconnect(connection))
    })
  }
}

test_that("Error is thrown when using incorrect dbms argument", {
  expect_error(createConnectionDetails(dbms = "foobar"), "DBMS 'foobar' not supported")
  expect_error(connect(dbms = "foobar"), "DBMS 'foobar' not supported")
})

test_that("getAvailableJavaHeapSpace returns a positive number", {
  expect_gt(getAvailableJavaHeapSpace(), 0)
})

test_that("Error is thrown when forgetting password", {
  details <- createConnectionDetails(
    dbms = "postgresql",
    user = Sys.getenv("CDM5_POSTGRESQL_USER"),
    server = Sys.getenv("CDM5_POSTGRESQL_SERVER")
  )
  expect_error(connection <- connect(details), "Connection propery 'password' is NULL")
})

test_that("dbms function maps DBI connections to correct SQL dialect", {
  mappings <- c(
    'Microsoft SQL Server' = 'sql server',
    'PqConnection' = 'postgresql',
    'RedshiftConnection' = 'redshift',
    'BigQueryConnection' = 'bigquery',
    'SQLiteConnection' = 'sqlite',
    'duckdb_connection'  = 'duckdb')
  for(i in seq_along(mappings)) {
    driver <- names(mappings)[i]
    dialect <- unname(mappings)[i]
    mockConstructor <- setClass(driver, contains = "DBIConnection")
    mockConnection <- mockConstructor()
    expect_equal(dbms(mockConnection), dialect)
  }
})
