library(testthat)

test_that("Open and close connection", {
  # Postgresql
  details <- createConnectionDetails(dbms = "postgresql",
                                     user = Sys.getenv("CDM5_POSTGRESQL_USER"),
                                     password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
                                     server = Sys.getenv("CDM5_POSTGRESQL_SERVER"),
                                     schema = Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"))
  connection <- connect(details)
  expect_true(inherits(connection, "JDBCConnection"))
  expect_true(DBI::dbDisconnect(connection))

  # SQL Server
  details <- createConnectionDetails(dbms = "sql server",
                                     user = Sys.getenv("CDM5_SQL_SERVER_USER"),
                                     password = URLdecode(Sys.getenv("CDM5_SQL_SERVER_PASSWORD")),
                                     server = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
                                     schema = Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA"))
  connection <- connect(details)
  expect_true(inherits(connection, "JDBCConnection"))
  expect_true(DBI::dbDisconnect(connection))

  # Oracle
  details <- createConnectionDetails(dbms = "oracle",
                                     user = Sys.getenv("CDM5_ORACLE_USER"),
                                     password = URLdecode(Sys.getenv("CDM5_ORACLE_PASSWORD")),
                                     server = Sys.getenv("CDM5_ORACLE_SERVER"),
                                     schema = Sys.getenv("CDM5_ORACLE_CDM_SCHEMA"))
  connection <- connect(details)
  expect_true(inherits(connection, "JDBCConnection"))
  expect_true(DBI::dbDisconnect(connection))
})
