library(testthat)

test_that("Open connection", {
  # Postgresql
  details <-
    createConnectionDetails(dbms = "postgresql",
                            user = Sys.getenv("CDM5_POSTGRESQL_USER"),
                            password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD"),
                            server = Sys.getenv("CDM5_POSTGRESQL_SERVER"),
                            schema = Sys.getenv("CDM5_POSTGRESQL_SCHEMA"))
#   connection <- tryCatch(connect(details),
#            error = function(e) {
#              print(traceback())
#              })
  connection <- connect(details)
  expect_true(inherits(connection, "JDBCConnection"))
#  expect_true(DBI::dbDisconnect(connection))
})
