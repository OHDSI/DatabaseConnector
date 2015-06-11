library(testthat)

test_that("Environmental variables", {
  expect_equal(Sys.getenv("CDM5_POSTGRESQL_USER"), "ohdsi")
  expect_equal(Sys.getenv("CDM5_POSTGRESQL_SERVER"), "jenkins.ohdsi.org/CDMV5")
  expect_equal(Sys.getenv("CDM5_POSTGRESQL_SCHEMA"), "public")
  pw <- Sys.getenv("CDM5_POSTGRESQL_PASSWORD")
  checkSum <- sum(as.numeric(charToRaw(pw)))
  expect_equal(checkSum, 1149)
  
})

test_that("Open connection", {
  # Postgresql
  details <-
    createConnectionDetails(dbms = "postgresql",
                            user = Sys.getenv("CDM5_POSTGRESQL_USER"),
                            password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD"),
                            server = Sys.getenv("CDM5_POSTGRESQL_SERVER"),
                            schema = Sys.getenv("CDM5_POSTGRESQL_SCHEMA"))
  connection <- connect(details)
  expect_true(inherits(connection, "JDBCConnection"))
#  expect_true(DBI::dbDisconnect(connection))
})
