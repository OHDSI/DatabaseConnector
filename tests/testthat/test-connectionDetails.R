library(testthat)

test_that("Open and close connection", {
  user <- "joe"

  expect_warning(
    connectionDetails <- createConnectionDetails(
      dbms = "postgresql",
      user = user
    )
  )
  expect_no_warning(
    connectionDetails <- createConnectionDetails(
      dbms = "postgresql",
      user =  Sys.getenv("CDM5_POSTGRESQL_USER")
    )
  )
})
  