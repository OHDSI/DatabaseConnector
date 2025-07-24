library(testthat)

test_that("Open and close connection", {
  connectionDetails <- createConnectionDetails(
    dbms = "postgresql",
    user = aNonExistingvariable
  )
  expect_error(
    connect(connectionDetails),
    regexp = "Unable to evaluate the 'user' argument"
  )
})
