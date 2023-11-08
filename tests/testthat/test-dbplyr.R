library(testthat)
library(DatabaseConnector)
source("dbplyrTestFunction.R")

for (testServer in testServers) {
  test_that(addDbmsToLabel("Test dbplyr", testServer), {
    options(sqlRenderTempEmulationSchema = testServer$tempEmulationSchema)
    testDbplyrFunctions(connectionDetails = testServer$connectionDetails,
                        cdmDatabaseSchema = testServer$cdmDatabaseSchema)
  })
}

test_that("Test dbplyr date functions on non-dbplyr data", {
  date <- c(as.Date("2000-02-01"), as.Date("2000-12-31"), as.Date("2000-01-31"))
  
  date2 <- c(as.Date("2000-02-05"), as.Date("2000-11-30"), as.Date("2002-01-31"))
  
  expect_equal(dateDiff("day", date, date2), c(4, -31, 731))
  
  expect_equal(eoMonth(date), c(as.Date("2000-02-29"), as.Date("2000-12-31"), as.Date("2000-01-31")))
  
  expect_equal(dateAdd("day", 1, date), c(as.Date("2000-02-02"), as.Date("2001-01-01"), as.Date("2000-02-01")))
  
  expect_equal(year(date), c(2000, 2000, 2000))
  
  expect_equal(month(date), c(2, 12, 1))
  
  expect_equal(day(date), c(1, 31, 31))
})
