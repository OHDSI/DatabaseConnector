test_that("dplyr tbl reference works", {
  
  dbFile <- tempfile()
  con <- connect(dbms = "sqlite",
                 server = dbFile)
  
  dbWriteTable(con, "cars", cars)
  cars_db <- dplyr::tbl(con, "cars")
  cars_local <- dplyr::collect(cars)
  expect_equal(cars, cars_local)
  
  disconnect(con)
})
