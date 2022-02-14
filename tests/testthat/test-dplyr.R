test_that("dplyr tbl reference works", {
  
  con <- connect(dbms = "sqlite", server = tempfile())
  dbWriteTable(con, "cars", cars)
  
  cars2 <- dplyr::tbl(con, "cars") %>% 
    dplyr::collect() %>% 
    as.data.frame()
  
  expect_equal(cars, cars2)
  disconnect(con)
})
