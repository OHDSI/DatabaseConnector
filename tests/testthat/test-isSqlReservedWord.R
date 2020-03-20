library(testthat)

test_that("isSqlReservedWord works", {
  expect_true(all(isSqlReservedWord(c("TRUE", "CASE", "IF", "#if", "#IF"))))
  expect_false(any(isSqlReservedWord(c("Tbl1", "#tempTable"))))
  expect_warning(isSqlReservedWord("TRUE", warn = T), "reserved")
})
