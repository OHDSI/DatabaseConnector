library(testthat)
library(dplyr)

test_that("Extra functions work on regular DBI connection", {
  connection <- DBI::dbConnect(
    drv = duckdb::duckdb(),
    dbdir = ":memory:"
  )
  on.exit(DBI::dbDisconnect(connection))
  insertTable(
    connection = connection,
    tableName = "cars",
    data = cars
  )

  cars2 <- querySql(connection, "SELECT * FROM cars;")  
  expect_equal(cars, cars2)
  
  andromeda <- Andromeda::andromeda()
  on.exit(Andromeda::close(andromeda), add = TRUE)
  querySqlToAndromeda(connection, "SELECT * FROM cars;", andromeda, "cars")
  cars3 <- andromeda$cars |>
    collect() |>
    as.data.frame()
  expect_equal(cars, cars3)
  
  hash <- computeDataHash(connection, "main")
  expect_true(is.character(hash))
  
  tables <- getTableNames(connection)
  expect_equal(tables, "cars")
  
  executeSql(connection, "DROP TABLE cars;")
  
  tables <- getTableNames(connection)
  expect_equal(length(tables), 0)
  
  expect_equal(dbms(connection), "duckdb")
})
