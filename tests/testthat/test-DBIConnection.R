test_that("querySql and executeSql work with a duckdb DBIConnection", {
  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  DBI::dbWriteTable(conn, "cars", cars)
  df <- querySql(conn, "select * from cars")
  expect_equal(df, cars)
  
  expect_output(executeSql(conn, "
    CREATE TABLE test (column1 integer, column2 varchar);
    INSERT INTO test VALUES (1, 'a');
    INSERT INTO test VALUES (2, 'b');
  "))
  
  df <- querySql(conn, "select * from test")
  expect_s3_class(df, "data.frame")
  expect_equal(nrow(df), 2)
  
  DBI::dbDisconnect(conn)
})

test_that("querySql and executeSql work with a sqlite DBIConnection", {
  conn <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  DBI::dbWriteTable(conn, "cars", cars)
  df <- querySql(conn, "select * from cars")
  expect_equal(df, cars)
  
  expect_output(executeSql(conn, "
    CREATE TABLE test (column1 integer, column2 varchar);
    INSERT INTO test VALUES (1, 'a');
    INSERT INTO test VALUES (2, 'b');
  "))
  
  df <- querySql(conn, "select * from test")
  expect_s3_class(df, "data.frame")
  expect_equal(nrow(df), 2)
  
  DBI::dbDisconnect(conn)
})


