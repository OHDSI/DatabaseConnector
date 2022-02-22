library(testthat)
library(dplyr)

# SQLite -------------------------------------------------
test_that("dplyr tbl reference works", {
  
  con <- connect(dbms = "sqlite", server = tempfile())
  dbWriteTable(con, "cars", cars)
  
  cars2 <- dplyr::tbl(con, "cars") %>% 
    dplyr::collect() %>% 
    as.data.frame()
  
  expect_equal(cars, cars2)
  disconnect(con)
})


details <- list()

# Postgresql --------------------------------------------------
details$postgresql <- createConnectionDetails(
  dbms = "postgresql",
  user = Sys.getenv("CDM5_POSTGRESQL_USER"),
  password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
  server = Sys.getenv("CDM5_POSTGRESQL_SERVER")
)

# SQL Server --------------------------------------------------
details$sql_server <- createConnectionDetails(
  dbms = "sql server",
  user = Sys.getenv("CDM5_SQL_SERVER_USER"),
  password = URLdecode(Sys.getenv("CDM5_SQL_SERVER_PASSWORD")),
  server = Sys.getenv("CDM5_SQL_SERVER_SERVER")
)

# Oracle --------------------------------------------------
details$oracle <- createConnectionDetails(
  dbms = "oracle",
  user = Sys.getenv("CDM5_ORACLE_USER"),
  password = URLdecode(Sys.getenv("CDM5_ORACLE_PASSWORD")),
  server = Sys.getenv("CDM5_ORACLE_SERVER")
)

# RedShift  --------------------------------------------------
details$redshift <- createConnectionDetails(
  dbms = "redshift",
  user = Sys.getenv("CDM5_REDSHIFT_USER"),
  password = URLdecode(Sys.getenv("CDM5_REDSHIFT_PASSWORD")),
  server = Sys.getenv("CDM5_REDSHIFT_SERVER")
)

# databases using lower case names
test_that("dplyr verbs work on redshift", {
  con <- connect(details[["redshift"]])
  person <- tbl(con, dbplyr::in_schema(Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA"), "person"))
  
  df <- person %>% 
    group_by(year_of_birth) %>% 
    summarise(n = n()) %>% 
    arrange(desc(n)) %>% 
    head(5) %>% 
    collect()
  
  expect_s3_class(df, "data.frame")
  expect_equal(nrow(df), 5)

  disconnect(con)
})

test_that("dplyr verbs work on postgresql", {
  con <- connect(details[["postgresql"]])
  person <- tbl(con, dbplyr::in_schema(Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"), "person"))
  
  df <- person %>% 
    group_by(year_of_birth) %>% 
    summarise(n = n()) %>% 
    arrange(desc(n)) %>% 
    head(5) %>% 
    collect()
  
  expect_s3_class(df, "data.frame")
  expect_equal(nrow(df), 5)

  disconnect(con)
})

# databases using uppercase names
test_that("dplyr verbs work with oracle", {
  con <- connect(details[["oracle"]])
  person <- tbl(con, dbplyr::in_schema(Sys.getenv("CDM5_ORACLE_CDM_SCHEMA"), "PERSON"))
  
  df <- person %>% 
    group_by(YEAR_OF_BIRTH) %>% 
    summarise(n = n()) %>% 
    arrange(desc(n)) %>% 
    head(5) %>% 
    collect()
  
  expect_s3_class(df, "data.frame")
  expect_equal(nrow(df), 5)

  disconnect(con)
  
})
