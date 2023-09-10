library(testthat)

test_that("Compute data hash", {
  # Postgresql -----------------------------------------------------------------
  con <- DBI::dbConnect(DatabaseConnectorDriver(),
    dbms = "postgresql",
    user = Sys.getenv("CDM5_POSTGRESQL_USER"),
    password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
    server = Sys.getenv("CDM5_POSTGRESQL_SERVER")
  )
  
  hash <- computeDataHash(con, Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"))
  expect_true(is.character(hash))

  DBI::dbDisconnect(con)

  # SQL Server -----------------------------------------------------------------
  con <- DBI::dbConnect(DatabaseConnectorDriver(),
    dbms = "sql server",
    user = Sys.getenv("CDM5_SQL_SERVER_USER"),
    password = URLdecode(Sys.getenv("CDM5_SQL_SERVER_PASSWORD")),
    server = Sys.getenv("CDM5_SQL_SERVER_SERVER")
  )
  
  hash <- computeDataHash(con, Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA"))
  expect_true(is.character(hash))
  
  DBI::dbDisconnect(con)
  
  # Oracle ---------------------------------------------------------------------
  con <- DBI::dbConnect(DatabaseConnectorDriver(),
    dbms = "oracle",
    user = Sys.getenv("CDM5_ORACLE_USER"),
    password = URLdecode(Sys.getenv("CDM5_ORACLE_PASSWORD")),
    server = Sys.getenv("CDM5_ORACLE_SERVER")
  )
  
  hash <- computeDataHash(con, Sys.getenv("CDM5_ORACLE_CDM_SCHEMA"))
  expect_true(is.character(hash))
  
  DBI::dbDisconnect(con)
  
  # RedShift  ------------------------------------------------------------------
  con <- DBI::dbConnect(DatabaseConnectorDriver(),
    dbms = "redshift",
    user = Sys.getenv("CDM5_REDSHIFT_USER"),
    password = URLdecode(Sys.getenv("CDM5_REDSHIFT_PASSWORD")),
    server = Sys.getenv("CDM5_REDSHIFT_SERVER")
  )
  
  hash <- computeDataHash(con, Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA"))
  expect_true(is.character(hash))
  
  DBI::dbDisconnect(con)
})
