library(testthat)

test_that("Fetch results", {
  connection <- connect(dbms = "postgresql",
                        user = Sys.getenv("CDM5_POSTGRESQL_USER"),
                        password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
                        server = Sys.getenv("CDM5_POSTGRESQL_SERVER"),
                        schema = Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"))
  # Fetch data.frame:
  count <- querySql(connection, "SELECT COUNT(*) FROM vocabulary")
  expect_equal(count[1, 1], 58)
  
  # Fetch ffdf:
  count <- querySql.ffdf(connection, "SELECT COUNT(*) FROM vocabulary")
  expect_equal(count[1, 1], 58)
  
  disconnect(connection)
  
  connection <- connect(dbms = "sql server",
                        user = Sys.getenv("CDM5_SQL_SERVER_USER"),
                        password = URLdecode(Sys.getenv("CDM5_SQL_SERVER_PASSWORD")),
                        server = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
                        schema = Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA"))
  # Fetch data.frame:
  count <- querySql(connection, "SELECT COUNT(*) FROM vocabulary")
  expect_equal(count[1, 1], 71)
  
  # Fetch ffdf:
  count <- querySql.ffdf(connection, "SELECT COUNT(*) FROM vocabulary")
  expect_equal(count[1, 1], 71)
  
  disconnect(connection)
  
  connection <- connect(dbms = "oracle",
                        user = Sys.getenv("CDM5_ORACLE_USER"),
                        password = URLdecode(Sys.getenv("CDM5_ORACLE_PASSWORD")),
                        server = Sys.getenv("CDM5_ORACLE_SERVER"),
                        schema = Sys.getenv("CDM5_ORACLE_CDM_SCHEMA"))
  # Fetch data.frame:
  count <- querySql(connection, "SELECT COUNT(*) FROM vocabulary")
  expect_equal(count[1, 1], 71)

  # Fetch ffdf:
  count <- querySql.ffdf(connection, "SELECT COUNT(*) FROM vocabulary")
  expect_equal(count[1, 1], 71)

  disconnect(connection)
})
