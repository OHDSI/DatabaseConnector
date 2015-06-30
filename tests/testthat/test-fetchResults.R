library(testthat)

test_that("Fetch results", {
  connection <- connect(dbms = "postgresql",
                        user = Sys.getenv("CDM5_POSTGRESQL_USER"),
                        password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
                        server = Sys.getenv("CDM5_POSTGRESQL_SERVER"),
                        schema = Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"))
  # Fetch data.frame:
  count <- querySql(connection, "SELECT COUNT(*) FROM vocabulary")
  expect_equal(count[1, 1], 63)

  # Fetch ffdf:
  count <- querySql.ffdf(connection, "SELECT COUNT(*) FROM vocabulary")
  expect_equal(count[1, 1], 63)

  DBI::dbDisconnect(connection)
})
