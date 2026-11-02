library(testthat)

for (testServer in testServers) {
  test_that(addDbmsToLabel("Get table names", testServer), {
    connection <- connect(testServer$connectionDetails)
    on.exit(disconnect(connection))
    tables <- getTableNames(connection, testServer$cdmDatabaseSchema)
    expect_true("person" %in% tables)
    expect_true(existsTable(connection, testServer$cdmDatabaseSchema, "person"))
    # This does not work on SQL Server or BigQuery:
    if (!testServer$connectionDetails$dbms %in% c("sql server", "bigquery")) {
      expect_true(DBI::dbExistsTable(connection, "person"))
    }
    
    # Quotes in schema identifier:
    if (testServer$connectionDetails$dbms == "spark") {
      tables <- getTableNames(connection, paste0("`", testServer$cdmDatabaseSchema, "`"))
      expect_true("person" %in% tables)
    }
    
  })
}

test_that("Get table names in schema using a DBI Postgres connection", {
  skip_if(Sys.getenv("CDM5_POSTGRESQL_SERVER") == "", "No Postgres test server")
  skip_if_not_installed("RPostgres")
  parts <- unlist(strsplit(Sys.getenv("CDM5_POSTGRESQL_SERVER"), "/"))
  connectionDetails <- createDbiConnectionDetails(
    dbms = "postgresql",
    drv = RPostgres::Postgres(),
    dbname = parts[2],
    host = parts[1],
    user = Sys.getenv("CDM5_POSTGRESQL_USER"),
    password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD"))
  )
  connection <- connect(connectionDetails)
  on.exit(disconnect(connection))
  cdmDatabaseSchema <- Sys.getenv("CDM5_POSTGRESQL_CDM54_SCHEMA")
  tables <- getTableNames(connection, cdmDatabaseSchema)
  expect_true("person" %in% tables)
  expect_true(existsTable(connection, cdmDatabaseSchema, "person"))
  expect_false(existsTable(connection, "non_existing_schema", "person"))
})
