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
