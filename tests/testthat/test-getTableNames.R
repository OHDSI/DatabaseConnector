library(testthat)

for (testServer in testServers) {
  test_that(addDbmsToLabel("Get table names", testServer), {
    connection <- connect(testServer$connectionDetails)
    on.exit(disconnect(connection))
    tables <- getTableNames(connection, testServer$cdmDatabaseSchema)
    expect_true("person" %in% tables)
    expect_true(existsTable(connection, testServer$cdmDatabaseSchema, "person"))
    # This does not work on SQL Server:
    if (testServer$connectionDetails$dbms != "sql server") {
      expect_true(DBI::dbExistsTable(connection, "person"))
    }
  })
}
