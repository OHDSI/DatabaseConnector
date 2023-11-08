library(testthat)

for (testServer in testServers) {
  test_that(addDbmsToLabel("Compute data hash", testServer), {
    connection <- connect(testServer$connectionDetails)
    on.exit(disconnect(connection))
    hash <- computeDataHash(connection, testServer$cdmDatabaseSchema)
    expect_true(is.character(hash))
  })
}
