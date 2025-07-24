library(testthat)

fun <- function(data, position, myString) {
  data$test <- myString
  return(data)
}
args <- list(myString = "MY STRING")

for (testServer in testServers) {
  test_that(addDbmsToLabel("Open and close connection", testServer), {
    connection <- connect(testServer$connectionDetails)
    on.exit(disconnect(connection))
    sql <- "SELECT TOP 10 * FROM @cdm_database_schema.vocabulary;"
    data <- renderTranslateQueryApplyBatched(
      connection,
      sql,
      fun,
      args,
      cdm_database_schema = testServer$cdmDatabaseSchema
    )
    data <- do.call(rbind, data)
    expect_true("test" %in% colnames(data))
    expect_true(all(data$test == "MY STRING"))
  })
}
