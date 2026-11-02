library(testthat)

for (testServer in testServers) {
  test_that(addDbmsToLabel("Error report contains Java and driver versions", testServer), {
    connection <- connect(testServer$connectionDetails)
    on.exit(disconnect(connection))
    
    errorReportFile <- tempfile(fileext = ".txt")
    on.exit(unlink(errorReportFile), add = TRUE)
    expect_error(
      querySql(connection, "SELECT * FROM definitely_not_a_table;", errorReportFile = errorReportFile),
      "Error executing SQL"
    )
    report <- readChar(errorReportFile, file.info(errorReportFile)$size)
    javaVersion <- rJava::.jcall("java/lang/System", "S", "getProperty", "java.version")
    expect_true(grepl(paste0("Java version:\n", javaVersion, "\n"), report, fixed = TRUE))
    if (inherits(connection, "DatabaseConnectorJdbcConnection")) {
      metaData <- rJava::.jcall(connection@jConnection, "Ljava/sql/DatabaseMetaData;", "getMetaData")
      driverVersion <- rJava::.jcall(metaData, "S", "getDriverVersion")
      expect_true(grepl("JDBC driver:\n", report, fixed = TRUE))
      expect_true(grepl(driverVersion, report, fixed = TRUE))
    } else {
      pkg <- attr(class(connection@dbiConnection), "package")
      expect_true(grepl(sprintf("DBI driver:\n%s (%s)\n", pkg, utils::packageVersion(pkg)), report, fixed = TRUE))
    }
  })
}

test_that("Error report contains DBI driver version for plain DBI connections", {
  connection <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(connection, shutdown = TRUE))
  
  errorReportFile <- tempfile(fileext = ".txt")
  on.exit(unlink(errorReportFile), add = TRUE)
  expect_error(
    executeSql(connection, "SELECT * FROM definitely_not_a_table;", errorReportFile = errorReportFile),
    "Error executing SQL"
  )
  report <- readChar(errorReportFile, file.info(errorReportFile)$size)
  expect_true(grepl("Java version:\n", report, fixed = TRUE))
  expect_true(grepl(sprintf("DBI driver:\nduckdb (%s)\n", utils::packageVersion("duckdb")), report, fixed = TRUE))
})
