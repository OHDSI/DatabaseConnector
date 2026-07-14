library(testthat)

test_that("errorReportFileLocation defaults to the working directory", {
  connection <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(connection), add = TRUE)

  oldOptions <- options(errorReportFileLocation = NULL)
  on.exit(options(oldOptions), add = TRUE)

  badSql <- "SELECT * FROM definitely_not_a_table;"
  reportFile <- file.path(getwd(), "errorReportSql.txt")
  unlink(reportFile)
  on.exit(unlink(reportFile), add = TRUE)

  expect_error(querySql(connection, badSql))
  expect_true(file.exists(reportFile))
})

test_that("errorReportFileLocation is created and used for reports", {
  connection <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(connection), add = TRUE)

  reportDir <- tempfile("error-report-dir-")
  oldOptions <- options(errorReportFileLocation = reportDir)
  on.exit(options(oldOptions), add = TRUE)

  badSql <- "SELECT * FROM definitely_not_a_table;"
  reportFile <- file.path(reportDir, "errorReportSql.txt")
  unlink(reportDir, recursive = TRUE, force = TRUE)
  on.exit(unlink(reportDir, recursive = TRUE, force = TRUE), add = TRUE)

  expect_error(executeSql(connection, badSql))
  expect_true(dir.exists(reportDir))
  expect_true(file.exists(reportFile))
})

test_that("errorReportFileLocation fails before database work when the directory cannot be created", {
  connection <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(connection), add = TRUE)

  reportPath <- tempfile("error-report-file-")
  writeLines("not a directory", reportPath)
  oldOptions <- options(errorReportFileLocation = reportPath)
  on.exit(options(oldOptions), add = TRUE)
  on.exit(unlink(reportPath, force = TRUE), add = TRUE)

  expect_error(querySql(connection, "SELECT * FROM definitely_not_a_table;"), "Could not create|not writable")
})

test_that("an explicit errorReportFile override still wins", {
  connection <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(connection), add = TRUE)

  optionDir <- tempfile("option-dir-")
  overrideDir <- tempfile("override-dir-")
  dir.create(overrideDir, recursive = TRUE, showWarnings = FALSE)
  oldOptions <- options(errorReportFileLocation = optionDir)
  on.exit(options(oldOptions), add = TRUE)
  on.exit(unlink(c(optionDir, overrideDir), recursive = TRUE, force = TRUE), add = TRUE)

  explicitFile <- file.path(overrideDir, "custom-report.txt")
  expect_error(querySql(connection, "SELECT * FROM definitely_not_a_table;", errorReportFile = explicitFile))
  expect_true(file.exists(explicitFile))
  expect_false(file.exists(file.path(optionDir, "errorReportSql.txt")))
})

test_that("querySqlToAndromeda and renderTranslateQuerySqlToAndromeda use the configured location", {
  connection <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(connection), add = TRUE)
  andromeda <- Andromeda::andromeda()
  on.exit(Andromeda::close(andromeda), add = TRUE)

  reportDir <- tempfile("andromeda-error-report-")
  oldOptions <- options(errorReportFileLocation = reportDir)
  on.exit(options(oldOptions), add = TRUE)
  on.exit(unlink(reportDir, recursive = TRUE, force = TRUE), add = TRUE)

  expect_error(
    querySqlToAndromeda(
      connection = connection,
      sql = "SELECT * FROM definitely_not_a_table;",
      andromeda = andromeda,
      andromedaTableName = "x"
    )
  )
  expect_error(
    renderTranslateQuerySqlToAndromeda(
      connection = connection,
      sql = "SELECT * FROM definitely_not_a_table;",
      andromeda = andromeda,
      andromedaTableName = "y"
    )
  )
  expect_true(file.exists(file.path(reportDir, "errorReportSql.txt")))
})
