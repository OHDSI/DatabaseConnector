library(testthat)
library(DatabaseConnector)
if (Sys.getenv("CDM5_POSTGRESQL_SERVER") != "") {
  test_check("DatabaseConnector")
} else {
  message("Skipping testing because environmental variables not set")
}
