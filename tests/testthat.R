library(testthat)
library(DatabaseConnector)
if (!is.null(Sys.getenv("CDM5_POSTGRESQL_SERVER"))) {
  test_check("DatabaseConnector")
} else {
  writeLines("Skipping testing because environmental variables not set") 
}
