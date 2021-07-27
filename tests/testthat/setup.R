# Download the JDBC drivers used in the tests

oldJarFolder <- Sys.getenv("DATABASECONNECTOR_JAR_FOLDER")
Sys.setenv("DATABASECONNECTOR_JAR_FOLDER" = tempfile("jdbcDrivers"))
downloadJdbcDrivers("postgresql")
downloadJdbcDrivers("sql server")
downloadJdbcDrivers("oracle")
downloadJdbcDrivers("redshift")
downloadJdbcDrivers("spark")

withr::defer({
  unlink(Sys.getenv("DATABASECONNECTOR_JAR_FOLDER"), recursive = TRUE, force = TRUE)
  Sys.setenv("DATABASECONNECTOR_JAR_FOLDER" = oldJarFolder)
}, testthat::teardown_env())
