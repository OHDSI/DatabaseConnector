# Run only DBI tests with testthat::test_file("tests/testthat/test-DBItest.R")

port <- Sys.getenv("CDM5_POSTGRESQL_PORT")
if (port == "") port <- "5432"

cdlist <- list(
  dbms = "postgresql",
  server = Sys.getenv("CDM5_POSTGRESQL_SERVER"),
  user = Sys.getenv("CDM5_POSTGRESQL_USER"),
  password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD"),
  port = port
)


default_skip <- c("package_name")

tweaks <- DBItest::tweaks(
  constructor_name = "DatabaseConnectorDriver",
  omit_blob_tests = TRUE
)

DBItest::make_context(
  new(
    "DBIConnector",
    .drv = DatabaseConnector::DatabaseConnectorDriver(),
    .conn_args = cdlist
  ),
  tweaks = tweaks,
  default_skip = default_skip
)


DBItest::test_getting_started(skip = c(
  "package_name" # DatabaseConnector package does not start with 'R'
))


DBItest::test_driver(skip = c(
  "connect_bigint.*" # Possibly need to fix bigint tests
))
