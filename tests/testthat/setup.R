library(DatabaseConnector)

# Download the JDBC drivers used in the tests ----------------------------------
if (Sys.getenv("DONT_DOWNLOAD_JDBC_DRIVERS", "") != "TRUE") {
  oldJarFolder <- Sys.getenv("DATABASECONNECTOR_JAR_FOLDER")
  Sys.setenv("DATABASECONNECTOR_JAR_FOLDER" = tempfile("jdbcDrivers"))
  dir.create(Sys.getenv("DATABASECONNECTOR_JAR_FOLDER"))
  downloadJdbcDrivers("postgresql")
  downloadJdbcDrivers("sql server")
  downloadJdbcDrivers("oracle")
  downloadJdbcDrivers("redshift")
  downloadJdbcDrivers("spark")
  downloadJdbcDrivers("snowflake")
  if (.Platform$OS.type == "windows") {
    downloadJdbcDrivers("bigquery")
  }
  
  if (testthat::is_testing()) {
    withr::defer({
      unlink(Sys.getenv("DATABASECONNECTOR_JAR_FOLDER"), recursive = TRUE, force = TRUE)
      Sys.setenv("DATABASECONNECTOR_JAR_FOLDER" = oldJarFolder)
    },
    testthat::teardown_env()
    )
  }
}

# Helper functions -------------------------------------------------------------
addDbmsToLabel <- function(label, testServer) {
  # Test sections are not shown in R check, so also printing them here:
  writeLines(sprintf("Test: %s (%s)", label, testServer$connectionDetails$dbms))
  return(sprintf("%s (%s)", label, testServer$connectionDetails$dbms))
}

# Create a list with testing server details ------------------------------
testServers <- list()

# Postgres
parts <- unlist(strsplit(Sys.getenv("CDM5_POSTGRESQL_SERVER"), "/"))
host <- parts[1]
database <- parts[2]
port <- "5432"
connectionString <- paste0("jdbc:postgresql://", host, ":", port, "/", database)
testServers[[length(testServers) + 1]] <- list(
  connectionDetails = createConnectionDetails(
    dbms = "postgresql",
    user = Sys.getenv("CDM5_POSTGRESQL_USER"),
    password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
    server = Sys.getenv("CDM5_POSTGRESQL_SERVER")
  ),
  connectionDetails2 = details <- createConnectionDetails(
    dbms = "postgresql",
    connectionString = !!connectionString,
    user = Sys.getenv("CDM5_POSTGRESQL_USER"),
    password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD"))
  ),
  cdmDatabaseSchema = Sys.getenv("CDM5_POSTGRESQL_CDM54_SCHEMA"),
  tempEmulationSchema = NULL
)

# SQL Server
connectionString <- paste0("jdbc:sqlserver://", Sys.getenv("CDM5_SQL_SERVER_SERVER"))
testServers[[length(testServers) + 1]] <- list(
  connectionDetails = details <- createConnectionDetails(
    dbms = "sql server",
    user = Sys.getenv("CDM5_SQL_SERVER_USER"),
    password = URLdecode(Sys.getenv("CDM5_SQL_SERVER_PASSWORD")),
    server = Sys.getenv("CDM5_SQL_SERVER_SERVER")
  ),
  connectionDetails2 = details <- createConnectionDetails(
    dbms = "sql server",
    connectionString = !!connectionString,
    user = Sys.getenv("CDM5_SQL_SERVER_USER"),
    password = URLdecode(Sys.getenv("CDM5_SQL_SERVER_PASSWORD"))
  ),
  cdmDatabaseSchema = Sys.getenv("CDM5_SQL_SERVER_CDM54_SCHEMA"),
  tempEmulationSchema = NULL
)

# Oracle
port <- "1521"
parts <- unlist(strsplit(Sys.getenv("CDM5_ORACLE_SERVER"), "/"))
host <- parts[1]
sid <- parts[2]
connectionString <- paste0("jdbc:oracle:thin:@", host, ":", port, ":", sid)
testServers[[length(testServers) + 1]] <- list(
  connectionDetails = details <- createConnectionDetails(
    dbms = "oracle",
    user = Sys.getenv("CDM5_ORACLE_USER"),
    password = URLdecode(Sys.getenv("CDM5_ORACLE_PASSWORD")),
    server = Sys.getenv("CDM5_ORACLE_SERVER")
  ),
  connectionDetails2 = details <- createConnectionDetails(
    dbms = "oracle",
    connectionString = !!connectionString,
    user = Sys.getenv("CDM5_ORACLE_USER"),
    password = URLdecode(Sys.getenv("CDM5_ORACLE_PASSWORD"))
  ),
  cdmDatabaseSchema = Sys.getenv("CDM5_ORACLE_CDM54_SCHEMA"),
  tempEmulationSchema = Sys.getenv("CDM5_ORACLE_OHDSI_SCHEMA")
)

# RedShift
parts <- unlist(strsplit(Sys.getenv("CDM5_REDSHIFT_SERVER"), "/"))
host <- parts[1]
database <- parts[2]
port <- "5439"
connectionString <- paste0("jdbc:redshift://", host, ":", port, "/", database)
testServers[[length(testServers) + 1]] <- list(
  connectionDetails = details <- createConnectionDetails(
    dbms = "redshift",
    user = Sys.getenv("CDM5_REDSHIFT_USER"),
    password = URLdecode(Sys.getenv("CDM5_REDSHIFT_PASSWORD")),
    server = Sys.getenv("CDM5_REDSHIFT_SERVER")
  ),
  connectionDetails2 = details <- createConnectionDetails(
    dbms = "redshift",
    connectionString = connectionString,
    user = Sys.getenv("CDM5_REDSHIFT_USER"),
    password = URLdecode(Sys.getenv("CDM5_REDSHIFT_PASSWORD"))
  ),
  cdmDatabaseSchema = Sys.getenv("CDM5_REDSHIFT_CDM54_SCHEMA"),
  tempEmulationSchema = NULL
)

# Snowflake
testServers[[length(testServers) + 1]] <- list(
  connectionDetails = details <- createConnectionDetails(
    dbms = "snowflake",
    user = Sys.getenv("CDM_SNOWFLAKE_USER"),
    password = URLdecode(Sys.getenv("CDM_SNOWFLAKE_PASSWORD")),
    connectionString = Sys.getenv("CDM_SNOWFLAKE_CONNECTION_STRING")
  ),
  NULL,
  cdmDatabaseSchema = Sys.getenv("CDM_SNOWFLAKE_CDM53_SCHEMA"),
  tempEmulationSchema = Sys.getenv("CDM_SNOWFLAKE_OHDSI_SCHEMA")
)

# Databricks (Spark)
# Databricks is causing segfault errors on Linux. Temporary workaround is not to test on
# Linux
if (.Platform$OS.type == "windows") {
  testServers[[length(testServers) + 1]] <- list(
    connectionDetails = details <- createConnectionDetails(
      dbms = "spark",
      user = Sys.getenv("CDM5_SPARK_USER"),
      password = URLdecode(Sys.getenv("CDM5_SPARK_PASSWORD")),
      connectionString = Sys.getenv("CDM5_SPARK_CONNECTION_STRING")
    ),
    NULL,
    cdmDatabaseSchema = Sys.getenv("CDM5_SPARK_CDM_SCHEMA"),
    tempEmulationSchema = Sys.getenv("CDM5_SPARK_OHDSI_SCHEMA")
  )
}

# BigQuery
# To avoid rate limit on BigQuery, only test on 1 OS:
if (.Platform$OS.type == "windows") {
  bqKeyFile <- tempfile(fileext = ".json")
  writeLines(Sys.getenv("CDM_BIG_QUERY_KEY_FILE"), bqKeyFile)
  if (testthat::is_testing()) {
    withr::defer(unlink(bqKeyFile, force = TRUE), testthat::teardown_env())
  }
  bqConnectionString <- gsub("<keyfile path>",
                             normalizePath(bqKeyFile, winslash = "/"),
                             Sys.getenv("CDM_BIG_QUERY_CONNECTION_STRING"))
  testServers[[length(testServers) + 1]] <- list(
    connectionDetails = details <- createConnectionDetails(
      dbms = "bigquery",
      user = "",
      password = "",
      connectionString = !!bqConnectionString
    ),
    NULL,
    cdmDatabaseSchema = Sys.getenv("CDM_BIG_QUERY_CDM_SCHEMA"),
    tempEmulationSchema = Sys.getenv("CDM_BIG_QUERY_OHDSI_SCHEMA")
  )
}

# SQLite
sqliteFile <- tempfile(fileext = ".sqlite")
if (testthat::is_testing()) {
  withr::defer(unlink(sqliteFile, force = TRUE), testthat::teardown_env())
}
cdmDatabaseSchema <- "main"
connectionDetails <- createConnectionDetails(
  dbms = "sqlite",
  server = sqliteFile
)
connection <- connect(connectionDetails)
insertTable(
  connection = connection,
  databaseSchema = cdmDatabaseSchema,
  tableName = "person",
  data = data.frame(person_id = seq_len(100),
                    year_of_birth = round(runif(100, 1900, 2000)),
                    race_concept_id = as.numeric(NA),
                    gender_concept_id = rep(c(8507, 8532), 50),
                    care_site_id = round(runif(100, 1, 1e7)))
)
insertTable(
  connection = connection,
  databaseSchema = cdmDatabaseSchema,
  tableName = "observation_period",
  data = data.frame(person_id = seq_len(100),
                    observation_period_start_date = rep(as.Date("2000-01-01"), 100),
                    observation_period_end_date = rep(as.Date(c("2000-06-01", "2001-12-31")), 50),
                    period_type_concept_id = rep(0, 100))
)
insertTable(
  connection = connection,
  databaseSchema = cdmDatabaseSchema,
  tableName = "vocabulary",
  data = data.frame(vocabulary_id = c("a", "b"),
                    vocabulary_name = c("a", "b"),
                    vocabulary_reference= c("a", "b"),
                    vocabulary_version = c("a", "b"),
                    vocabulary_concpet_id = c(1, 2))
)
disconnect(connection)
testServers[[length(testServers) + 1]] <- list(
  connectionDetails = connectionDetails,
  NULL,
  cdmDatabaseSchema = cdmDatabaseSchema,
  tempEmulationSchema = NULL
)

# DuckDB
duckdbFile <- tempfile(fileext = ".duckdb")
if (testthat::is_testing()) {
  withr::defer(unlink(duckdbFile, force = TRUE), testthat::teardown_env())
}
cdmDatabaseSchema <- "main"
connectionDetails <- createConnectionDetails(
  dbms = "duckdb",
  server = duckdbFile
)
connection <- connect(connectionDetails)
insertTable(
  connection = connection,
  databaseSchema = cdmDatabaseSchema,
  tableName = "person",
  data = data.frame(person_id = seq_len(100),
                    year_of_birth = round(runif(100, 1900, 2000)),
                    race_concept_id = as.numeric(NA),
                    gender_concept_id = rep(c(8507, 8532), 50),
                    care_site_id = round(runif(100, 1, 1e7)))
)
insertTable(
  connection = connection,
  databaseSchema = cdmDatabaseSchema,
  tableName = "observation_period",
  data = data.frame(person_id = seq_len(100),
                    observation_period_start_date = rep(as.Date("2000-01-01"), 100),
                    observation_period_end_date = rep(as.Date(c("2000-06-01", "2001-12-31")), 50),
                    period_type_concept_id = rep(0, 100))
)
insertTable(
  connection = connection,
  databaseSchema = cdmDatabaseSchema,
  tableName = "vocabulary",
  data = data.frame(vocabulary_id = c("a", "b"),
                    vocabulary_name = c("a", "b"),
                    vocabulary_reference= c("a", "b"),
                    vocabulary_version = c("a", "b"),
                    vocabulary_concpet_id = c(1, 2))
)
disconnect(connection)
testServers[[length(testServers) + 1]] <- list(
  connectionDetails = connectionDetails,
  NULL,
  cdmDatabaseSchema = cdmDatabaseSchema,
  tempEmulationSchema = NULL
)
