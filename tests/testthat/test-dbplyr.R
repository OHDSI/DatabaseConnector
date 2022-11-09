library(testthat)
library(dplyr)

test_that("Get results using dbplyr", {
  # Postgres ----------------------------------------------------------
  connection <- connect(
    dbms = "postgresql",
    user = Sys.getenv("CDM5_POSTGRESQL_USER"),
    password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
    server = Sys.getenv("CDM5_POSTGRESQL_SERVER")
  )
  cdmDatabaseSchema <- Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")
  
  person <- tbl(connection, inDatabaseSchema(cdmDatabaseSchema, "person"))
  nMales <- person %>%
    filter(gender_concept_id == 8507) %>%
    count() %>%
    pull()
  expect_equal(nMales, 502)
  
  observationPeriod <- tbl(connection, inDatabaseSchema(cdmDatabaseSchema, "observation_period"))
  nObsOverOneYear <- observationPeriod %>%
    filter(datediff(day, observation_period_start_date, observation_period_end_date) > 365) %>%
    count() %>%
    pull()
  
  expect_equal(nObsOverOneYear, 955)

  disconnect(connection)
  
  # SQL Server --------------------------------------
  connection <- connect(
    dbms = "sql server",
    user = Sys.getenv("CDM5_SQL_SERVER_USER"),
    password = URLdecode(Sys.getenv("CDM5_SQL_SERVER_PASSWORD")),
    server = Sys.getenv("CDM5_SQL_SERVER_SERVER")
  )
  cdmDatabaseSchema <- Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA")
  
  person <- tbl(connection, inDatabaseSchema(cdmDatabaseSchema, "person"))
  nMales <- person %>%
    filter(gender_concept_id == 8507) %>%
    count() %>%
    pull()
  expect_equal(nMales, 502)
  
  observationPeriod <- tbl(connection, inDatabaseSchema(cdmDatabaseSchema, "observation_period"))
  nObsOverOneYear <- observationPeriod %>%
    filter(datediff(day, observation_period_start_date, observation_period_end_date) > 365) %>%
    count() %>%
    pull()
  
  expect_equal(nObsOverOneYear, 955)
  
  disconnect(connection)
  
  # Oracle ---------------------------------------
  connection <- connect(
    dbms = "oracle",
    user = Sys.getenv("CDM5_ORACLE_USER"),
    password = URLdecode(Sys.getenv("CDM5_ORACLE_PASSWORD")),
    server = Sys.getenv("CDM5_ORACLE_SERVER")
  )
  cdmDatabaseSchema <- Sys.getenv("CDM5_ORACLE_CDM_SCHEMA")
  
  person <- tbl(connection, inDatabaseSchema(cdmDatabaseSchema, "person"))
  nMales <- person %>%
    filter(gender_concept_id == 8507) %>%
    count() %>%
    pull()
  expect_equal(nMales, 502)
  
  observationPeriod <- tbl(connection, inDatabaseSchema(cdmDatabaseSchema, "observation_period"))
  nObsOverOneYear <- observationPeriod %>%
    filter(datediff(day, observation_period_start_date, observation_period_end_date) > 365) %>%
    count() %>%
    pull()
  
  expect_equal(nObsOverOneYear, 955)
  
  disconnect(connection)
  
  # RedShift ----------------------------------------------
  connection <- connect(
    dbms = "redshift",
    user = Sys.getenv("CDM5_REDSHIFT_USER"),
    password = URLdecode(Sys.getenv("CDM5_REDSHIFT_PASSWORD")),
    server = Sys.getenv("CDM5_REDSHIFT_SERVER")
  )
  cdmDatabaseSchema <- Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA")
 
  person <- tbl(connection, inDatabaseSchema(cdmDatabaseSchema, "person"))
  nMales <- person %>%
    filter(gender_concept_id == 8507) %>%
    count() %>%
    pull()
  expect_equal(nMales, 461)
  
  observationPeriod <- tbl(connection, inDatabaseSchema(cdmDatabaseSchema, "observation_period"))
  nObsOverOneYear <- observationPeriod %>%
    filter(datediff(day, observation_period_start_date, observation_period_end_date) > 365) %>%
    count() %>%
    pull()
  
  expect_equal(nObsOverOneYear, 963)
  
  disconnect(connection)
  
  # SQLite -------------------------------------------------
  databaseFile <- tempfile(fileext = ".sqlite")
  cdmDatabaseSchema <- "main"
  connection <- connect(
    dbms = "sqlite",
    server = databaseFile
  )
  insertTable(
    connection = connection,
    databaseSchema = cdmDatabaseSchema,
    tableName = "person",
    data = data.frame(gender_concept_id = rep(c(8507, 8532), 50))
  )
  insertTable(
    connection = connection,
    databaseSchema = cdmDatabaseSchema,
    tableName = "observation_period",
    data = data.frame(observation_period_start_date = rep(as.Date("2000-01-01"), 100),
                      observation_period_end_date = rep(as.Date(c("2000-06-01", "2001-12-31")), 50))
  )
  
  person <- tbl(connection, inDatabaseSchema(cdmDatabaseSchema, "person"))
  nMales <- person %>%
    filter(gender_concept_id == 8507) %>%
    count() %>%
    pull()
  expect_equal(nMales, 50)
  
  observationPeriod <- tbl(connection, inDatabaseSchema(cdmDatabaseSchema, "observation_period"))
  nObsOverOneYear <- observationPeriod %>%
    filter(datediff(day, observation_period_start_date, observation_period_end_date) > 365) %>%
    count() %>%
    pull()
  
  expect_equal(nObsOverOneYear, 50)
  
  disconnect(connection)
  unlink(databaseFile)  
})



test_that("Temp tables using dbplyr", {
  # Postgres ----------------------------------------------------------
  connection <- connect(
    dbms = "postgresql",
    user = Sys.getenv("CDM5_POSTGRESQL_USER"),
    password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
    server = Sys.getenv("CDM5_POSTGRESQL_SERVER")
  )
  cdmDatabaseSchema <- Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")
  
  person <- tbl(connection, inDatabaseSchema(cdmDatabaseSchema, "person"))
  tempTable <- person %>%
    filter(gender_concept_id == 8507) %>%
    compute()
  nMales <- tempTable %>%
    count() %>%
    pull()
  expect_equal(nMales, 502)
  
  disconnect(connection)
  
  # SQL Server --------------------------------------
  connection <- connect(
    dbms = "sql server",
    user = Sys.getenv("CDM5_SQL_SERVER_USER"),
    password = URLdecode(Sys.getenv("CDM5_SQL_SERVER_PASSWORD")),
    server = Sys.getenv("CDM5_SQL_SERVER_SERVER")
  )
  cdmDatabaseSchema <- Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA")
  
  person <- tbl(connection, inDatabaseSchema(cdmDatabaseSchema, "person"))
  tempTable <- person %>%
    filter(gender_concept_id == 8507) %>%
    compute()
  nMales <- tempTable %>%
    count() %>%
    pull()
  expect_equal(nMales, 502)
  
  disconnect(connection)
  
  # Oracle ---------------------------------------
  connection <- connect(
    dbms = "oracle",
    user = Sys.getenv("CDM5_ORACLE_USER"),
    password = URLdecode(Sys.getenv("CDM5_ORACLE_PASSWORD")),
    server = Sys.getenv("CDM5_ORACLE_SERVER")
  )
  cdmDatabaseSchema <- Sys.getenv("CDM5_ORACLE_CDM_SCHEMA")
  options(sqlRenderTempEmulationSchema = Sys.getenv("CDM5_ORACLE_OHDSI_SCHEMA"))
  
  person <- tbl(connection, inDatabaseSchema(cdmDatabaseSchema, "person"))
  tempTable <- person %>%
    filter(gender_concept_id == 8507) %>%
    compute()
  nMales <- tempTable %>%
    count() %>%
    pull()
  expect_equal(nMales, 502)
  
  dropEmulatedTempTables(connection)
  disconnect(connection)
  
  # RedShift ----------------------------------------------
  connection <- connect(
    dbms = "redshift",
    user = Sys.getenv("CDM5_REDSHIFT_USER"),
    password = URLdecode(Sys.getenv("CDM5_REDSHIFT_PASSWORD")),
    server = Sys.getenv("CDM5_REDSHIFT_SERVER")
  )
  cdmDatabaseSchema <- Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA")
  
  person <- tbl(connection, inDatabaseSchema(cdmDatabaseSchema, "person"))
  tempTable <- person %>%
    filter(gender_concept_id == 8507) %>%
    compute()
  nMales <- tempTable %>%
    count() %>%
    pull()
  expect_equal(nMales, 461)
  
  disconnect(connection)
  
  # SQLite -------------------------------------------------
  databaseFile <- tempfile(fileext = ".sqlite")
  cdmDatabaseSchema <- "main"
  connection <- connect(
    dbms = "sqlite",
    server = databaseFile
  )
  insertTable(
    connection = connection,
    databaseSchema = cdmDatabaseSchema,
    tableName = "person",
    data = data.frame(gender_concept_id = rep(c(8507, 8532), 50))
  )
  person <- tbl(connection, inDatabaseSchema(cdmDatabaseSchema, "person"))
  tempTable <- person %>%
    filter(gender_concept_id == 8507) %>%
    compute()
  nMales <- tempTable %>%
    count() %>%
    pull()
  expect_equal(nMales, 50)
  
  disconnect(connection)
  unlink(databaseFile)  
})
