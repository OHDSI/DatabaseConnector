library(testthat)
library(DatabaseConnector)
source("dbplyrTestFunction.R")

test_that("Test dbplyr on Postgres", {
  # Postgres ----------------------------------------------------------
  connectionDetails <- createConnectionDetails(
    dbms = "postgresql",
    user = Sys.getenv("CDM5_POSTGRESQL_USER"),
    password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
    server = Sys.getenv("CDM5_POSTGRESQL_SERVER")
  )
  cdmDatabaseSchema <- Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")
  testDbplyrFunctions(connectionDetails, cdmDatabaseSchema)
})

test_that("Test dbplyr on SQL Server", {
  # SQL Server --------------------------------------
  connectionDetails <- createConnectionDetails(
    dbms = "sql server",
    user = Sys.getenv("CDM5_SQL_SERVER_USER"),
    password = URLdecode(Sys.getenv("CDM5_SQL_SERVER_PASSWORD")),
    server = Sys.getenv("CDM5_SQL_SERVER_SERVER")
  )
  cdmDatabaseSchema <- Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA")
  testDbplyrFunctions(connectionDetails, cdmDatabaseSchema)
  
})

test_that("Test dbplyr on Oracle", {
  # Oracle ---------------------------------------
  connectionDetails <- createConnectionDetails(
    dbms = "oracle",
    user = Sys.getenv("CDM5_ORACLE_USER"),
    password = URLdecode(Sys.getenv("CDM5_ORACLE_PASSWORD")),
    server = Sys.getenv("CDM5_ORACLE_SERVER")
  )
  cdmDatabaseSchema <- Sys.getenv("CDM5_ORACLE_CDM_SCHEMA")
  options(sqlRenderTempEmulationSchema = Sys.getenv("CDM5_ORACLE_OHDSI_SCHEMA"))
  testDbplyrFunctions(connectionDetails, cdmDatabaseSchema)
})

test_that("Test dbplyr on RedShift", {
  # RedShift ----------------------------------------------
  connectionDetails <- createConnectionDetails(
    dbms = "redshift",
    user = Sys.getenv("CDM5_REDSHIFT_USER"),
    password = URLdecode(Sys.getenv("CDM5_REDSHIFT_PASSWORD")),
    server = Sys.getenv("CDM5_REDSHIFT_SERVER")
  )
  cdmDatabaseSchema <- Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA")
  testDbplyrFunctions(connectionDetails, cdmDatabaseSchema)
  
})

test_that("Test dbplyr on SQLite", {
  # SQLite -------------------------------------------------
  databaseFile <- tempfile(fileext = ".sqlite")
  cdmDatabaseSchema <- "main"
  connectionDetails <- createConnectionDetails(
    dbms = "sqlite",
    server = databaseFile
  )
  connection <- connect(connectionDetails)
  insertTable(
    connection = connection,
    databaseSchema = cdmDatabaseSchema,
    tableName = "person",
    data = data.frame(person_id = seq_len(100), 
                      year_of_birth = round(runif(100, 1900, 2000)),
                      race_concept_id = as.numeric(NA),
                      gender_concept_id = rep(c(8507, 8532), 50))
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
  disconnect(connection)
  testDbplyrFunctions(connectionDetails, cdmDatabaseSchema)
  unlink(databaseFile)  
})
