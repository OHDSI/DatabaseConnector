test_that("renderTranslateQueryApplyBatched works", {

  connection <- connect(dbms = "postgresql",
                        user = Sys.getenv("CDM5_POSTGRESQL_USER"),
                        password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
                        server = Sys.getenv("CDM5_POSTGRESQL_SERVER"))
  cdmDatabaseSchema <- Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")
  sql <- "SELECT * FROM @cdm_database_schema.vocabulary LIMIT 10"

  fun <- function(data) {
    data$test <- "MY STRING"
    return(data)
  }
  data <- renderTranslateQueryApplyBatched(connection,
                                           sql,
                                           fun,
                                           cdm_database_schema = cdmDatabaseSchema,
                                           returnResultsData = TRUE)
  expect_true("test" %in% colnames(data))
  expect_true(all(data$test == "MY STRING"))
  disconnect(connection)


  # Oracle ---------------------------------------
  connection <- connect(dbms = "oracle",
                        user = Sys.getenv("CDM5_ORACLE_USER"),
                        password = URLdecode(Sys.getenv("CDM5_ORACLE_PASSWORD")),
                        server = Sys.getenv("CDM5_ORACLE_SERVER"))
  cdmDatabaseSchema <- Sys.getenv("CDM5_ORACLE_CDM_SCHEMA")
  sql <- "SELECT * FROM @cdm_database_schema.vocabulary FETCH FIRST 10 ROWS ONLY"
  data <- renderTranslateQueryApplyBatched(connection,
                                           sql,
                                           fun,
                                           cdm_database_schema = cdmDatabaseSchema,
                                           returnResultsData = TRUE)
  expect_true("test" %in% colnames(data))
  expect_true(all(data$test == "MY STRING"))
  disconnect(connection)

  # SQL Server --------------------------------------
  connection <- connect(dbms = "sql server",
                        user = Sys.getenv("CDM5_SQL_SERVER_USER"),
                        password = URLdecode(Sys.getenv("CDM5_SQL_SERVER_PASSWORD")),
                        server = Sys.getenv("CDM5_SQL_SERVER_SERVER"))
  cdmDatabaseSchema <- Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA")

  sql <- "SELECT TOP 10 * FROM @cdm_database_schema.vocabulary"
  data <- renderTranslateQueryApplyBatched(connection,
                                           sql,
                                           fun,
                                           cdm_database_schema = cdmDatabaseSchema,
                                           returnResultsData = TRUE)
  expect_true("test" %in% colnames(data))
  expect_true(all(data$test == "MY STRING"))
  disconnect(connection)

  # RedShift ----------------------------------------------
  connection <- connect(dbms = "redshift",
                        user = Sys.getenv("CDM5_REDSHIFT_USER"),
                        password = URLdecode(Sys.getenv("CDM5_REDSHIFT_PASSWORD")),
                        server = Sys.getenv("CDM5_REDSHIFT_SERVER"))
  cdmDatabaseSchema <- Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA")

  sql <- "SELECT TOP 10 * FROM @cdm_database_schema.vocabulary"
  data <- renderTranslateQueryApplyBatched(connection,
                                           sql,
                                           fun,
                                           cdm_database_schema = cdmDatabaseSchema,
                                           returnResultsData = TRUE)
  expect_true("test" %in% colnames(data))
  expect_true(all(data$test == "MY STRING"))
  disconnect(connection)

})