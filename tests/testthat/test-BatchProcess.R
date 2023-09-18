test_that("renderTranslateQueryApplyBatched works", {
  connection <- connect(
    dbms = "postgresql",
    user = Sys.getenv("CDM5_POSTGRESQL_USER"),
    password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
    server = Sys.getenv("CDM5_POSTGRESQL_SERVER")
  )
  cdmDatabaseSchema <- Sys.getenv("CDM5_POSTGRESQL_CDM54_SCHEMA")
  sql <- "SELECT TOP 10 * FROM @cdm_database_schema.vocabulary;"

  fun <- function(data, position, myString) {
    data$test <- myString
    return(data)
  }
  args <- list(myString = "MY STRING")
  data <- renderTranslateQueryApplyBatched(connection,
    sql,
    fun,
    args,
    cdm_database_schema = cdmDatabaseSchema
  )
  data <- do.call(rbind, data)
  expect_true("test" %in% colnames(data))
  expect_true(all(data$test == "MY STRING"))
  disconnect(connection)


  # Oracle ---------------------------------------
  connection <- connect(
    dbms = "oracle",
    user = Sys.getenv("CDM5_ORACLE_USER"),
    password = URLdecode(Sys.getenv("CDM5_ORACLE_PASSWORD")),
    server = Sys.getenv("CDM5_ORACLE_SERVER")
  )
  cdmDatabaseSchema <- Sys.getenv("CDM5_ORACLE_CDM54_SCHEMA")
  sql <- "SELECT TOP 10 * FROM @cdm_database_schema.vocabulary;"
  data <- renderTranslateQueryApplyBatched(connection,
    sql,
    fun,
    args,
    cdm_database_schema = cdmDatabaseSchema
  )
  data <- do.call(rbind, data)
  expect_true("test" %in% colnames(data))
  expect_true(all(data$test == "MY STRING"))
  disconnect(connection)

  # SQL Server --------------------------------------
  connection <- connect(
    dbms = "sql server",
    user = Sys.getenv("CDM5_SQL_SERVER_USER"),
    password = URLdecode(Sys.getenv("CDM5_SQL_SERVER_PASSWORD")),
    server = Sys.getenv("CDM5_SQL_SERVER_SERVER")
  )
  cdmDatabaseSchema <- Sys.getenv("CDM5_SQL_SERVER_CDM54_SCHEMA")

  sql <- "SELECT TOP 10 * FROM @cdm_database_schema.vocabulary;"
  data <- renderTranslateQueryApplyBatched(connection,
    sql,
    fun,
    args,
    cdm_database_schema = cdmDatabaseSchema
  )
  data <- do.call(rbind, data)
  expect_true("test" %in% colnames(data))
  expect_true(all(data$test == "MY STRING"))
  disconnect(connection)

  # RedShift ----------------------------------------------
  connection <- connect(
    dbms = "redshift",
    user = Sys.getenv("CDM5_REDSHIFT_USER"),
    password = URLdecode(Sys.getenv("CDM5_REDSHIFT_PASSWORD")),
    server = Sys.getenv("CDM5_REDSHIFT_SERVER")
  )
  cdmDatabaseSchema <- Sys.getenv("CDM5_REDSHIFT_CDM54_SCHEMA")

  sql <- "SELECT TOP 10 * FROM @cdm_database_schema.vocabulary;"
  data <- renderTranslateQueryApplyBatched(connection,
    sql,
    fun,
    args,
    cdm_database_schema = cdmDatabaseSchema
  )
  data <- do.call(rbind, data)
  expect_true("test" %in% colnames(data))
  expect_true(all(data$test == "MY STRING"))
  disconnect(connection)


  # Sqlite --------------------------------------------------
  dbFile <- tempfile()
  details <- createConnectionDetails(
    dbms = "sqlite",
    server = dbFile
  )
  connection <- connect(details)
  executeSql(connection, "CREATE TABLE person (x INT);
  INSERT INTO person (x) VALUES (1); INSERT INTO person (x) VALUES (2); INSERT INTO person (x) VALUES (3);")

  sql <- "SELECT * FROM person;"
  data <- renderTranslateQueryApplyBatched(
    connection,
    sql,
    fun,
    args
  )
  data <- do.call(rbind, data)
  expect_true("test" %in% colnames(data))
  expect_true(all(data$test == "MY STRING"))

  disconnect(connection)
  unlink(dbFile)
})
