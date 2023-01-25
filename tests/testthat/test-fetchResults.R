library(testthat)

if (DatabaseConnector:::is_installed("ParallelLogger")) {
  options(LOG_DATABASECONNECTOR_SQL = TRUE)
  logFileName <- tempfile(fileext = ".txt")
  ParallelLogger::addDefaultFileLogger(logFileName, name = "TEST_LOGGER")
}

test_that("Fetch results", {
  # Postgres ----------------------------------------------------------
  connection <- connect(
    dbms = "postgresql",
    user = Sys.getenv("CDM5_POSTGRESQL_USER"),
    password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
    server = Sys.getenv("CDM5_POSTGRESQL_SERVER")
  )
  cdmDatabaseSchema <- Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")
  sql <- "SELECT COUNT(*) AS row_count FROM @cdm_database_schema.vocabulary"
  renderedSql <- SqlRender::render(sql, cdm_database_schema = cdmDatabaseSchema)
  
  # Fetch data.frame:
  count <- querySql(connection, renderedSql)
  expect_equal(count[1, 1], 58)
  count <- renderTranslateQuerySql(connection, sql, cdm_database_schema = cdmDatabaseSchema)
  expect_equal(count[1, 1], 58)
  
  # Fetch Andromeda:
  andromeda <- Andromeda::andromeda()
  querySqlToAndromeda(connection, renderedSql, andromeda = andromeda, andromedaTableName = "test", snakeCaseToCamelCase = TRUE)
  expect_equivalent(dplyr::collect(andromeda$test)$rowCount[1], 58)
  renderTranslateQuerySqlToAndromeda(connection,
                                     sql,
                                     cdm_database_schema = cdmDatabaseSchema,
                                     andromeda = andromeda,
                                     andromedaTableName = "test2",
                                     snakeCaseToCamelCase = TRUE
  )
  expect_equivalent(dplyr::collect(andromeda$test2)$rowCount[1], 58)
  if (inherits(andromeda, "SQLiteConnection")) {
    Andromeda::close(andromeda)
  } else {
    close(andromeda)
  }
  
  disconnect(connection)
  
  # SQL Server --------------------------------------
  connection <- connect(
    dbms = "sql server",
    user = Sys.getenv("CDM5_SQL_SERVER_USER"),
    password = URLdecode(Sys.getenv("CDM5_SQL_SERVER_PASSWORD")),
    server = Sys.getenv("CDM5_SQL_SERVER_SERVER")
  )
  cdmDatabaseSchema <- Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA")
  sql <- "SELECT COUNT(*) AS row_count FROM @cdm_database_schema.vocabulary"
  renderedSql <- SqlRender::render(sql, cdm_database_schema = cdmDatabaseSchema)
  
  # Fetch data.frame:
  count <- querySql(connection, renderedSql)
  expect_equal(count[1, 1], 71)
  count <- renderTranslateQuerySql(connection, sql, cdm_database_schema = cdmDatabaseSchema)
  expect_equal(count[1, 1], 71)
  
  # Fetch Andromeda:
  andromeda <- Andromeda::andromeda()
  querySqlToAndromeda(connection, renderedSql, andromeda = andromeda, andromedaTableName = "test", snakeCaseToCamelCase = TRUE)
  expect_equivalent(dplyr::collect(andromeda$test)$rowCount[1], 71)
  renderTranslateQuerySqlToAndromeda(connection,
                                     sql,
                                     cdm_database_schema = cdmDatabaseSchema,
                                     andromeda = andromeda,
                                     andromedaTableName = "test2",
                                     snakeCaseToCamelCase = TRUE
  )
  expect_equivalent(dplyr::collect(andromeda$test2)$rowCount[1], 71)
  if (inherits(andromeda, "SQLiteConnection")) {
    Andromeda::close(andromeda)
  } else {
    close(andromeda)
  }
  
  disconnect(connection)
  
  # Oracle ---------------------------------------
  connection <- connect(
    dbms = "oracle",
    user = Sys.getenv("CDM5_ORACLE_USER"),
    password = URLdecode(Sys.getenv("CDM5_ORACLE_PASSWORD")),
    server = Sys.getenv("CDM5_ORACLE_SERVER")
  )
  cdmDatabaseSchema <- Sys.getenv("CDM5_ORACLE_CDM_SCHEMA")
  sql <- "SELECT COUNT(*) AS row_count FROM @cdm_database_schema.vocabulary"
  renderedSql <- SqlRender::render(sql, cdm_database_schema = cdmDatabaseSchema)
  
  # Fetch types correctly:
  x <- querySql(connection, "
    SELECT
        1/10 as a,
        x1,
        x2,
        x1 / x2 AS b,
        CAST(1 AS INT) AS c,
        CAST(1.1 AS NUMBER(1,0)) AS d,
        CAST(1.1 AS FLOAT) AS e,
        0.1 AS f,
        CAST(9223372036854775807 AS NUMBER(19)) AS g
    FROM (
        SELECT
          1 AS x1,
          10 AS x2
        FROM
          DUAL
      )
  ", integerAsNumeric = FALSE, integer64AsNumeric = FALSE)
  
  expect_identical(x, data.frame(
    a = 0.1,
    x1 = 1,
    x2 = 10,
    b = 0.1,
    c = as.integer(1),
    d = as.integer(1),
    e = 1.1,
    f = 0.1,
    g = bit64::as.integer64("9223372036854775807")
  ))
  
  # Fetch data.frame:
  count <- querySql(connection, renderedSql)
  expect_equal(count[1, 1], 71)
  count <- renderTranslateQuerySql(connection, sql, cdm_database_schema = cdmDatabaseSchema)
  expect_equal(count[1, 1], 71)
  
  # Fetch Andromeda:
  andromeda <- Andromeda::andromeda()
  querySqlToAndromeda(connection, renderedSql, andromeda = andromeda, andromedaTableName = "test", snakeCaseToCamelCase = TRUE)
  expect_equivalent(dplyr::collect(andromeda$test)$rowCount[1], 71)
  renderTranslateQuerySqlToAndromeda(connection,
                                     sql,
                                     cdm_database_schema = cdmDatabaseSchema,
                                     andromeda = andromeda,
                                     andromedaTableName = "test2",
                                     snakeCaseToCamelCase = TRUE
  )
  expect_equivalent(dplyr::collect(andromeda$test2)$rowCount[1], 71)
  if (inherits(andromeda, "SQLiteConnection")) {
    Andromeda::close(andromeda)
  } else {
    close(andromeda)
  }
  
  disconnect(connection)
  
  # RedShift ----------------------------------------------
  connection <- connect(
    dbms = "redshift",
    user = Sys.getenv("CDM5_REDSHIFT_USER"),
    password = URLdecode(Sys.getenv("CDM5_REDSHIFT_PASSWORD")),
    server = Sys.getenv("CDM5_REDSHIFT_SERVER")
  )
  cdmDatabaseSchema <- Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA")
  sql <- "SELECT COUNT(*) AS row_count FROM @cdm_database_schema.vocabulary"
  renderedSql <- SqlRender::render(sql, cdm_database_schema = cdmDatabaseSchema)
  
  # Fetch data.frame:
  count <- querySql(connection, renderedSql)
  expect_equal(count[1, 1], 91)
  count <- renderTranslateQuerySql(connection, sql, cdm_database_schema = cdmDatabaseSchema)
  expect_equal(count[1, 1], 91)
  
  # Fetch Andromeda:
  andromeda <- Andromeda::andromeda()
  querySqlToAndromeda(connection, renderedSql, andromeda = andromeda, andromedaTableName = "test", snakeCaseToCamelCase = TRUE)
  expect_equivalent(dplyr::collect(andromeda$test)$rowCount[1], 91)
  renderTranslateQuerySqlToAndromeda(connection,
                                     sql,
                                     cdm_database_schema = cdmDatabaseSchema,
                                     andromeda = andromeda,
                                     andromedaTableName = "test2",
                                     snakeCaseToCamelCase = TRUE
  )
  expect_equivalent(dplyr::collect(andromeda$test2)$rowCount[1], 91)
  if (inherits(andromeda, "SQLiteConnection")) {
    Andromeda::close(andromeda)
  } else {
    close(andromeda)
  }
  
  disconnect(connection)
  
  # SQLite --------------------------------------------------
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
  # Fetch data.frame:
  count <- querySql(connection, "SELECT COUNT(*) FROM main.person;")
  expect_equal(count[1, 1], 100)
  count <- renderTranslateQuerySql(connection, "SELECT COUNT(*) FROM @cdm.person;", cdm = cdmDatabaseSchema)
  expect_equal(count[1, 1], 100)
  
  # Fetch Andromeda:
  andromeda <- Andromeda::andromeda()
  querySqlToAndromeda(connection, "SELECT * FROM main.person;", andromeda = andromeda, andromedaTableName = "test", snakeCaseToCamelCase = TRUE)
  expect_equivalent(nrow(dplyr::collect(andromeda$test)), 100)
  
  if (inherits(andromeda, "SQLiteConnection")) {
    Andromeda::close(andromeda)
  } else {
    close(andromeda)
  }
  
  disconnect(connection)
  unlink(databaseFile)  
  
})

test_that("dbFetch works", {
  connection <- connect(
    dbms = "postgresql",
    user = Sys.getenv("CDM5_POSTGRESQL_USER"),
    password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
    server = Sys.getenv("CDM5_POSTGRESQL_SERVER")
  )
  cdmDatabaseSchema <- Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")
  sql <- "SELECT * FROM @cdm_database_schema.vocabulary LIMIT 10"
  renderedSql <- SqlRender::render(sql, cdm_database_schema = cdmDatabaseSchema)
  
  queryResult <- dbSendQuery(connection, renderedSql)
  df <- dbFetch(queryResult)
  dbClearResult(queryResult)
  
  expect_s3_class(df, "data.frame")
  expect_equal(nrow(df), 10)
  disconnect(connection)
  
  # Oracle ---------------------------------------
  connection <- connect(
    dbms = "oracle",
    user = Sys.getenv("CDM5_ORACLE_USER"),
    password = URLdecode(Sys.getenv("CDM5_ORACLE_PASSWORD")),
    server = Sys.getenv("CDM5_ORACLE_SERVER")
  )
  cdmDatabaseSchema <- Sys.getenv("CDM5_ORACLE_CDM_SCHEMA")
  
  sql <- "SELECT * FROM @cdm_database_schema.vocabulary FETCH FIRST 10 ROWS ONLY"
  renderedSql <- SqlRender::render(sql, cdm_database_schema = cdmDatabaseSchema)
  
  queryResult <- dbSendQuery(connection, renderedSql)
  df <- dbFetch(queryResult)
  dbClearResult(queryResult)
  
  disconnect(connection)
  
  # SQL Server --------------------------------------
  connection <- connect(
    dbms = "sql server",
    user = Sys.getenv("CDM5_SQL_SERVER_USER"),
    password = URLdecode(Sys.getenv("CDM5_SQL_SERVER_PASSWORD")),
    server = Sys.getenv("CDM5_SQL_SERVER_SERVER")
  )
  cdmDatabaseSchema <- Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA")
  
  sql <- "SELECT TOP 10 * FROM @cdm_database_schema.vocabulary"
  renderedSql <- SqlRender::render(sql, cdm_database_schema = cdmDatabaseSchema)
  
  queryResult <- dbSendQuery(connection, renderedSql)
  df <- dbFetch(queryResult)
  dbClearResult(queryResult)
  
  disconnect(connection)
  
  # RedShift ----------------------------------------------
  connection <- connect(
    dbms = "redshift",
    user = Sys.getenv("CDM5_REDSHIFT_USER"),
    password = URLdecode(Sys.getenv("CDM5_REDSHIFT_PASSWORD")),
    server = Sys.getenv("CDM5_REDSHIFT_SERVER")
  )
  cdmDatabaseSchema <- Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA")
  
  sql <- "SELECT TOP 10 * FROM @cdm_database_schema.vocabulary"
  renderedSql <- SqlRender::render(sql, cdm_database_schema = cdmDatabaseSchema)
  
  queryResult <- dbSendQuery(connection, renderedSql)
  df <- dbFetch(queryResult)
  dbClearResult(queryResult)
  
  disconnect(connection)
})

test_that("Logging query times", {
  skip_if_not_installed("ParallelLogger")
  
  queryTimes <- extractQueryTimes(logFileName)
  expect_gt(nrow(queryTimes), 16)
  ParallelLogger::unregisterLogger("TEST_LOGGER")
  unlink(logFileName)
})
