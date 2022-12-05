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
        1/10 as A,
        X1,
        X2,
        X1 / X2 as B,
        CAST(1 AS INT) as C,
        CAST(1.1 AS NUMBER(1,0)) as D,
        CAST(1.1 AS FLOAT) as E,
        0.1 as F,
        CAST(9223372036854775807 as NUMBER(19)) as G
    FROM (
        SELECT
          1 as X1,
          10 as X2
        FROM
          DUAL
      )
  ", integerAsNumeric = FALSE, integer64AsNumeric = FALSE)
  
  expect_identical(x, data.frame(
    A = 0.1,
    X1 = 1,
    X2 = 10,
    B = 0.1,
    C = as.integer(1),
    D = as.integer(1),
    E = 1.1,
    F = 0.1,
    G = bit64::as.integer64("9223372036854775807")
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
