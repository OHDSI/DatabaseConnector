library(testthat)

test_that("Fetch results", {
  # Postgres ----------------------------------------------------------
  connection <- connect(dbms = "postgresql",
                        user = Sys.getenv("CDM5_POSTGRESQL_USER"),
                        password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
                        server = Sys.getenv("CDM5_POSTGRESQL_SERVER"))
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
  renderTranslateQuerySqlToAndromeda(connection, sql, cdm_database_schema = cdmDatabaseSchema, andromeda = andromeda, andromedaTableName = "test2", snakeCaseToCamelCase = TRUE)
  expect_equivalent(dplyr::collect(andromeda$test2)$rowCount[1], 58)


  disconnect(connection)
  
  # SQL Server --------------------------------------
  connection <- connect(dbms = "sql server",
                        user = Sys.getenv("CDM5_SQL_SERVER_USER"),
                        password = URLdecode(Sys.getenv("CDM5_SQL_SERVER_PASSWORD")),
                        server = Sys.getenv("CDM5_SQL_SERVER_SERVER"))
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
  renderTranslateQuerySqlToAndromeda(connection, sql, cdm_database_schema = cdmDatabaseSchema, andromeda = andromeda, andromedaTableName = "test2", snakeCaseToCamelCase = TRUE)
  expect_equivalent(dplyr::collect(andromeda$test2)$rowCount[1], 71)

  disconnect(connection)
  
  # Oracle ---------------------------------------
  connection <- connect(dbms = "oracle",
                        user = Sys.getenv("CDM5_ORACLE_USER"),
                        password = URLdecode(Sys.getenv("CDM5_ORACLE_PASSWORD")),
                        server = Sys.getenv("CDM5_ORACLE_SERVER"))
  cdmDatabaseSchema <- Sys.getenv("CDM5_ORACLE_CDM_SCHEMA")
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
  renderTranslateQuerySqlToAndromeda(connection, sql, cdm_database_schema = cdmDatabaseSchema, andromeda = andromeda, andromedaTableName = "test2", snakeCaseToCamelCase = TRUE)
  expect_equivalent(dplyr::collect(andromeda$test2)$rowCount[1], 71)

  disconnect(connection)
})