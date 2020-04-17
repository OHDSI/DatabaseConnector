library(testthat)

test_that("Fetch results", {
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
  
  disconnect(connection)
  
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
  
  disconnect(connection)
})