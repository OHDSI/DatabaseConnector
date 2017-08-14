library(testthat)

test_that("Get table names", {
  # Postgresql
  details <- createConnectionDetails(dbms = "postgresql",
                                     user = Sys.getenv("CDM5_POSTGRESQL_USER"),
                                     password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
                                     server = Sys.getenv("CDM5_POSTGRESQL_SERVER"))
  connection <- connect(details)
  tables <- getTableNames(connection, Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"))
  expect_true("PERSON" %in% tables)
  disconnect(connection)

  # SQL Server
  details <- createConnectionDetails(dbms = "sql server",
                                     user = Sys.getenv("CDM5_SQL_SERVER_USER"),
                                     password = URLdecode(Sys.getenv("CDM5_SQL_SERVER_PASSWORD")),
                                     server = Sys.getenv("CDM5_SQL_SERVER_SERVER"))
  connection <- connect(details)
  tables <- getTableNames(connection, Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA"))
  expect_true("PERSON" %in% tables)
  disconnect(connection)

  # Oracle
  details <- createConnectionDetails(dbms = "oracle",
                                     user = Sys.getenv("CDM5_ORACLE_USER"),
                                     password = URLdecode(Sys.getenv("CDM5_ORACLE_PASSWORD")),
                                     server = Sys.getenv("CDM5_ORACLE_SERVER"))
  connection <- connect(details)
  tables <- getTableNames(connection, Sys.getenv("CDM5_ORACLE_CDM_SCHEMA"))
  expect_true("PERSON" %in% tables)
  disconnect(connection)

  # RedShift (need to fix insert for non-AWS) details <- createConnectionDetails(dbms = 'redshift',
  # user = Sys.getenv('CDM5_REDSHIFT_USER'), password =
  # URLdecode(Sys.getenv('CDM5_REDSHIFT_PASSWORD')), server = Sys.getenv('CDM5_REDSHIFT_SERVER'),
  # schema = Sys.getenv('CDM5_REDSHIFT_CDM_SCHEMA')) connection <- connect(details)
  # insertTable(connection = connection, tableName = 'person', data = data.frame(person_id = 1),
  # dropTableIfExists = TRUE, createTable = TRUE, tempTable = FALSE) tables <-
  # getTableNames(connection, Sys.getenv('CDM5_REDSHIFT_CDM_SCHEMA')) expect_true('PERSON' %in% tables)
  # disconnect(connection)
})
