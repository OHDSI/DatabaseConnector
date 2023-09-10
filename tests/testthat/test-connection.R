library(testthat)

test_that("postgresql - dbConnect", {
  con <- DBI::dbConnect(DatabaseConnectorDriver(),
                        dbms = "postgresql",
                        server = Sys.getenv("CDM5_POSTGRESQL_SERVER"),
                        user = Sys.getenv("CDM5_POSTGRESQL_USER"),
                        password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD"))
  
  expect_true(inherits(con, "DatabaseConnectorConnection"))
  expect_equal(dbms(con), "postgresql")
  expect_true(DBI::dbIsValid(con))
  expect_true(DBI::dbDisconnect(con))
  expect_false(DBI::dbIsValid(con))
})

test_that("sql server - dbConnect", {
  con <- DBI::dbConnect(DatabaseConnectorDriver(),
                        dbms = "sql server",
                        server = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
                        user = Sys.getenv("CDM5_SQL_SERVER_USER"),
                        password = Sys.getenv("CDM5_SQL_SERVER_PASSWORD"),
                        port = Sys.getenv("CDM5_SQL_SERVER_PORT"))
                        
  expect_true(inherits(con, "DatabaseConnectorConnection"))
  expect_equal(dbms(con), "sql server")
  expect_true(DBI::dbIsValid(con))
  expect_true(DBI::dbDisconnect(con))
  expect_false(DBI::dbIsValid(con))
})

test_that("oracle - dbConnect", {
  con <- DBI::dbConnect(DatabaseConnectorDriver(),
                        dbms = "oracle",
                        user = Sys.getenv("CDM5_ORACLE_USER"),
                        password = Sys.getenv("CDM5_ORACLE_PASSWORD"),
                        server = Sys.getenv("CDM5_ORACLE_SERVER"))
  
  expect_true(inherits(con, "DatabaseConnectorConnection"))
  expect_equal(dbms(con), "oracle")
  expect_true(DBI::dbIsValid(con))
  expect_true(DBI::dbDisconnect(con))
  expect_false(DBI::dbIsValid(con))
})

test_that("redshift - dbConnect", {
  con <- DBI::dbConnect(DatabaseConnectorDriver(),
                        dbms = "redshift",
                        server = Sys.getenv("CDM5_REDSHIFT_SERVER"),
                        user = Sys.getenv("CDM5_REDSHIFT_USER"),
                        password = Sys.getenv("CDM5_REDSHIFT_PASSWORD"),
                        port = Sys.getenv("CDM5_REDSHIFT_PORT"))
                        conn <- connect(connectionDetails)
                        
  expect_true(inherits(con, "DatabaseConnectorConnection"))
  expect_equal(dbms(con), "redshift")
  expect_true(DBI::dbIsValid(con))
  expect_true(DBI::dbDisconnect(con))
  expect_false(DBI::dbIsValid(con))
})

# Test connection strings with embedded user and pw -----
test_that("postgresql - connectionString", {
  
  parts <- unlist(strsplit(Sys.getenv("CDM5_POSTGRESQL_SERVER"), "/"))
  host <- parts[1]
  database <- parts[2]
  port <- "5432"
  connectionString <- paste0(
    "jdbc:postgresql://",
    host,
    ":",
    port,
    "/",
    database,
    "?user=",
    Sys.getenv("CDM5_POSTGRESQL_USER"),
    "&password=",
    URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD"))
  )
  
  con <- DBI::dbConnect(
    DatabaseConnectorDriver(),
    dbms = "postgresql", 
    connectionString = connectionString)
  
  expect_true(inherits(con, "DatabaseConnectorConnection"))
  expect_equal(dbms(con), "postgresql")
  expect_true(DBI::dbIsValid(con))
  expect_true(DBI::dbDisconnect(con))
  expect_false(DBI::dbIsValid(con))
})


test_that("sql server - connectionString", {
  connectionString <- paste0(
    "jdbc:sqlserver://",
    Sys.getenv("CDM5_SQL_SERVER_SERVER"),
    ";user=",
    Sys.getenv("CDM5_SQL_SERVER_USER"),
    ";password=",
    URLdecode(Sys.getenv("CDM5_SQL_SERVER_PASSWORD"))
  )
  
  con <- DBI::dbConnect(
    DatabaseConnectorDriver(),
    dbms = "sql server", 
    connectionString = connectionString)
  
  expect_true(inherits(con, "DatabaseConnectorConnection"))
  expect_equal(dbms(con), "sql server")
  expect_true(DBI::dbIsValid(con))
  expect_true(DBI::dbDisconnect(con))
  expect_false(DBI::dbIsValid(con))
})

test_that("oracle - connectionString", {
  port <- "1521"
  parts <- unlist(strsplit(Sys.getenv("CDM5_ORACLE_SERVER"), "/"))
  host <- parts[1]
  sid <- parts[2]

  connectionString <- paste0(
    "jdbc:oracle:thin:",
    Sys.getenv("CDM5_ORACLE_USER"),
    "/",
    URLdecode(Sys.getenv("CDM5_ORACLE_PASSWORD")),
    "@",
    host,
    ":",
    port,
    ":",
    sid
  )

  con <- DBI::dbConnect(
    DatabaseConnectorDriver(),
    dbms = "oracle", 
    connectionString = connectionString)
  
  expect_true(inherits(con, "DatabaseConnectorConnection"))
  expect_equal(dbms(con), "sql server")
  expect_true(DBI::dbIsValid(con))
  expect_true(DBI::dbDisconnect(con))
  expect_false(DBI::dbIsValid(con))
  
})

test_that("redshift - connectionString", {
  
  parts <- unlist(strsplit(Sys.getenv("CDM5_REDSHIFT_SERVER"), "/"))
  host <- parts[1]
  database <- parts[2]
  port <- "5439"
  connectionString <- paste0(
    "jdbc:redshift://",
    host,
    ":",
    port,
    "/",
    database,
    "?user=",
    Sys.getenv("CDM5_REDSHIFT_USER"),
    "&password=",
    URLdecode(Sys.getenv("CDM5_REDSHIFT_PASSWORD"))
  )
  
  con <- DBI::dbConnect(
    DatabaseConnectorDriver(),
    dbms = "oracle", 
    connectionString = connectionString)
  
  expect_true(inherits(con, "DatabaseConnectorConnection"))
  expect_equal(dbms(con), "sql server")
  expect_true(DBI::dbIsValid(con))
  expect_true(DBI::dbDisconnect(con))
  expect_false(DBI::dbIsValid(con))
})

# TODO set up manual test
  # Spark --------------------------------------------------
  # Disabling Spark unit tests until new testing server is up
  # connectionString <- sprintf(
  #   "%s;UID=%s;PWD=%s",
  #   Sys.getenv("CDM5_SPARK_CONNECTION_STRING"),
  #   Sys.getenv("CDM5_SPARK_USER"),
  #   URLdecode(Sys.getenv("CDM5_SPARK_PASSWORD"))
  # )
  # 
  # details <- createConnectionDetails(
  #   dbms = "spark",
  #   connectionString = connectionString
  # )
  # connection <- connect(details)
  # expect_true(inherits(connection, "DatabaseConnectorConnection"))
  # expect_true(disconnect(connection))

test_that("snowflake - connectionString", {
  skip("failing test")
  
  connectionString <- sprintf(
    "%s;UID=%s;PWD=%s",
    Sys.getenv("CDM5_SNOWFLAKE_CONNECTION_STRING"),
    Sys.getenv("CDM5_SNOWFLAKE_USER"),
    URLdecode(Sys.getenv("CDM5_SNOWFLAKE_PASSWORD"))
  )
  con <- DBI::dbConnect(
    DatabaseConnectorDriver(),
    dbms = "snowflake", 
    connectionString = connectionString)

  expect_true(inherits(con, "DatabaseConnectorConnection"))
  expect_equal(dbms(con), "snowflake")
  expect_true(DBI::dbIsValid(con))
  expect_true(DBI::dbDisconnect(con))
  expect_false(DBI::dbIsValid(con))
})


test_that("postgresql - connectionString without password", {
  parts <- unlist(strsplit(Sys.getenv("CDM5_POSTGRESQL_SERVER"), "/"))
  host <- parts[1]
  database <- parts[2]
  port <- "5432"
  connectionString <- paste0("jdbc:postgresql://", host, ":", port, "/", database)
  
  con <- DBI::dbConnect(DatabaseConnectorDriver(),
    dbms = "postgresql",
    connectionString = connectionString,
    user = Sys.getenv("CDM5_POSTGRESQL_USER"),
    password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD")
  )
  
  expect_true(inherits(con, "DatabaseConnectorConnection"))
  expect_equal(dbms(con), "postgresql")
  expect_true(DBI::dbIsValid(con))
  expect_true(DBI::dbDisconnect(con))
  expect_false(DBI::dbIsValid(con))
  
})

test_that("sql server - connectionString without password", {
  connectionString <- paste0("jdbc:sqlserver://", Sys.getenv("CDM5_SQL_SERVER_SERVER"))
  
  con <- DBI::dbConnect(DatabaseConnectorDriver(),
                        dbms = "sql server",
                        connectionString = connectionString,
                        user = Sys.getenv("CDM5_SQL_SERVER_USER"),
                        password = URLdecode(Sys.getenv("CDM5_SQL_SERVER_PASSWORD"))
  )
  
  expect_true(inherits(con, "DatabaseConnectorConnection"))
  expect_equal(dbms(con), "sql server")
  expect_true(DBI::dbIsValid(con))
  expect_true(DBI::dbDisconnect(con))
  expect_false(DBI::dbIsValid(con))
})

test_that("oracle - connectionString without password", {
  port <- "1521"
  parts <- unlist(strsplit(Sys.getenv("CDM5_ORACLE_SERVER"), "/"))
  host <- parts[1]
  sid <- parts[2]
  connectionString <- paste0("jdbc:oracle:thin:@", host, ":", port, ":", sid)
  
  con <- DBI::dbConnect(DatabaseConnectorDriver(),
    dbms = "oracle",
    connectionString = connectionString,
    user = Sys.getenv("CDM5_ORACLE_USER"),
    password = URLdecode(Sys.getenv("CDM5_ORACLE_PASSWORD"))
  )
  
  expect_true(inherits(con, "DatabaseConnectorConnection"))
  expect_equal(dbms(con), "oracle")
  expect_true(DBI::dbIsValid(con))
  expect_true(DBI::dbDisconnect(con))
  expect_false(DBI::dbIsValid(con))
})

test_that("redshift - connectionString without password", {
  parts <- unlist(strsplit(Sys.getenv("CDM5_REDSHIFT_SERVER"), "/"))
  host <- parts[1]
  database <- parts[2]
  port <- "5439"
  connectionString <- paste0("jdbc:redshift://", host, ":", port, "/", database)
  
  con <- DBI::dbConnect(DatabaseConnectorDriver(),
                        dbms = "redshift",
                        connectionString = connectionString,
                        user = Sys.getenv("CDM5_REDSHIFT_USER"),
                        password = URLdecode(Sys.getenv("CDM5_REDSHIFT_PASSWORD"))
  )
  
  expect_true(inherits(con, "DatabaseConnectorConnection"))
  expect_equal(dbms(con), "oracle")
  expect_true(DBI::dbIsValid(con))
  expect_true(DBI::dbDisconnect(con))
  expect_false(DBI::dbIsValid(con))
})

test_that("Error is thrown when forgetting password", {
  expect_error(
    DBI::dbConnect(DatabaseConnectorDriver(),
                   dbms = "postgresql",
                   server = Sys.getenv("CDM5_POSTGRESQL_SERVER"),
                   user = Sys.getenv("CDM5_POSTGRESQL_USER")), 
    "Connection propery 'password' is NULL")
})
