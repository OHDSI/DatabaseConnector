library(testthat)

test_that("Open and close connection", {
  # Postgresql --------------------------------------------------
  details <- createConnectionDetails(
    dbms = "postgresql",
    user = Sys.getenv("CDM5_POSTGRESQL_USER"),
    password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
    server = Sys.getenv("CDM5_POSTGRESQL_SERVER")
  )
  connection <- connect(details)
  expect_true(inherits(connection, "DatabaseConnectorConnection"))
  expect_true(disconnect(connection))

  # SQL Server --------------------------------------------------
  details <- createConnectionDetails(
    dbms = "sql server",
    user = Sys.getenv("CDM5_SQL_SERVER_USER"),
    password = URLdecode(Sys.getenv("CDM5_SQL_SERVER_PASSWORD")),
    server = Sys.getenv("CDM5_SQL_SERVER_SERVER")
  )
  connection <- connect(details)
  expect_true(inherits(connection, "DatabaseConnectorConnection"))
  expect_true(disconnect(connection))

  # Oracle --------------------------------------------------
  details <- createConnectionDetails(
    dbms = "oracle",
    user = Sys.getenv("CDM5_ORACLE_USER"),
    password = URLdecode(Sys.getenv("CDM5_ORACLE_PASSWORD")),
    server = Sys.getenv("CDM5_ORACLE_SERVER")
  )
  connection <- connect(details)
  expect_true(inherits(connection, "DatabaseConnectorConnection"))
  expect_true(disconnect(connection))

  # RedShift  --------------------------------------------------
  details <- createConnectionDetails(
    dbms = "redshift",
    user = Sys.getenv("CDM5_REDSHIFT_USER"),
    password = URLdecode(Sys.getenv("CDM5_REDSHIFT_PASSWORD")),
    server = Sys.getenv("CDM5_REDSHIFT_SERVER")
  )
  connection <- connect(details)
  expect_true(inherits(connection, "DatabaseConnectorConnection"))
  expect_true(disconnect(connection))
})

test_that("Open and close connection using connection strings with embedded user and pw", {
  # Postgresql --------------------------------------------------
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
  details <- createConnectionDetails(dbms = "postgresql", connectionString = connectionString)
  connection <- connect(details)
  expect_true(inherits(connection, "DatabaseConnectorConnection"))
  expect_true(disconnect(connection))

  # SQL Server --------------------------------------------------
  connectionString <- paste0(
    "jdbc:sqlserver://",
    Sys.getenv("CDM5_SQL_SERVER_SERVER"),
    ";user=",
    Sys.getenv("CDM5_SQL_SERVER_USER"),
    ";password=",
    URLdecode(Sys.getenv("CDM5_SQL_SERVER_PASSWORD"))
  )

  details <- createConnectionDetails(dbms = "sql server", connectionString = connectionString)
  connection <- connect(details)
  expect_true(inherits(connection, "DatabaseConnectorConnection"))
  expect_true(disconnect(connection))

  # Oracle --------------------------------------------------
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

  details <- createConnectionDetails(dbms = "oracle", connectionString = connectionString)
  connection <- connect(details)
  expect_true(inherits(connection, "DatabaseConnectorConnection"))
  expect_true(disconnect(connection))

  # RedShift --------------------------------------------------
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
  details <- createConnectionDetails(
    dbms = "redshift",
    connectionString = connectionString
  )
  connection <- connect(details)
  expect_true(inherits(connection, "DatabaseConnectorConnection"))
  expect_true(disconnect(connection))

  # Spark --------------------------------------------------
  connectionString <- sprintf(
    "%s;UID=%s;PWD=%s",
    Sys.getenv("CDM5_SPARK_CONNECTION_STRING"),
    Sys.getenv("CDM5_SPARK_USER"),
    URLdecode(Sys.getenv("CDM5_SPARK_PASSWORD"))
  )

  details <- createConnectionDetails(
    dbms = "spark",
    connectionString = connectionString
  )
  connection <- connect(details)
  expect_true(inherits(connection, "DatabaseConnectorConnection"))
  expect_true(disconnect(connection))
})

test_that("Open and close connection using connection strings with separate user and pw", {
  # Postgresql --------------------------------------------------
  parts <- unlist(strsplit(Sys.getenv("CDM5_POSTGRESQL_SERVER"), "/"))
  host <- parts[1]
  database <- parts[2]
  port <- "5432"
  connectionString <- paste0("jdbc:postgresql://", host, ":", port, "/", database)
  details <- createConnectionDetails(
    dbms = "postgresql",
    connectionString = connectionString,
    user = Sys.getenv("CDM5_POSTGRESQL_USER"),
    password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD"))
  )
  connection <- connect(details)
  expect_true(inherits(connection, "DatabaseConnectorConnection"))
  expect_true(disconnect(connection))

  # SQL Server --------------------------------------------------
  connectionString <- paste0("jdbc:sqlserver://", Sys.getenv("CDM5_SQL_SERVER_SERVER"))
  details <- createConnectionDetails(
    dbms = "sql server",
    connectionString = connectionString,
    user = Sys.getenv("CDM5_SQL_SERVER_USER"),
    password = URLdecode(Sys.getenv("CDM5_SQL_SERVER_PASSWORD"))
  )
  connection <- connect(details)
  expect_true(inherits(connection, "DatabaseConnectorConnection"))
  expect_true(disconnect(connection))

  # Oracle --------------------------------------------------
  port <- "1521"
  parts <- unlist(strsplit(Sys.getenv("CDM5_ORACLE_SERVER"), "/"))
  host <- parts[1]
  sid <- parts[2]
  connectionString <- paste0("jdbc:oracle:thin:@", host, ":", port, ":", sid)
  details <- createConnectionDetails(
    dbms = "oracle",
    connectionString = connectionString,
    user = Sys.getenv("CDM5_ORACLE_USER"),
    password = URLdecode(Sys.getenv("CDM5_ORACLE_PASSWORD"))
  )
  connection <- connect(details)
  expect_true(inherits(connection, "DatabaseConnectorConnection"))
  expect_true(disconnect(connection))

  # RedShift --------------------------------------------------
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
    database
  )
  details <- createConnectionDetails(
    dbms = "redshift",
    connectionString = connectionString,
    user = Sys.getenv("CDM5_REDSHIFT_USER"),
    password = URLdecode(Sys.getenv("CDM5_REDSHIFT_PASSWORD"))
  )
  connection <- connect(details)
  expect_true(inherits(connection, "DatabaseConnectorConnection"))
  expect_true(disconnect(connection))

  # Spark --------------------------------------------------
  details <- createConnectionDetails(
    dbms = "spark",
    connectionString = Sys.getenv("CDM5_SPARK_CONNECTION_STRING"),
    user = Sys.getenv("CDM5_SPARK_USER"),
    password = URLdecode(Sys.getenv("CDM5_SPARK_PASSWORD"))
  )
  connection <- connect(details)
  expect_true(inherits(connection, "DatabaseConnectorConnection"))
  expect_true(disconnect(connection))
})

test_that("Error is thrown when using incorrect dbms argument", {
  expect_error(createConnectionDetails(dbms = "foobar"), "DBMS 'foobar' not supported")
  expect_error(connect(dbms = "foobar"), "DBMS 'foobar' not supported")
})

test_that("getAvailableJavaHeapSpace returns a positive number", {
  expect_gt(getAvailableJavaHeapSpace(), 0)
})
