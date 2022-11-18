library(testthat)

if (DatabaseConnector:::is_installed("ParallelLogger")) {
  logFileName <- tempfile(fileext = ".txt")
  ParallelLogger::addDefaultFileLogger(logFileName, name = "TEST_LOGGER")
}


test_that("insertTable", {
  set.seed(0)
  day.start <- "1960/01/01"
  day.end <- "2000/12/31"
  time.start <- as.POSIXct("2018-11-12 09:04:07 CET")
  dayseq <- seq.Date(as.Date(day.start), as.Date(day.end), by = "week")
  timeSeq <- time.start + (1:length(dayseq)) * 60 * 60 * 24
  makeRandomStrings <- function(n = 1, lenght = 12) {
    randomString <- c(1:n)
    for (i in 1:n) {
      randomString[i] <- paste(sample(c(0:9, letters, LETTERS), lenght, replace = TRUE),
        collapse = ""
      )
    }
    return(randomString)
  }
  bigInts <- bit64::runif64(length(dayseq))
  data <- data.frame(
    start_date = dayseq,
    some_datetime = timeSeq,
    person_id = as.integer(round(runif(length(dayseq), 1, 1e+07))),
    value = runif(length(dayseq)),
    id = makeRandomStrings(length(dayseq)),
    big_ints = bigInts,
    stringsAsFactors = FALSE
  )

  data$start_date[4] <- NA
  data$some_datetime[6] <- NA
  data$person_id[5] <- NA
  data$value[2] <- NA
  data$id[3] <- NA
  data$big_ints[7] <- NA
  data$big_ints[8] <- 3.3043e+10

  # Postgresql --------------------------------------
  details <- createConnectionDetails(
    dbms = "postgresql",
    user = Sys.getenv("CDM5_POSTGRESQL_USER"),
    password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
    server = Sys.getenv("CDM5_POSTGRESQL_SERVER")
  )
  connection <- connect(details)
  insertTable(
    connection = connection,
    tableName = "temp",
    data = data,
    createTable = TRUE,
    tempTable = TRUE
  )

  # Check data on server is same as local
  data2 <- querySql(connection, "SELECT * FROM temp", integer64AsNumeric = FALSE)
  names(data2) <- tolower(names(data2))
  expect_equal(data, data2, check.attributes = FALSE)

  # Check data types
  res <- dbSendQuery(connection, "SELECT * FROM temp")
  columnInfo <- dbColumnInfo(res)
  dbClearResult(res)
  expect_equal(as.character(columnInfo$field.type), c("date", "timestamp", "int4", "numeric", "varchar", "int8"))

  disconnect(connection)


  # SQL Server ----------------------------------------------
  details <- createConnectionDetails(
    dbms = "sql server",
    user = Sys.getenv("CDM5_SQL_SERVER_USER"),
    password = URLdecode(Sys.getenv("CDM5_SQL_SERVER_PASSWORD")),
    server = Sys.getenv("CDM5_SQL_SERVER_SERVER")
  )
  connection <- connect(details)
  insertTable(
    connection = connection,
    tableName = "#temp",
    data = data,
    createTable = TRUE,
    tempTable = TRUE
  )

  # Check data on server is same as local
  data2 <- querySql(connection, "SELECT * FROM #temp", integer64AsNumeric = FALSE)
  names(data2) <- tolower(names(data2))
  data <- data[order(data$person_id), ]
  data2 <- data2[order(data2$person_id), ]
  row.names(data) <- NULL
  row.names(data2) <- NULL
  expect_equal(data, data2, check.attributes = FALSE)

  # Check data types
  res <- dbSendQuery(connection, "SELECT * FROM #temp")
  columnInfo <- dbColumnInfo(res)
  dbClearResult(res)
  expect_equal(as.character(columnInfo$field.type), c("date", "datetime2", "int", "float", "varchar", "bigint"))

  disconnect(connection)


  # Oracle -----------------------------------------
  details <- createConnectionDetails(
    dbms = "oracle",
    user = Sys.getenv("CDM5_ORACLE_USER"),
    password = URLdecode(Sys.getenv("CDM5_ORACLE_PASSWORD")),
    server = Sys.getenv("CDM5_ORACLE_SERVER")
  )
  schema <- Sys.getenv("CDM5_ORACLE_OHDSI_SCHEMA")
  connection <- connect(details)
  insertTable(
    connection = connection,
    databaseSchema = schema,
    tableName = "temp",
    data = data,
    createTable = TRUE,
    tempTable = FALSE
  )


  # lowLevelQuerySql(connection, "SELECT * FROM all_tab_columns WHERE owner = 'OHDSI' AND table_name = 'TEMP'")

  # Check data on server is same as local
  sql <- SqlRender::render("SELECT * FROM @schema.temp", schema = schema)
  data2 <- querySql(connection, sql, integer64AsNumeric = FALSE)
  names(data2) <- tolower(names(data2))
  data <- data[order(data$person_id), ]
  data2 <- data2[order(data2$person_id), ]
  row.names(data) <- NULL
  row.names(data2) <- NULL
  expect_equal(data, data2, check.attributes = FALSE)

  # Check data types
  res <- dbSendQuery(connection, sprintf("SELECT * FROM %s.temp", schema))
  columnInfo <- dbColumnInfo(res)
  dbClearResult(res)
  expect_equal(as.character(columnInfo$field.type), c("DATE", "TIMESTAMP", "NUMBER", "NUMBER", "VARCHAR2", "NUMBER"))

  disconnect(connection)

  # SQLite ---------------------------------------------------------------
  dbFile <- tempfile()
  details <- createConnectionDetails(
    dbms = "sqlite",
    server = dbFile
  )
  connection <- connect(details)
  insertTable(
    connection = connection,
    tableName = "temp",
    data = data,
    createTable = TRUE,
    tempTable = FALSE
  )

  # Check data on server is same as local
  data2 <- querySql(connection, "SELECT * FROM temp", integer64AsNumeric = FALSE)
  names(data2) <- tolower(names(data2))
  data <- data[order(data$person_id), ]
  data2 <- data2[order(data2$person_id), ]
  row.names(data) <- NULL
  row.names(data2) <- NULL

  attr(data$some_datetime, "tzone") <- NULL
  attr(data2$some_datetime, "tzone") <- NULL
  expect_equal(data, data2, check.attributes = FALSE)

  # Check data types
  res <- dbSendQuery(connection, "SELECT * FROM temp")
  columnInfo <- dbColumnInfo(res)
  dbClearResult(res)
  expect_equal(as.character(columnInfo$type), c("double", "double", "integer", "double", "character", "double"))

  disconnect(connection)
  unlink(dbFile)

  # RedShift --------------------------------------
  details <- createConnectionDetails(
    dbms = "redshift",
    user = Sys.getenv("CDM5_REDSHIFT_USER"),
    password = URLdecode(Sys.getenv("CDM5_REDSHIFT_PASSWORD")),
    server = Sys.getenv("CDM5_REDSHIFT_SERVER")
  )
  connection <- connect(details)

  # Use subset of data because inserts on RedShift are slow:
  subset <- data[1:10, ]

  insertTable(
    connection = connection,
    tableName = "temp",
    data = subset,
    createTable = TRUE,
    tempTable = TRUE
  )

  # Check data on server is same as local
  data2 <- querySql(connection, "SELECT * FROM temp", integer64AsNumeric = FALSE)
  names(data2) <- tolower(names(data2))
  subset <- subset[order(subset$person_id), ]
  data2 <- data2[order(data2$person_id), ]
  expect_equal(subset, data2, check.attributes = FALSE)

  # Check data types
  res <- dbSendQuery(connection, "SELECT * FROM temp")
  columnInfo <- dbColumnInfo(res)
  dbClearResult(res)
  expect_equal(as.character(columnInfo$field.type), c("date", "timestamp", "int4", "float8", "varchar", "int8"))

  disconnect(connection)
})

test_that("Test temp emulation helper functions", {
  expect_true(requiresTempEmulation("bigquery"))
  expect_false(requiresTempEmulation("postgresql"))
  
  expect_error(assertTempEmulationSchemaSet("oracle", tempEmulationSchema = NULL), "emp emulation schema is not set")
  expect_no_error(assertTempEmulationSchemaSet("oracle", tempEmulationSchema = "some_schema"))
  expect_no_error(assertTempEmulationSchemaSet("sql server", tempEmulationSchema = NULL))
})

test_that("Logging insertTable times", {
  skip_if_not_installed("ParallelLogger")
  log <- readLines(logFileName)
  insertCount <- sum(grepl("Inserting [0-9]+ rows", log))
  expect_gt(insertCount, 4)
  # writeLines(log)
  ParallelLogger::unregisterLogger("TEST_LOGGER")
  unlink(logFileName)
})
