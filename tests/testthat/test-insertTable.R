library(testthat)

if (DatabaseConnector:::is_installed("ParallelLogger")) {
  options(LOG_DATABASECONNECTOR_SQL = TRUE)
  logFileName <- tempfile(fileext = ".txt")
  ParallelLogger::addDefaultFileLogger(logFileName, name = "TEST_LOGGER")
}

set.seed(0)
day.start <- "1960/01/01"
day.end <- "2000/12/31"
time.start <- as.POSIXct("2018-11-12 09:04:07")
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

for (testServer in testServers) {
  test_that(addDbmsToLabel("Insert data", testServer), {
    if (testServer$connectionDetails$dbms %in% c("redshift", "bigquery")) {
      # Inserting on RedShift or BigQuery is slow (Without bulk upload), so 
      # taking subset:
      dataCopy1 <- data[1:10, ]
    } else {
      dataCopy1 <- data
    }
    
    connection <- connect(testServer$connectionDetails)
    options(sqlRenderTempEmulationSchema = testServer$tempEmulationSchema)
    on.exit(dropEmulatedTempTables(connection))
    on.exit(disconnect(connection), add = TRUE)
    insertTable(
      connection = connection,
      tableName = "#temp",
      data = dataCopy1,
      createTable = TRUE,
      tempTable = TRUE
    )
    
    # Check data on server is same as local
    dataCopy2 <- renderTranslateQuerySql(connection, "SELECT * FROM #temp;", integer64AsNumeric = FALSE)
    names(dataCopy2) <- tolower(names(dataCopy2))
    dataCopy1 <- data[order(dataCopy1$person_id), ]
    dataCopy2 <- dataCopy2[order(dataCopy2$person_id), ]
    row.names(dataCopy1) <- NULL
    row.names(dataCopy2) <- NULL
    attr(dataCopy1$some_datetime, "tzone") <- NULL
    attr(dataCopy2$some_datetime, "tzone") <- NULL
    expect_equal(dataCopy1, dataCopy2, check.attributes = FALSE, tolerance = 1e-7)
    
    # Check data types
    res <- dbSendQuery(connection, "SELECT * FROM #temp;")
    columnInfo <- dbColumnInfo(res)
    dbClearResult(res)
    dbms <- testServer$connectionDetails$dbms
    if (dbms == "postgresql") {
      expect_equal(as.character(columnInfo$field.type), c("date", "timestamp", "int4", "numeric", "varchar", "int8"))
    } else if (dbms == "sql server") {
      expect_equal(as.character(columnInfo$field.type), c("date", "datetime2", "int", "float", "varchar", "bigint"))
    } else if (dbms == "oracle") {
      expect_equal(as.character(columnInfo$field.type), c("DATE", "TIMESTAMP", "NUMBER", "NUMBER", "VARCHAR2", "NUMBER"))
    } else if (dbms == "redshift") {
      expect_equal(as.character(columnInfo$field.type), c("date", "timestamp", "int4", "float8", "varchar", "int8" ))
    } else if (dbms == "sqlite") {
      expect_equal(as.character(columnInfo$type), c("double", "double", "integer", "double", "character", "double"))
    } else if (dbms == "duckdb") {
      expect_equal(as.character(columnInfo$type), c("Date", "POSIXct", "integer", "numeric", "character", "numeric"))
    } else if (dbms == "snowflake") {
      expect_equal(as.character(columnInfo$field.type), c("DATE", "TIMESTAMPNTZ", "NUMBER", "DOUBLE", "VARCHAR", "NUMBER"))
    } else if (dbms == "spark") {
      expect_equal(as.character(columnInfo$field.type), c("DATE", "TIMESTAMP", "INT", "FLOAT", "STRING", "BIGINT"))
    } else if (dbms == "bigquery") {
      expect_equal(as.character(columnInfo$field.type), c("DATE", "DATETIME", "INT64", "FLOAT64", "STRING", "INT64"))
    } else {
      warning("Unable to check column types for ", dbms)
    } 
  })
}

test_that("Logging insertTable times", {
  skip_if_not_installed("ParallelLogger")
  log <- readLines(logFileName)
  insertCount <- sum(grepl("Inserting [0-9]+ rows", log))
  expect_gt(insertCount, 0)
  # writeLines(log)
  ParallelLogger::unregisterLogger("TEST_LOGGER")
  unlink(logFileName)
})

data <- data.frame(
  id = 1:3,
  isPrime = c(NA, FALSE, TRUE)
)

for (testServer in testServers) {
  test_that(addDbmsToLabel("Converting logical to numeric in insertTable", testServer), {
    connection <- connect(testServer$connectionDetails)
    options(sqlRenderTempEmulationSchema = testServer$tempEmulationSchema)
    on.exit(dropEmulatedTempTables(connection))
    on.exit(disconnect(connection), add = TRUE)
    expect_warning(
      insertTable(
        connection = connection,
        tableName = "#temp",
        data = data,
        createTable = TRUE,
        tempTable = TRUE
      ),
      "Column 'isPrime' is of type 'logical'")
    data2 <- renderTranslateQuerySql(connection, "SELECT * FROM #temp;")
    data$isPrime <- as.numeric(data$isPrime)
    names(data2) <- tolower(names(data2))
    data2 <- data2[order(data2$id), ]
    expect_equal(data, data2, check.attributes = FALSE)
  })
}
