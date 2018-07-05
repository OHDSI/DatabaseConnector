library(testthat)

test_that("Send updates", {
  set.seed(0)
  day.start <- "1960/01/01"
  day.end <- "2000/12/31"
  dayseq <- seq.Date(as.Date(day.start), as.Date(day.end), by = "week")
  makeRandomStrings <- function(n = 1, lenght = 12) {
    randomString <- c(1:n)
    for (i in 1:n) randomString[i] <- paste(sample(c(0:9, letters, LETTERS), lenght, replace = TRUE),
                                            collapse = "")
    return(randomString)
  }
  data <- data.frame(start_date = dayseq,
                     person_id = as.integer(round(runif(length(dayseq), 1, 1e+07))),
                     value = runif(length(dayseq)),
                     id = makeRandomStrings(length(dayseq)),
                     stringsAsFactors = FALSE)
  
  data$start_date[4] <- NA
  data$person_id[5] <- NA
  data$value[2] <- NA
  data$id[3] <- NA
  
  
  # Postgresql
  details <- createConnectionDetails(dbms = "postgresql",
                                     user = Sys.getenv("CDM5_POSTGRESQL_USER"),
                                     password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
                                     server = Sys.getenv("CDM5_POSTGRESQL_SERVER"),
                                     schema = Sys.getenv("CDM5_POSTGRESQL_OHDSI_SCHEMA"))
  connection <- connect(details)
  insertTable(connection = connection,
              tableName = "temp",
              data = data,
              createTable = TRUE,
              tempTable = TRUE)
  data2 <- querySql(connection, "SELECT * FROM temp")
  names(data2) <- tolower(names(data2))
  expect_equal(data, data2)
  disconnect(connection)
  
  
  # SQL Server
  details <- createConnectionDetails(dbms = "sql server",
                                     user = Sys.getenv("CDM5_SQL_SERVER_USER"),
                                     password = URLdecode(Sys.getenv("CDM5_SQL_SERVER_PASSWORD")),
                                     server = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
                                     schema = Sys.getenv("CDM5_SQL_SERVER_OHDSI_SCHEMA"))
  connection <- connect(details)
  insertTable(connection = connection,
              tableName = "#temp",
              data = data,
              createTable = TRUE,
              tempTable = TRUE)
  data2 <- querySql(connection, "SELECT * FROM #temp")
  names(data2) <- tolower(names(data2))
  expect_equal(data, data2)
  disconnect(connection)
  
  
  # Oracle
  details <- createConnectionDetails(dbms = "oracle",
                                     user = Sys.getenv("CDM5_ORACLE_USER"),
                                     password = URLdecode(Sys.getenv("CDM5_ORACLE_PASSWORD")),
                                     server = Sys.getenv("CDM5_ORACLE_SERVER"),
                                     schema = Sys.getenv("CDM5_ORACLE_OHDSI_SCHEMA"))
  connection <- connect(details)
  insertTable(connection = connection,
              tableName = "temp",
              data = data,
              createTable = TRUE,
              tempTable = FALSE)
  data2 <- querySql(connection, "SELECT * FROM temp")
  names(data2) <- tolower(names(data2))
  data <- data[order(data$person_id), ]
  data2 <- data2[order(data2$person_id), ]
  row.names(data) <- NULL
  row.names(data2) <- NULL
  expect_equal(data, data2)
  disconnect(connection)
})
