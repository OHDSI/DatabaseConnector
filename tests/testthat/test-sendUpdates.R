library(testthat)

test_that("Send updates", {
  connection <- connect(dbms = "postgresql",
                        user = Sys.getenv("CDM5_POSTGRESQL_USER"),
                        password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
                        server = Sys.getenv("CDM5_POSTGRESQL_SERVER"),
                        schema = Sys.getenv("CDM5_POSTGRESQL_OHDSI_SCHEMA"))

  # Insert table:
  data <- data.frame(a = c(1, 2, 3), b = c("a", "b", "c"), stringsAsFactors = FALSE)
  insertTable(connection = connection,
              tableName = "temp",
              data = data,
              createTable = TRUE,
              tempTable = TRUE)
  data2 <- querySql(connection, "SELECT * FROM temp")
  names(data2) <- tolower(names(data2))
  expect_identical(data, data2)

  disconnect(connection)
})
