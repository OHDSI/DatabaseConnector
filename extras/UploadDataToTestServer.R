# Copy Eunomia to a test server ------------------------------------------------

# Assumes connectionDetails points to the test server to upload to
databaseSchema <- "eunomia"

library(DatabaseConnector)


connection <- connect(connectionDetails)
renderTranslateExecuteSql(
  connection = connection, 
  sql = "CREATE SCHEMA @database_schema;",
  database_schema = databaseSchema
)

eunomiaCd <- Eunomia::getEunomiaConnectionDetails()
eunomiaConn <- connect(eunomiaCd)

tableNames <- getTableNames(eunomiaConn, "main")
for (i in seq_along(tableNames)) {
  # for (i in 15:length(tableNames)) {
  message(sprintf("Copying table %s", tableNames[i]))
  table <- renderTranslateQuerySql(
    connection = eunomiaConn,
    sql = "SELECT * FROM main.@table_name;",
    table_name = tableNames[i])
  if (tableNames[i] == "cost") {
    colnames(table)[colnames(table) == "REVEUE_CODE_SOURCE_VALUE"] <- "REVENUE_CODE_SOURCE_VALUE"
  }
  insertTable(
    connection = connection,
    databaseSchema = databaseSchema,
    tableName = tableNames[i],
    data = table,
    dropTableIfExists = TRUE,
    createTable = TRUE,
    progressBar = TRUE
  )
}

disconnect(connection)
disconnect(eunomiaConn)

# Copy data from one test server to another ------------------------------------

library(DatabaseConnector)
options(andromedaTempFolder = "d:/andromedaTemp")

fromConnection <- connect(
  dbms = "sql server",
  user = Sys.getenv("CDM5_SQL_SERVER_USER"),
  password = URLdecode(Sys.getenv("CDM5_SQL_SERVER_PASSWORD")),
  server = Sys.getenv("CDM5_SQL_SERVER_SERVER")
)
fromDatabaseSchema <- Sys.getenv("CDM5_SQL_SERVER_CDM54_SCHEMA")

toConnection <- connect(
  dbms = "postgresql",
  user = Sys.getenv("CDM5_POSTGRESQL_USER"),
  password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
  server = Sys.getenv("CDM5_POSTGRESQL_SERVER")
)
toDatabaseSchema <- Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")

tableNames <- getTableNames(fromConnection, fromDatabaseSchema)
for (i in seq_along(tableNames)) {
  # for (i in 31:length(tableNames)) {
  message(sprintf("Copying table %s", tableNames[i]))
  andromeda <- Andromeda::andromeda()
  renderTranslateQuerySqlToAndromeda(
    connection = fromConnection,
    sql = "SELECT * FROM @database_schema.@table_name;",
    andromeda = andromeda,
    andromedaTableName = tableNames[i],
    database_schema = fromDatabaseSchema,
    table_name = tableNames[i])
  insertTable(
    connection = toConnection,
    databaseSchema = toDatabaseSchema,
    tableName = tableNames[i],
    data = andromeda[[tableNames[i]]],
    dropTableIfExists = TRUE,
    createTable = TRUE,
    progressBar = TRUE
  )
  # querySql(toConnection, "SELECT * FROM cdmv5.condition_era LIMIT 10;")
  Andromeda::close(andromeda)
}


# change timestamp fields to dates if needed:
toTableNames <- getTableNames(toConnection, toDatabaseSchema)
for (i in seq_along(toTableNames)) {
  tableName <- toTableNames[i]
  row <- renderTranslateQuerySql(toConnection,
                                 sql = "SELECT TOP 1 * FROM @schema.@table;",
                                 schema = toDatabaseSchema,
                                 table = tableName)
  dateFieldIdx <- which(grepl("_DATE$", colnames(row)))
  for (j in dateFieldIdx) {
    if (!is(row[,j], "Date")) {
      writeLines(sprintf("Incorrect type of field %s in table %s: %s", colnames(row)[j], tableName, class(row[,j])[1]))
      sql <- "ALTER TABLE @schema.@table ALTER COLUMN @column TYPE date;"
      renderTranslateExecuteSql(toConnection,
                                sql = sql,
                                schema = toDatabaseSchema,
                                table = tableName,
                                column = colnames(row)[j])
    }
  }
  
}

disconnect(fromConnection)
disconnect(toConnection)


