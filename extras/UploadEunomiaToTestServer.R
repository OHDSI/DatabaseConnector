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
