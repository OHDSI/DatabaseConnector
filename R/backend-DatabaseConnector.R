
#' @export
#' @importFrom dbplyr dbplyr_edition
dbplyr_edition.DatabaseConnectorConnection <- function(con) {
  2L
}



#' @export
#' @importFrom dbplyr sql_translation 
sql_translation.DatabaseConnectorJdbcConnection <- function(con) {
  
  switch(dbms(con),
     "postgresql" = get("sql_translation.PqConnection", envir = asNamespace("dbplyr")),     
         
     # "oracle" = dbplyr:::sql_translation.Oracle(con),
     # "postgresql" = dbplyr:::sql_translation.PqConnection(con),
     # "redshift" = dbplyr:::sql_translation.RedshiftConnection(con),
     # "sql server" = `dbplyr:::sql_translation.Microsoft SQL Server`(con),
     # "bigquery" = dbplyr:::sql_translation.BigQueryConnection(con),
     # "sqlite" = dbplyr:::sql_translation.SQLiteConnection(con),
     # "sqlite extended" = dbplyr:::sql_translation.SQLiteConnection(con),
     # "spark" = `dbplyr:::sql_translation.Spark SQL`(con),
     # "snowflake" = dbplyr:::sql_translation.Snowflake(con),
     # "synapse" = `dbplyr:::sql_translation.Microsoft SQL Server`(con),
     # "duckdb" = duckdb:::sql_translation.duckdb_connection(con),
     rlang::abort("Sql dialect is not supported!")) 
}

