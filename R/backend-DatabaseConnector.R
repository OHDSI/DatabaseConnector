#' @export
#' @importFrom dbplyr dbplyr_edition
dbplyr_edition.DatabaseConnectorJdbcConnection <- function(con) {
  2L
}

#' @export
#' @importFrom dbplyr dbplyr_edition
dbplyr_edition.DatabaseConnectorConnection <- function(con) {
  2L
}



#' @export
#' @importFrom dbplyr sql_translation
sql_translation.DatabaseConnectorJdbcConnection <- function(con) {
  # Temporarily add the correct class for dbplyr
  # browser()
  
  dbmsClass <-  switch(dbms(con),
                       "oracle" = "Oracle",
                       "postgresql" = "PqConnection",
                       "redshift" = "RedshiftConnection",
                       "sql server" = "Microsoft SQL Server",
                       "bigquery" = "BigQueryConnection",
                       "sqlite" = "SQLiteConnection",
                       "sqlite extended" = "SQLiteConnection",
                       "spark" = "Spark SQL",
                       "snowflake" = "Snowflake",
                       "synapse" = "Microsoft SQL Server",
                       "duckdb" = "duckdb_connection",
                       "blah") 
  
  if (dbmsClass != "blah") {
    class(con) <- c(dbmsClass, class(con))
    on.exit(class(con) <- setdiff(class(con), dbmsClass))
  }
  NextMethod()
}