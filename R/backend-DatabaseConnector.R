
#' @export
#' @importFrom dbplyr dbplyr_edition
dbplyr_edition.DatabaseConnectorConnection <- function(con) {
  2L
}

#' @export
#' @importFrom dbplyr sql_translation 
sql_translation.DatabaseConnectorJdbcConnection <- function(con) {
  
  switch(dbms(con),
     "oracle" = utils::getFromNamespace("sql_translation.Oracle", "dbplyr")(con),
     "postgresql" = utils::getFromNamespace("sql_translation.PqConnection", "dbplyr")(con),
     "redshift" = utils::getFromNamespace("sql_translation.RedshiftConnection", "dbplyr")(con),
     "sql server" = utils::getFromNamespace("sql_translation.Microsoft SQL Server", "dbplyr")(con),
     "bigquery" = utils::getFromNamespace("sql_translation.BigQueryConnection", "bigrquery")(con),
     "spark" = utils::getFromNamespace("sql_translation.Spark SQL", "dbplyr")(con),
     "snowflake" = utils::getFromNamespace("sql_translation.Snowflake", "dbplyr")(con),
     "synapse" = utils::getFromNamespace("sql_translation.Microsoft SQL Server", "dbplyr")(con),
     rlang::abort("Sql dialect is not supported!")) 
}
