#' @param databaseSchema   The name of the database schema. See details for platform-specific details.
#'
#' @details
#' The \code{databaseSchema} argument is interpreted differently according to the different platforms:
#' SQL Server and PDW: The databaseSchema schema should specify both the database and the schema, e.g.
#' 'my_database.dbo'. Impala: the databaseSchema should specify the database. Oracle:
#' The databaseSchema should specify the Oracle 'user'. All other : The databaseSchema should
#' specify the schema.
