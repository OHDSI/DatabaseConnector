#' @param andromeda              An open Andromeda object, for example as created
#'                               using [Andromeda::andromeda()].
#' @param andromedaTableName     The name of the table in the local Andromeda object where the
#'                               results of the query will be stored.
#' @param appendToTable          If FALSE, any existing table in the Andromeda with the same name will be 
#'                               replaced with the new data. If TRUE, data will be appended to an existing
#'                               table, assuming it has the exact same structure.
