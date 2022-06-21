# details <- dbConnectDetails(
#   dbms = "postgresql",
#   user = Sys.getenv("CDM5_POSTGRESQL_USER"),
#   password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
#   server = Sys.getenv("CDM5_POSTGRESQL_SERVER")
# )
# connection <- connect(details)
# expect_true(inherits(connection, "DatabaseConnectorConnection"))
# expect_true(disconnect(connection))



