library(usethis)
library(devtools)
#library(ParallelLogger)
#load_all("C:/Users/bdeboe/Github/intersystems-community/OHDSI-SqlRender", TRUE, TRUE, TRUE, TRUE, TRUE)
#.jaddClassPath("C:/Users/bdeboe/Github/intersystems-community/OHDSI-SqlRender/target/SqlRender-1.19.0-SNAPSHOT.jar")
load_all("C:/Users/bdeboe/Github/intersystems-community/OHDSI-DatabaseConnector", TRUE, TRUE, TRUE, TRUE, TRUE)

#Sys.setenv(DATABASECONNECTOR_JAR = "C:\\InterSystems\\SQLML\\dev\\java\\lib\\1.8")
#options(LOG_DATABASECONNECTOR_SQL = TRUE)
# assign("noLogging", FALSE, envir = globalVars)
# logger<-createLogger(name="SIMPLE", threshold="TRACE", appenders=list(createConsoleAppender(layout=layoutTimestamp)))

# ParallelLogger::logTrace("Trace this!")

Sys.setenv("DATABASECONNECTOR_JAR_FOLDER" = "C:\\tmp\\ohdsi")
downloadJdbcDrivers("iris")

connectionDetails <- createConnectionDetails(
  dbms = "iris",
  server = "localhost",
  port = 51774,
  database = "OMOP2",
  user = "_SYSTEM",
  password = "SYS"
)

connection <- DatabaseConnector::connect(NULL, "iris", "_SYSTEM", "SYS", "localhost", 51774)
# connection <- DatabaseConnector::connect(NULL, "iris", "_SYSTEM", "SYS", "localhost", 51774, NULL, NULL, NULL, Sys.getenv("DATABASECONNECTOR_JAR"))
# connection <- DatabaseConnector::connect(connectionDetails)
# ParallelLogger::logTrace("connection succesfull!")

# connection <- DatabaseConnector::connect(NULL, "iris", "SQLAdmin", "Welcome123!", NULL, NULL, NULL, NULL, "jdbc:IRIS://k8s-7dde6a84-a8a08f1d-624d64b975-b5e7efc5c79fe489.elb.us-east-1.amazonaws.com:443/USER/iris-jdbc.log:::true", Sys.getenv("DATABASECONNECTOR_JAR"))

DatabaseConnector::renderTranslateQuerySql(connection, "SELECT 124 as \"test\"")

# ParallelLogger::logTrace("so far so good!")

DatabaseConnector::renderTranslateQuerySql(connection, "WITH x (abx) AS (SELECT 124 as ABX) SELECT * FROM x")

sql <- "WITH rawData (x) AS (SELECT 123 AS x), summ AS (SELECT SUM(x) as s FROM rawData) CREATE TABLE ohdsi.t AS SELECT * FROM summ;"
SqlRender::translate(sql, "iris")
DatabaseConnector::renderTranslateExecuteSql(connection, sql)



Sys.setenv("CDM_IRIS_USER" = "_SYSTEM")
Sys.setenv("CDM_IRIS_PASSWORD" = "SYS")
Sys.setenv("CDM_IRIS_CONNECTION_STRING" = "jdbc:IRIS://localhost:51774/OMOP")
Sys.setenv("CDM_IRIS_CDM53_SCHEMA" = "OMOP_CDM")
Sys.setenv("CDM_IRIS_OHDSI_SCHEMA" = "OMOP_Results")
