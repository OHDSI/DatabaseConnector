DatabaseConnector
=================

A wrapper around RJDBC containing drivers for various DBMSs. Currently included are:
* SQL Server
* Oracle
* Postgres
* Amazon Redshift
* MySQL

For example:
```r
connectionDetails <- createConnectionDetails(dbms="mysql", server="localhost",user="root",password="blah",schema="cdm_v4")
conn <- connect(connectionDetails)
dbGetQuery(conn,"SELECT COUNT(*) FROM person")
dbDisconnect(conn)
```
# Acknowledgements
- This project is supported in part through the National Science Foundation grant IIS 1251151.

