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

Dependecies
===========

Please note that this package requires Java to be installed. If you don't have Java already intalled on your computed (on most computers it already is installed), go to [java.com](http://java.com) to get the latest version.

# Acknowledgements
- This project is supported in part through the National Science Foundation grant IIS 1251151.

