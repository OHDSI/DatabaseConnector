DatabaseConnector
=================

A wrapper around RJDBC containing drivers for various DBMSs. Currently included are:
* SQL Server
* Oracle
* Postgres
* Amazon Redshift
* MySQL
* Netezza (driver not included)

For example:
```r
connectionDetails <- createConnectionDetails(dbms="mysql", server="localhost",user="root",password="blah",schema="cdm_v4")
conn <- connect(connectionDetails)
dbGetQuery(conn,"SELECT COUNT(*) FROM person")
dbDisconnect(conn)
```
Getting Started
===========
Install DatabaseConnector to R in invoke it:

  ```r
  install.packages("devtools")
  library(devtools)
  install_github("ohdsi/DatabaseConnector") 
  library(DatabaseConnector)
  ```

Dependecies
===========

Please note that this package requires Java to be installed. If you don't have Java already intalled on your computed (on most computers it already is installed), go to [java.com](http://java.com) to get the latest version.

To be able to use Windows authentication for SQL Server, you have to install the JDBC driver. Download the .exe from [Microsoft](http://www.microsoft.com/en-us/download/details.aspx?displaylang=en&id=11774) and run it, thereby extracting its contents to a folder. In the extracted folder you will find the file sqljdbc_4.0/enu/auth/x64/sqljdbc_auth.dll (64-bits) or sqljdbc_4.0/enu/auth/x86/sqljdbc_auth.dll (32-bits), which needs to be moved to location on the system path, for example to c:/windows/system32.

In order to enable Netezza support, place your Netezza jdbc driver at `inst/java/nzjdbc.jar` in this package.

# Acknowledgements
- This project is supported in part through the National Science Foundation grant IIS 1251151.

