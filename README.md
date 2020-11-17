DatabaseConnector
=================

[![Build Status](https://travis-ci.org/OHDSI/DatabaseConnector.svg?branch=master)](https://travis-ci.org/OHDSI/DatabaseConnector)
[![codecov.io](https://codecov.io/github/OHDSI/DatabaseConnector/coverage.svg?branch=master)](https://codecov.io/github/OHDSI/DatabaseConnector?branch=master)
[![CRAN_Status_Badge](http://www.r-pkg.org/badges/version/DatabaseConnector)](https://cran.r-project.org/package=DatabaseConnector)
[![CRAN_Status_Badge](http://cranlogs.r-pkg.org/badges/DatabaseConnector)](https://cran.r-project.org/package=DatabaseConnector)

DatabaseConnector is part of the [OHDSI Methods Library](https://ohdsi.github.io/MethodsLibrary).

Introduction
============
This R package provides function for connecting to various DBMSs. 

Features
========
- Create connections to the various database platforms:
  - MicrosoftSQL Server
  - Oracle
  - PostgresSql
  - Microsoft Parallel Data Warehouse (a.k.a. Analytics Platform System)
  - Amazon Redshift
  - Apache Impala
  - Google BigQuery
  - IBM Netezza
  - SQLite
- Statements for executing queries with 
  - Error reporting to file
  - Progress reporting
  - Multiple statements per query
- Support for fetching data to ffdf objects
- Insert data frame to a database table
- Supports the DBI interface
- Integrates with RStudio's Connections tab

Examples
========
```r
connectionDetails <- createConnectionDetails(dbms="postgresql", 
                                             server="localhost",
                                             user="root",
                                             password="blah",
                                             schema="cdm_v4")
conn <- connect(connectionDetails)
querySql(conn,"SELECT COUNT(*) FROM person")
disconnect(conn)
```

```r
## regular data insert
insertTable(connection = connection, 
            tableName = "scratch.somedata", 
            data = data, 
            dropTableIfExists = TRUE, 
            createTable = TRUE, 
            tempTable = FALSE, 
            useMppBulkLoad = FALSE)
            
## bulk data insert with Redshift or PDW
insertTable(connection = connection, 
            tableName = "scratch.somedata", 
            data = data, 
            dropTableIfExists = TRUE, 
            createTable = TRUE, 
            tempTable = FALSE, 
            useMppBulkLoad = TRUE)
```

Technology
============
DatabaseConnector is an R package using Java's JDBC drivers. 

System Requirements
===================
Requires R. Also requires Java 1.6 or higher (Oracle Java is recommended. [Issues](https://github.com/OHDSI/DatabaseConnector/issues/8) have been reported when using GCJ.) 

Dependencies
============
Please note that this package requires Java to be installed. If you don't have Java already installed on your computed (on most computers it already is installed), go to [java.com](http://java.com) to get the latest version.

To be able to use Windows authentication for SQL Server, you have to install the JDBC driver. Download the .exe from [Microsoft](http://www.microsoft.com/en-us/download/details.aspx?displaylang=en&id=11774) and run it, thereby extracting its contents to a folder. In the extracted folder you will find the file sqljdbc_4.0/enu/auth/x64/sqljdbc_auth.dll (64-bits) or sqljdbc_4.0/enu/auth/x86/sqljdbc_auth.dll (32-bits), which needs to be moved to location on the system path, for example to c:/windows/system32. If you not have write access to any folder in the system path, you can also specify the path to the folder containing the dll by setting the environmental variable `PATH_TO_AUTH_DLL`, so for example `Sys.setenv("PATH_TO_AUTH_DLL" = "c:/temp")`.

DatabaseConnector also depends on the OHDSI DatabaseConnectorJars and SqlRender packages.

For Redshift Bulk Mpp inserts, the cloudyR aws S3 package is required.

Getting Started
===============
To install the latest stable version, install from CRAN:

```r
install.packages("DatabaseConnector")
```
  
  
To install the latest development version, install from GitHub:

```r
install.packages("devtools")
devtools::install_github("ohdsi/DatabaseConnector")
```

To download and use the JDBC drivers for BigQuery, Impala, or Netezza, see [these instructions](http://ohdsi.github.io/DatabaseConnector/reference/jdbcDrivers.html).

User Documentation
==================
* Vignette: [Using DatabaseConnector](https://github.com/OHDSI/DatabaseConnector/raw/master/inst/doc/UsingDatabaseConnector.pdf)
* Package manual: [DatabaseConnector manual](https://raw.githubusercontent.com/OHDSI/DatabaseConnector/master/extras/DatabaseConnector.pdf) 

Support
=======
* Developer questions/comments/feedback: <a href="http://forums.ohdsi.org/c/developers">OHDSI Forum</a>
* We use the <a href="https://github.com/OHDSI/DatabaseConnector/issues">GitHub issue tracker</a> for all bugs/issues/enhancements

License
=======
DatabaseConnector is licensed under Apache License 2.0. The JDBC drivers [fall under their own respective licenses](https://raw.githubusercontent.com/OHDSI/DatabaseConnector/master/inst/COPYRIGHTS).

Development
===========
DatabaseConnector is being developed in R Studio.

### Development status

Stable. The code is actively being used in several projects.


# Acknowledgements
- This project is supported in part through the National Science Foundation grant IIS 1251151.

