DatabaseConnector
=================

[![Build Status](https://github.com/OHDSI/DatabaseConnector/workflows/R-CMD-check/badge.svg)](https://github.com/OHDSI/DatabaseConnector/actions?query=workflow%3AR-CMD-check)
[![codecov.io](https://codecov.io/github/OHDSI/DatabaseConnector/coverage.svg?branch=main)](https://app.codecov.io/github/OHDSI/DatabaseConnector?branch=mai)
[![CRAN_Status_Badge](https://www.r-pkg.org/badges/version/DatabaseConnector)](https://cran.r-project.org/package=DatabaseConnector)
[![CRAN_Status_Badge](https://cranlogs.r-pkg.org/badges/DatabaseConnector)](https://cran.r-project.org/package=DatabaseConnector)

DatabaseConnector is part of [HADES](https://ohdsi.github.io/Hades/).


Introduction
============

This R package provides function for connecting to various DBMSs. Together with the `SqlRender` package, the main goal of `DatabaseConnector` is to provide a uniform interface across database platforms: the same code should run and produce equivalent results, regardless of the database back end.

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
  - Spark
- Statements for executing queries with 
  - Error reporting to file
  - Progress reporting
  - Multiple statements per query
- Support for fetching data to Andromeda objects
- Insert data frame to a database table
- Supports the `DBI` interface, with SQL statements automatically translated to the appropriate dialect.
- Supports the `dbplyr` interface.
- Integrates with RStudio's Connections tab


Examples
========

```r
connectionDetails <- createConnectionDetails(dbms="postgresql", 
                                             server="localhost",
                                             user="root",
                                             password="blah")
conn <- connect(connectionDetails)
querySql(conn,"SELECT COUNT(*) FROM person")
disconnect(conn)
```


Technology
============

DatabaseConnector is an R package using Java's JDBC drivers and other DBI drivers. 


System Requirements
===================

Running the package requires R with the package rJava installed. Also requires Java 1.8 or higher.


Installation
============

1. See the instructions [here](https://ohdsi.github.io/Hades/rSetup.html) for configuring your R environment, including Java.

2. To install the latest stable version, install from CRAN:

```r
install.packages("DatabaseConnector")
```

3. Download the database drivers as described [here](http://ohdsi.github.io/DatabaseConnector/articles/Connecting.html#obtaining-drivers).

4. (Optionally) To use Windows Authentication for SQL Server, download the authentication DDL file as described  [here](http://ohdsi.github.io/DatabaseConnector/reference/connect.html#windows-authentication-for-sql-server-1).

User Documentation
==================
Documentation can be found on the [package website](https://ohdsi.github.io/DatabaseConnector/).

PDF versions of the documentation are also available:

* Vignette: [Connecting to a database](https://github.com/OHDSI/DatabaseConnector/raw/main/inst/doc/Connecting.pdf)
* Vignette: [Querying a database](https://github.com/OHDSI/DatabaseConnector/raw/main/inst/doc/Querying.pdf)
* Vignette: [Using DatabaseConnector through DBI and dbplyr](https://github.com/OHDSI/DatabaseConnector/raw/main/inst/doc/DbiAndDbplyr.pdf)
* Package manual: [DatabaseConnector manual](https://raw.githubusercontent.com/OHDSI/DatabaseConnector/main/extras/DatabaseConnector.pdf) 


Support
=======

* Developer questions/comments/feedback: <a href="http://forums.ohdsi.org/c/developers">OHDSI Forum</a>
* We use the <a href="https://github.com/OHDSI/DatabaseConnector/issues">GitHub issue tracker</a> for all bugs/issues/enhancements


Contributing
============

Read [here](https://ohdsi.github.io/Hades/contribute.html) how you can contribute to this package.


License
=======

DatabaseConnector is licensed under Apache License 2.0. The JDBC drivers [fall under their own respective licenses](https://raw.githubusercontent.com/OHDSI/DatabaseConnector/main/inst/COPYRIGHTS).


Development
===========

DatabaseConnector is being developed in R Studio.


### Development status

Stable. The code is actively being used in several projects.


Acknowledgements
================

- This project is supported in part through the National Science Foundation grant IIS 1251151.
