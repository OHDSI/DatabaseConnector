DatabaseConnector 6.2.3
=======================

Changes: 

1. The `dbFetch()` function now respects `n = -1` and `n = Inf` arguments. Will throw warning if other value is used.

Bugfixes:

1. Fixing error about missing origin when fetching dates on older R versions.

2. Fixing RStudio connection panel information for DuckDB.


DatabaseConnector 6.2.2
=======================

Changes:

1. Changing heuristic for detecting when almost running out of Java heap.

2. Setting default `fetchRingBufferSize` for RedShift to 100MB (instead of 1GB) to preven Java out of heap errors, and overall better performance.

3. Using integers instead of strings to pass dates from Java to R for improved speed.

4. Using doubles instead of strings to pass datetimes from Java to R for improved speed.

Bugfixes:

1. Fixing connection issue for 'Hive'.


DatabaseConnector 6.2.1
=======================

Bugfixes:

1. Workaround for `rJava` and `rlang` interaction causing `no field, method or inner class called 'use_cli_format'`errors to be thrown when Java throws an error.


DatabaseConnector 6.2.0
=======================

Changes:

1. Can now use `downloadJdbcDrivers()` to download BigQuery drivers (directly from Google). 

2. Updating Spark JDBC driver to latest version.

Bugfixes:

1. Not asking to delete old files when not in interactive mode and calling `downloadJdbcDrivers()`.

2. Fixing error in RStudio table preview on Postgres when database name contains uppercase characters.

3. Several bugfixes for Spark.


DatabaseConnector 6.1.0
=======================

Changes:

1. Adding support for DuckDb

Bugfixes:

1. Fixed `capacity < 0` error message when using a large Java heap space.

2. Fixed 'optional feature not supported' error when connecting to DataBricks using JDBC.

3. Fixed `insertTable()` on Snowflake when data includes `POSIXct` type.

4. Fixed 'out of Java heap space' when fetching data with (large) strings. This is achieved by checking the available Java heap space at every 10,000 rows, stopping the batch when less than half is still available. Additionally, all strings from the previous batch are de-referenced before starting a new batch.

5. Fixing DATETIME shifts on Snowflake.

6. Fixing unit tests for upcoming Andromeda version (using arrow instead of SQLite).


DatabaseConnector 6.0.0
=======================

Changes:

1. Complete overhaul of DBI interface. DBI functions now apply translation to all incoming SQL, thus making the DatabaseConnector DBI interface an interface to a virtual database platform speaking OHDSISql as defined in `SqlRender`. This should aid write once - execute everywhere code development. It also allows using `dbplyr` with `DatabaseConnector`.

2. Adding `requiresTempEmulation()` and `assertTempEmulationSchemaSet()` functions.

3. The `dropEmulatedTempTables()` function can be called on all platforms (it doesn't do anything on platforms not requiring temp table emulation).

4. Ensuring `dbms()`, `insertTable()`, `renderTranslateExecuteSql()`, `renderTranslateQuerySql()`, and `renderTranslateQuerySqlToAndromeda()` functions work with connection pool objects.

5. Adding logging at 'TRACE' level for performance profiling using the `ParallelLogger` package. Use `options(LOG_DATABASECONNECTOR_SQL = TRUE)` to enable. The query times can be automatically extracted using the new `extractQueryTimes()` function.

6. Adding the `computeDataHash()` function.

7. The `inserTable()` function now converts logical fields to integer fields (and throws a warning if it does so).

8. The `dbListTables()` and `getTableNames()` functions return results in lowercase instead of uppercase for consistency with DBI functions. 

9. Throwing informative error if user calls `createConnectionDetails()` with credential arguments that cannot be evaluated in a multi-threading setting.

10. Supporting external tables on RedShift.

11. Adding support for ODBC connections to Spark.

12. Querying to Andromeda when using a DBI driver (instead of a JDBC driver) now also uses batching to avoid running out of memory.

13. Adding `appendToTable` argument to `querySqlToAndromeda()`, `renderTranslateQuerySqlToAndromeda()`, and `lowLevelQuerySqlToAndromeda()`.



Bugfixes:

1. Fix error when calling `insertTable()` and all column names require quotes.


DatabaseConnector 5.1.0
=======================

Changes:

1. Supporting upcoming version of `Andromeda` using `arrow` backend.

2. Adding new `dbms()` function, which is the preferred way to establish the dbms of a connection.

3. Adding support for Snowflake.

4. Updating RedShift driver to V2.1.0.9. Fixes error when uploading data.


Bugfixes:

1. Fix erroneous dates when inserting dates in a tibble into a SQLite database.

2. Ensuring errors when inserting data turn into R errors.

3. Throwing informative error when using `insertTable()` on BigQuery without specifying the `tempEmulationSchema`.


DatabaseConnector 5.0.4
=======================

Bugfixes:

1. Prevent error when `connectionString` is empty string (instead of `NULL`).


DatabaseConnector 5.0.3
=======================

Changes:

1. Ensuring Andromeda works with different backends (e.g. arrow).


Bugfixes:

1. Fixing 'DBMS not supported' error when connecting to Hive.

2. Fixing error when bulk uploading to Postgress with NULL values.

3. Fixing warning when automatically converting `Integer64` to `numeric` in R 4.2.0.


DatabaseConnector 5.0.2
=======================

Changes:

1. Updating Spark JDBC driver in response to Log4J vulnerability.

2. Using new Andromeda functions for better abstraction.

3. More informative error messages when user forgets to provide a connection property (like a password).

4. Also deprecating `oracleTempSchema` in DBI functions.

5. Adding `existsTable()` function.

6. Splitting vignette into two, because many users will only need to now how to connect to their database.

7. Improved error messaging related to the driver folder.

Bugfixes:

1. Fixing `getTableNames()` when the database or schema name contains escaped characters.


DatabaseConnector 5.0.1
=======================

Changes:

1. Added `dropEmulatedTempTables()` function.

Bugfixes:

1. Fixed R-Studio connection panel for BigQuery.


DatabaseConnector 5.0.0
=======================

Changes:

1. 32-bit integers are also converted to numeric by default because (a) the output of some SQL functions (e.g. DATEDIFF) is INT on some platforms, BIG_INT on others, and (b) dplyr often throws errors when working with both integer and numeric vectors.

2. Reusing byte buffer when passing 64-bit integers from Java to R for efficiency.

3. Adding support for connecting to Spark.

4. Adding `renderTranslateQueryApplyBatched()` function.

5. Throw informative error when provided `dbms` argument does not match any of the expected values.

6. Adding `getAvailableJavaHeapSpace()` function for debugging purposes.

Bugfixes: 

1. Fixing field type of numeric fields on Oracle when fetching data.

2. Fixing issues when fetching dates from SQLite (needed casting to numeric in some scenarios before conversion to date in R).

3. Fixing `insertTable()` when using a `databaseSchema` argument on SQLite.

4. Fixing `insertTable()` for inserting into temp table on RedShift when table name does not start with '#'.


DatabaseConnector 4.0.2
=======================

Bugfixes: 

1. Fixing handling of tibbles when inserting tables to avoid issues with date fields.


DatabaseConnector 4.0.1
=======================

Changes:

1. Using `SqlRender`'s new `tempEmulationSchema` argument. Avoids deprecation warning.

2. Throwing an informative error when the user points the `pathToDriver` argument to a file instead of a folder.


DatabaseConnector 4.0.0
=======================

Changes:

1. JDBC drivers are no longer pre-packaged in the `DatabaseConnectorJars` package. Instead, drivers need to be downloaded manually to a user-specified location in the local file system. The `downloadJdbcDrivers()` function has been added to make downing drivers easier. The path to the drivers can be set in the 'DATABASECONNECTOR_JAR_FOLDER' environmental variable.

2. Support for 64-bit integers using bit64's `integer64` type, although this is disabled by default because of the danger of silent errors when using `integer64` in R. Each query function has a `integer64AsNumeric` argument, and overall behavior can be set using`options(databaseConnectorInteger64AsNumeric = FALSE)`.

3. INT fields are now translated to R integers and back.

4. ConnectionDetails delays evaluation of sensitive arguments until needed for improved security.

5. Removing deprecated `schema` argument from `createConnectionDetails` and `connect`.

6. Deprecating `useMppBulkLoad` argument of `insertTable()` function in favor of `bulkLoad` argument.

7. Deprecating `oracleTempSchema` argument in various functions in favor of `tempEmulationSchema` argument, which can be set via the "sqlRenderTempEmulationSchema" option.

8. Adding bulk upload mode for PostgreSQL.

9. Batch mode in `executeSql` divides SQL into batches (1000 statements per batch) to avoid running our of Java heap memory.

10. Removed message that 'JDBC driver supports batch updates' when running executeSql in batch mode.

11. Added optional `databaseSchema` argument to `insertTable` for consistency with general OHDSI framework.


Bugfixes:

1. Fixed 'Warning: no non-missing arguments to max; returning -Inf' when a column only has NA values, or no values at all.

2. Fixed error when trying to insert data from a tibble.

3. Error report now includes all SQL in a batch when failing in batch mode.


DatabaseConnector 3.0.0
=======================

Changes:

1. Dropping support for orphaned ff package.

2. Adding support for Andromeda package to replace ff.

3. Error report file name now defaults to errorReportSql.txt to avoid confusion with other error reports.

4. Enforcing delay if about to exceed query quota on BigQuery.


DatabaseConnector 2.4.4
=======================

Changes:

1. Deprecating schema argument in connect and createConnectionDetails functions.

2. Checking table and column names for SQL reserved words when inserting a table.

3. Switching to Java 1.8 as minimum requirement.

4. Preparing for DatabaseConnectorJars v1.2.0, which will have JDBC v4.2 drivers (instead of JDBC v4).

Bugfixes:

1. Setting append = TRUE when not creating table in insertTable for RSQLite.

2. On RedShift, converting schema name to lowercase before getting table names to fix empty results.

3. Fixed insertTable on BigQuery.

4. Fixing autocommit not supported error on BigQuery.


DatabaseConnector 2.4.3
=======================

Changes:

1. Adding bulk upload for Hive.

Bugfixes:

2. Reducing batch size when performing CTAS hack on Hive to avoid error.


DatabaseConnector 2.4.2
=======================

Changes:

1. BigQuery insertTable now also uses CTAS hack for improved speed and avoiding insert size limitations.

Bugfixes:

1. Preventing scientific notation when bulk uploading to PDW to avoid error.

2. Fixing null error when calling getSchemaNames for BigQuery.


DatabaseConnector 2.4.1
=======================

Changes:

1. Adding ability to set the path where the DLL is located for Windows Integrated Authentication for SQL Server and PDW.

Bugfixes:

1. No longer dropping '#' prefix for temp table names when inserting data for RedShift.

DatabaseConnector 2.4.0
=======================

Changes:

1. Adding runAsBatch argument to executeSql and renderTranslateExecuteSql functions to allow running as a batch (often faster).

Bugfixes:

1. Not adding '#' prefix when performing insert into RedShift.

2. Disabling autocommit when sending updates to RedShift to prevent errors with new JDBC driver.

3. Preventing 'FeatureNotSupportedError' from terminating query on platforms that do no support autocommit.

4. Fixing some errors related to insertTable when determining correct field type for columns.

DatabaseConnector 2.3.0
=======================

Changes:

1. Adding support for SQLite through RSQLite (mainly for demonstration and testing)

2. Adding convenience functions renderTranslateExecuteSql, renderTranslateQuerySql, and renderTranslateQuerySql.ffdf

3. Dropping Starschema BigQuery driver (in favor of newer Simba driver)

4. Added support for inserting BIGINTs (large integers stored as numeric in R)

5. Applying CTAS hack to improve insertion performance for RedShift (was already used for PDW)

Bugfixes:

1. Executing multi-statement SQL where one statement returns results no longer causes error.


DatabaseConnector 2.2.1
=======================

Changes:

1. Now supporting proper insertion and extraction of DATETIME fields

Bugfixes:

1. Closing output stream when writing to zip file to avoid orphan file locks
2. Fixed the problem that Jar file is not detected when setting JDBC driver manually

DatabaseConnector 2.2.0
=======================

Changes:

1. Checking number of inserted rows after bulk upload, throwing error if not correct
2. Added convenience function for cross-platform zipping of files and folders


DatabaseConnector 2.1.4
=======================

Changes:

1. Faster inserts by building batches in Java instead of R


DatabaseConnector 2.1.3
=======================

Changes:

1. Updated to DBI specification 1.0
2. Defaulting connect arguments to NULL to prevent missing argument warnings in RStudio

Bugfixes:

1. Now generating unique display names for RStudio's Connections tab to prevent problems when opening two connections to the same server.

DatabaseConnector 2.1.2
=======================

Changes: initial submission to CRAN
