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
