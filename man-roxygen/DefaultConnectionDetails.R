#' @param user               The user name used to access the server.
#' @param password           The password for that user.
#' @param server             The name of the server.
#' @param port               (optional) The port on the server to connect to.
#' @param extraSettings      (optional) Additional configuration settings specific to the database
#'                           provider to configure things as security for SSL. For connections using 
#'                           JDBC these will be appended to end of the connection string. For 
#'                           connections using DBI, these settings will additionally be used to call
#'                           [dbConnect()].
#' @param oracleDriver       Specify which Oracle drive you want to use. Choose between `"thin"`
#'                           or `"oci"`.
#' @param connectionString   The JDBC connection string. If specified, the `server`, `port`,
#'                           `extraSettings`, and `oracleDriver` fields are ignored. If
#'                           `user` and `password` are not specified, they are assumed to
#'                           already be included in the connection string.
#' @param pathToDriver       Path to a folder containing the JDBC driver JAR files. See
#'                           [downloadJdbcDrivers()] for instructions on how to download the
#'                           relevant drivers.
#'
#' @description 
#' ## DBMS parameter details:
#'
#' Depending on the DBMS, the function arguments have slightly different
#' interpretations: 
#' 
#' Oracle:
#' 
#'  - `user`. The user name used to access the server
#'  - `password`. The password for that user
#'  - `server`. This field contains the SID, or host and servicename, SID, or TNSName:
#'         'sid', 'host/sid', 'host/service name', or 'tnsname'
#'  - `port`. Specifies the port on the server (default = 1521)
#'  - `extraSettings`. The configuration settings for the connection (i.e. SSL Settings such
#'         as "(PROTOCOL=tcps)")
#'  - `oracleDriver`. The driver to be used. Choose between "thin" or "oci".
#'  - `pathToDriver`. The path to the folder containing the Oracle JDBC driver JAR files.
#' 
#' Microsoft SQL Server:
#' 
#'  - `user`. The user used to log in to the server. If the user is not specified, Windows
#'         Integrated Security will be used, which requires the SQL Server JDBC drivers to be installed
#'         (see details below).
#'  - `password`. The password used to log on to the server
#'  - `server`. This field contains the host name of the server
#'  - `port`. Not used for SQL Server
#'  - `extraSettings`. The configuration settings for the connection (i.e. SSL Settings such
#'         as "encrypt=true; trustServerCertificate=false;")
#'  - `pathToDriver`. The path to the folder containing the SQL Server JDBC driver JAR files.
#' 
#' Microsoft PDW:
#' 
#'  - `user`. The user used to log in to the server. If the user is not specified, Windows
#'         Integrated Security will be used, which requires the SQL Server JDBC drivers to be installed
#'         (see details below).
#'  - `password`. The password used to log on to the server
#'  - `server`. This field contains the host name of the server
#'  - `port`. Not used for SQL Server
#'  - `extraSettings`. The configuration settings for the connection (i.e. SSL Settings such
#'         as "encrypt=true; trustServerCertificate=false;")
#'  - `pathToDriver`. The path to the folder containing the SQL Server JDBC driver JAR files.
#' 
#' PostgreSQL:
#' 
#'  - `user`. The user used to log in to the server
#'  - `password`. The password used to log on to the server
#'  - `server`. This field contains the host name of the server and the database holding the
#'         relevant schemas: host/database
#'  - `port`. Specifies the port on the server (default = 5432)
#'  - `extraSettings`. The configuration settings for the connection (i.e. SSL Settings such
#'         as "ssl=true")
#'  - `pathToDriver`. The path to the folder containing the PostgreSQL JDBC driver JAR files.
#' 
#' Redshift:
#' 
#'  - `user`. The user used to log in to the server
#'  - `password`. The password used to log on to the server
#'  - `server`. This field contains the host name of the server and the database holding the
#'         relevant schemas: host/database
#'  - `port`. Specifies the port on the server (default = 5439)
#'  - `extraSettings The configuration settings for the connection (i.e. SSL Settings such
#'         as "ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory")
#'  - `pathToDriver`. The path to the folder containing the RedShift JDBC driver JAR files.
#' 
#' Netezza:
#' 
#'  - `user`. The user used to log in to the server
#'  - `password`. The password used to log on to the server
#'  - `server`. This field contains the host name of the server and the database holding the
#'         relevant schemas: host/database
#'  - `port`. Specifies the port on the server (default = 5480)
#'  - `extraSettings`. The configuration settings for the connection (i.e. SSL Settings such
#'         as "ssl=true")
#'  - `pathToDriver`. The path to the folder containing the Netezza JDBC driver JAR file
#'         (nzjdbc.jar).
#' 
#' Impala:
#' 
#'  - `user`. The user name used to access the server
#'  - `password`. The password for that user
#'  - `server`. The host name of the server
#'  - `port`. Specifies the port on the server (default = 21050)
#'  - `extraSettings`. The configuration settings for the connection (i.e. SSL Settings such
#'         as "SSLKeyStorePwd=*****")
#'  - `pathToDriver`. The path to the folder containing the Impala JDBC driver JAR files.
#' 
#' SQLite:
#' 
#'  - `server`. The path to the SQLIte file.
#' 
#' Spark / Databricks:
#' 
#' Currently both JDBC and ODBC connections are supported for Spark. Set the 
#' `connectionString` argument to use JDBC, otherwise ODBC is used:
#' 
#'  - `connectionString`. The JDBC connection string (e.g. something like
#'         'jdbc:databricks://my-org.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/abcde12345;').
#'  - `user`. The user name used to access the server. This can be set to 'token' when using a personal token (recommended).
#'  - `password`. The password for that user. This should be your personal token  when using a personal token (recommended).
#'  - `server`. The host name of the server (when using ODBC), e.g. 'my-org.cloud.databricks.com')
#'  - `port`. Specifies the port on the server (when using ODBC)
#'  - `extraSettings`. Additional settings for the ODBC connection, for example 
#'         `extraSettings = list(HTTPPath = "/sql/1.0/warehouses/abcde12345",
#'                               SSL = 1,
#'                               ThriftTransport = 2,
#'                               AuthMech = 3)`
#' 
#' Snowflake:
#' 
#'  - `connectionString`. The connection string (e.g. starting with
#'         'jdbc:snowflake://host:port/?db=database').
#'  - `user`. The user name used to access the server.
#'  - `password`. The password for that user.
#' 
#' InterSystems IRIS:
#'  - `connectionString`. The connection string (e.g. starting with
#'         'jdbc:iris://host:port/namespace'). Alternatively, you can provide
#'         values for `server` and `port`, in which case the default `USER` namespace
#'         is used to connect.
#'  - `user`. The user name used to access the server.
#'  - `password`. The password for that user.
#'  - `pathToDriver`. The path to the folder containing the InterSystems IRIS JDBC driver JAR file.
#'
#' ## Windows authentication for SQL Server:
#'
#' To be able to use Windows authentication for SQL Server (and PDW), you have to install the JDBC
#' driver. Download the version 9.2.0 .zip from [Microsoft](https://learn.microsoft.com/en-us/sql/connect/jdbc/release-notes-for-the-jdbc-driver?view=sql-server-ver15#92-releases)
#' and extract its contents to a folder. In the extracted folder you will find the file
#' sqljdbc_9.2/enu/auth/x64/mssql-jdbc_auth-9.2.0.x64.dll (64-bits) or
#' ssqljdbc_9.2/enu/auth/x86/mssql-jdbc_auth-9.2.0.x86.dll (32-bits), which needs to be moved to
#' location on the system path, for example to c:/windows/system32. If you not have write access to
#' any folder in the system path, you can also specify the path to the folder containing the dll by
#' setting the environmental variable PATH_TO_AUTH_DLL, so for example
#' `Sys.setenv("PATH_TO_AUTH_DLL" = "c:/temp")` Note that the environmental variable needs to be
#' set before calling [connect()] for the first time.
