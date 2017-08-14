#' @param dbms               The type of DBMS running on the server. Valid values are
#'                           \itemize{
#'                             \item {"mysql" for MySQL}
#'                             \item {"oracle" for Oracle}
#'                             \item {"postgresql" for PostgreSQL}
#'                             \item {"redshift" for Amazon Redshift}
#'                             \item {"sql server" for Microsoft SQL Server}
#'                             \item {"pdw" for Microsoft Parallel Data Warehouse (PDW)}
#'                             \item {"netezza" for IBM Netezza}
#'                             \item {"bigquery" for Google BigQuery}
#'                           }
#'
#' @param user               The user name used to access the server.
#' @param domain             For SQL Server only: the Windows domain (optional).
#' @param password           The password for that user.
#' @param server             The name of the server.
#' @param port               (optional) The port on the server to connect to.
#' @param schema             (optional) The name of the schema to connect to.
#' @param extraSettings      (optional) Additional configuration settings specific to the database
#'                           provider to configure things as security for SSL. These must follow the
#'                           format for the JDBC connection for the RDBMS specified in dbms.
#' @param oracleDriver       Specify which Oracle drive you want to use. Choose between \code{"thin"}
#'                           or \code{"oci"}.
#' @param connectionString   The JDBC connection string. If specified, the \code{server}, \code{port},
#'                           \code{extraSettings}, and \code{oracleDriver} fields are ignored. If
#'                           \code{user} and \code{password} are not specified, they are assumed to
#'                           already be included in the connection string.
#' @param pathToDriver       Path to the JDBC driver JAR files. Currently only needed for Impala and
#'                           Netezza.
#'
#' @section
#' DBMS parameter details: Depending on the DBMS, the function arguments have slightly different
#' interpretations:
#' MySQL:
#' \itemize{
#'   \item \code{user}. The user name used to access the server
#'   \item \code{password}. The password for that user
#'   \item \code{server}. The host name of the server
#'   \item \code{port}. Specifies the port on the server (default = 3306)
#'   \item \code{schema}. The database containing the tables
#'   \item \code{extraSettings} The configuration settings for the connection (i.e. SSL Settings such
#'         as "SSL Mode=Required")
#' }
#' Oracle:
#' \itemize{
#'   \item \code{user}. The user name used to access the server
#'   \item \code{password}. The password for that user
#'   \item \code{server}. This field contains the SID, or host and servicename, SID, or TNSName:
#'         '<sid>', '<host>/<sid>', '<host>/<service name>', or '<tnsname>'
#'   \item \code{port}. Specifies the port on the server (default = 1521)
#'   \item \code{schema}. This field contains the schema (i.e. 'user' in Oracle terms) containing the
#'         tables
#'   \item \code{extraSettings} The configuration settings for the connection (i.e. SSL Settings such
#'         as "(PROTOCOL=tcps)")
#'   \item \code{oracleDriver} The driver to be used. Choose between "thin" or "oci".
#' }
#' Microsoft SQL Server:
#' \itemize{
#'   \item \code{user}. The user used to log in to the server. If the user is not specified, Windows
#'         Integrated Security will be used, which requires the SQL Server JDBC drivers to be installed
#'         (see details below).
#'   \item \code{domain}. Optionally, the domain can be specified here.(See note below).
#'   \item \code{password}. The password used to log on to the server
#'   \item \code{server}. This field contains the host name of the server
#'   \item \code{port}. Not used for SQL Server
#'   \item \code{schema}. The database containing the tables. If both database and schema are specified
#'         (e.g. 'my_database.dbo', then only the database part is used, the schema is ignored.
#'   \item \code{extraSettings} The configuration settings for the connection (i.e. SSL Settings such
#'         as "encrypt=true; trustServerCertificate=false;")
#' }
#' Microsoft PDW:
#' \itemize{
#'   \item \code{user}. The user used to log in to the server. If the user is not specified, Windows
#'         Integrated Security will be used, which requires the SQL Server JDBC drivers to be installed
#'         (see details below).
#'   \item \code{password}. The password used to log on to the server
#'   \item \code{server}. This field contains the host name of the server
#'   \item \code{port}. Not used for SQL Server
#'   \item \code{schema}. The database containing the tables
#'   \item \code{extraSettings} The configuration settings for the connection (i.e. SSL Settings such
#'         as "encrypt=true; trustServerCertificate=false;")
#' }
#' Connections where the domain need to be specified are not supported.
#' PostgreSQL:
#' \itemize{
#'   \item \code{user}. The user used to log in to the server
#'   \item \code{password}. The password used to log on to the server
#'   \item \code{server}. This field contains the host name of the server and the database holding the
#'         relevant schemas: <host>/<database>
#'   \item \code{port}. Specifies the port on the server (default = 5432)
#'   \item \code{schema}. The schema containing the tables.
#'   \item \code{extraSettings} The configuration settings for the connection (i.e. SSL Settings such
#'         as "ssl=true")
#' }
#' Redshift:
#' \itemize{
#'   \item \code{user}. The user used to log in to the server
#'   \item \code{password}. The password used to log on to the server
#'   \item \code{server}. This field contains the host name of the server and the database holding the
#'         relevant schemas: <host>/<database>
#'   \item \code{port}. Specifies the port on the server (default = 5439)
#'   \item \code{schema}. The schema containing the tables.
#'   \item \code{extraSettings} The configuration settings for the connection (i.e. SSL Settings such
#'         as "ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory")
#' }
#' Netezza:
#' \itemize{
#'   \item \code{user}. The user used to log in to the server
#'   \item \code{password}. The password used to log on to the server
#'   \item \code{server}. This field contains the host name of the server and the database holding the
#'         relevant schemas: <host>/<database>
#'   \item \code{port}. Specifies the port on the server (default = 5480)
#'   \item \code{schema}. The schema containing the tables.
#'   \item \code{extraSettings} The configuration settings for the connection (i.e. SSL Settings such
#'         as "ssl=true")
#'   \item \code{pathToDriver} The path to the folder containing the Netezza JDBC driver JAR file
#'         (nzjdbc.jar).
#' }
#' Impala:
#' \itemize{
#'   \item \code{user}. The user name used to access the server
#'   \item \code{password}. The password for that user
#'   \item \code{server}. The host name of the server
#'   \item \code{port}. Specifies the port on the server (default = 21050)
#'   \item \code{schema}. The database containing the tables
#'   \item \code{extraSettings} The configuration settings for the connection (i.e. SSL Settings such
#'         as "SSLKeyStorePwd=*****")
#'   \item \code{pathToDriver} The path to the folder containing the Impala JDBC driver JAR files.
#' }
#'
#' To be able to use Windows authentication for SQL Server (and PDW), you have to install the JDBC
#' driver. Download the .exe from
#' \href{http://www.microsoft.com/en-us/download/details.aspx?displaylang=en&id=11774}{Microsoft} and
#' run it, thereby extracting its contents to a folder. In the extracted folder you will find the file
#' sqljdbc_4.0/enu/auth/x64/sqljdbc_auth.dll (64-bits) or sqljdbc_4.0/enu/auth/x86/sqljdbc_auth.dll
#' (32-bits), which needs to be moved to location on the system path, for example to
#' c:/windows/system32. When using a Windows domain to log in to SQL Server, DatabaseConnector must
#' rely on a non-Microsoft driver. This driver has know issues with retrieving dates. We therefor
#' recommend to either use Windows integrated security, or if a different user is needed, try running
#' RStudio using that user: \code{runas /netonly /user:domain\\username
#' "C:\path\to\rstudio\bin\rstudio.exe"}.
