\name{connect}
\alias{connect}
\title{connect}
\usage{
connect(dbms = "sql server", user, password, server, port, schema)
connect(connectionDetails)
}
\arguments{
  \item{dbms}{The type of DBMS running on the server. Valid
  values are \itemize{ \item{"mysql" for MySQL}
  \item{"oracle" for Oracle} \item{"postgresql" for
  PostgreSQL} \item{"redshift" for Amazon Redshift}
  \item{"sql server" for Microsoft SQL Server} }}

  \item{user}{The user name used to access the server.}

  \item{password}{The password for that user}

  \item{server}{The name of the server}

  \item{port}{(optional) The port on the server to connect
  to}

  \item{schema}{(optional) The name of the schema to
  connect to}

  \item{connectionDetails}{an object of class
  \code{connectionDetails}}
}
\value{
An object that extends \code{DBIConnection} in a
database-specific manner. This object is used to direct
commands to the database engine.
}
\description{
\code{connect} creates a connection to a database server.
}
\details{
This function creates a list containing all details needed
to connect to a database. The list can then be used in the
\code{connect} function.

Depending on the DBMS, the function arguments have slightly
different interpretations:

MySQL: \itemize{ \item \code{user}. The user name used to
access the server \item \code{password}. The password for
that user \item \code{server}. The host name of the server
\item \code{port}. Specifies the port on the server
(default = 3306) \item \code{schema}. The database
containing the tables }

Oracle: \itemize{ \item \code{user}. The user name used to
access the server \item \code{password}. The password for
that user \item \code{server}. This field contains the SID,
or host and servicename or SID: '<sid>', '<host>/<sid>',
'<host>/<service name>' \item \code{port}. Specifies the
port on the server (default = 1521) \item \code{schema}.
This field contains the schema (i.e. 'user' in Oracle
terms) containing the tables } Microsoft SQL Server:
\itemize{ \item \code{user}. The user used to log in to the
server. If the user is not specified, Windows Integrated
Security will be used, which requires the SQL Server JDBC
drivers to be installed. Optionally, the domain can be
specified as <domain>/<user> (e.g. 'MyDomain/Joe') \item
\code{password}. The password used to log on to the server
\item \code{server}. This field contains the host name of
the server \item \code{port}. Not used for SQL Server \item
\code{schema}. The database containing the tables }

PostgreSQL: \itemize{ \item \code{user}. The user used to
log in to the server \item \code{password}. The password
used to log on to the server \item \code{server}. This
field contains the host name of the server and the database
holding the relevant schemas: <host>/<database> \item
\code{port}. Specifies the port on the server (default =
5432) \item \code{schema}. The schema containing the
tables. } Redshift: \itemize{ \item \code{user}. The user
used to log in to the server \item \code{password}. The
password used to log on to the server \item \code{server}.
This field contains the host name of the server and the
database holding the relevant schemas: <host>/<database>
\item \code{port}. Specifies the port on the server
(default = 5432) \item \code{schema}. The schema containing
the tables. }
}
\examples{
\dontrun{
  conn <- connect(dbms="mysql", server="localhost",user="root",password="xxx",schema="cdm_v4")
  dbGetQuery(conn,"SELECT COUNT(*) FROM person")
  dbDisconnect(conn)

  conn <- connect(dbms="sql server", server="RNDUSRDHIT06.jnj.com",schema="Vocabulary")
  dbGetQuery(conn,"SELECT COUNT(*) FROM concept")
  dbDisconnect(conn)

  conn <- connect(dbms="oracle", server="127.0.0.1/xe",user="system",password="xxx",schema="test")
  dbGetQuery(conn,"SELECT COUNT(*) FROM test_table")
  dbDisconnect(conn)
}
}

