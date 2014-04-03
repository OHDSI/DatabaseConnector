\name{connect}
\alias{connect}
\title{connecte}
\usage{
connect(dbms = "mysql", user, password, server, port, schema)
}
\arguments{
  \item{dbms}{The type of DBMS running on the server. Valid
  values are \itemize{ \item{"mysql" for MySQL}
  \item{"oracle" for Oracle} \item{"postgresql" for
  PostgreSQL} \item{"sql server" for Microsoft SQL Server}
  }}

  \item{user}{The user name used to access the server. For
  Microsoft SQL Server, this parameter can be omitted to
  use Windows integrated security, which will require
  installation of the JDBC driver for SQL Server. When not
  using Windows integrated security, the domain can be
  specified using <mydomain>/<user>.}

  \item{password}{The password for that user}

  \item{server}{The name or IP-address of the server}

  \item{schema}{(optional) The name of the schema to
  connect to}
}
\value{
An object that extends \code{DBIConnection} in a
database-specific manner. This object is used to direct
commands to the database engine.
}
\description{
\code{connectToDatabase} creates a connection to a database
server.
}
\details{
This function loads the appropriate database driver, which
is included in the library, and connects to the database
server.
}
\examples{
conn <- connect(dbms="mysql", server="localhost",user="root",password="F1r3starter",schema="cdm_v4")
dbGetQuery(conn,"SELECT COUNT(*) FROM person")
dbDisconnect(conn)

conn <- connect(dbms="sql server", server="RNDUSRDHIT06.jnj.com",schema="Vocabulary")
dbGetQuery(conn,"SELECT COUNT(*) FROM concept")
dbDisconnect(conn)

conn <- connect(dbms="oracle", server="127.0.0.1:1521/xe",user="system",password="F1r3starter",schema="test")
dbGetQuery(conn,"SELECT COUNT(*) FROM test_table")
dbDisconnect(conn)
}

