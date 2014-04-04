\name{createConnectionDetails}
\alias{createConnectionDetails}
\title{createConnectionDetails}
\usage{
createConnectionDetails(dbms = "mysql", user, password, server, port, schema)
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

  \item{port}{(optional) The port on the server to connect
  to}

  \item{schema}{(optional) The name of the schema to
  connect to}
}
\value{
A list with all the details needed to connect to a
database.
}
\description{
\code{createConnectionDetails} creates a list containing
all details needed to connect to a database.
}
\details{
This function creates a list containing all details needed
to connect to a database. The list can then be used in the
\code{connectUsingConnectionDetails} function.
}
\examples{
\dontrun{
  connectionDetails <- createConnectionDetails(dbms="mysql", server="localhost",user="root",password="F1r3starter",schema="cdm_v4")
  conn <- connectUsingConnectionDetails(connectionDetails)
  dbGetQuery(conn,"SELECT COUNT(*) FROM person")
  dbDisconnect(conn)
}
}

