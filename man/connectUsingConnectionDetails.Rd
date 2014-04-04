\name{connectUsingConnectionDetails}
\alias{connectUsingConnectionDetails}
\title{connectUsingConnectionDetails}
\usage{
connectUsingConnectionDetails(connectionDetails)
}
\arguments{
  \item{connectionDetails}{a list created by the
  \code{createConnectionDetails} function.}
}
\value{
An object that extends \code{DBIConnection} in a
database-specific manner. This object is used to direct
commands to the database engine.
}
\description{
\code{connectUsingConnectionDetails} connects to a database
given the provided details.
}
\details{
This function connects to a database given the provided
details. The details should be in a list created by
\code{createConnectionDetails}.
}
\examples{
\dontrun{
  connectionDetails <- createConnectionDetails(dbms="mysql", server="localhost",user="root",password="xxx",schema="cdm_v4")
  conn <- connectUsingConnectionDetails(connectionDetails)
  dbGetQuery(conn,"SELECT COUNT(*) FROM person")
  dbDisconnect(conn)
}
}

