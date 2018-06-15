Kerberos Support
=================
DatabaseConnector supports kerberos for Impala and Hive DBMSs.
Tested with Cloudera's CDH v5.7.0

Impala Examples
=================
To connect Impala service you should pass Kerberos details to the ImpalaJDBC driver using extraSettings parameter.

```
library(DatabaseConnector)
connectionDetails <- createConnectionDetails(dbms = "impala",
server = "127.0.0.1",
extraSettings = "AuthMech=1;KrbRealm=CLOUDERA;KrbHostFQDN=quickstart.cloudera;KrbServiceName=impala"
)

conn <- connect(connectionDetails)
result <- querySql(conn, "SELECT COUNT(*) FROM person")
print(result)
disconnect(conn)
```
Kerberos user will be readed from local Kerberos session.

To check if you are logged into the Kerberos run:
```
klist
```

To log into the Kerberos run:
```
kinit your-user@EXAMPLE.COM
```
or
```
kinit -k -t /path/to/file/your-user.keytab your-user@EXAMPLE.COM
```


More details about Impala's JDBC driver params you can read in the [Impala JDBC driver Documentation](https://www.cloudera.com/documentation/other/connectors/impala-jdbc/latest.html)

Hive Examples
=================
To connect Impala or Hive service using Hive-JDBC driver you should pass **kerberosAuthenticationDetails** with **connectionDetails**.

**'hive'** DBMS supports two options of Kerberos authorization.

**1.** Read logged user from local Kerberos session.
```
library(DatabaseConnector)
kerberosAuthenticationDetails <- createKerberosAuthenticationDetails()

connectionDetails <- createConnectionDetails(dbms="hive",
                                             server="127.0.0.1",
                                             port=21050, # impala port
                                             extraSettings="principal=impala/quickstart.cloudera@CLOUDERA",
                                             kerberosAuthenticationDetails=kerberosAuthenticationDetails
)

conn <- connect(connectionDetails)
result <- querySql(conn, "SELECT COUNT(*) FROM person")
print(result)
disconnect(conn)
```

**2.** Login user with keytab file
```
library(DatabaseConnector)
kerberosAuthenticationDetails <- createKerberosAuthenticationDetails(principal = "impala/quickstart.cloudera@CLOUDERA",
keytabFilePath = "/path/to/file/impala.keytab")

connectionDetails <- createConnectionDetails(dbms="hive",
                                             server="127.0.0.1",
                                             port=21050,
                                             extraSettings="principal=impala/quickstart.cloudera@CLOUDERA",
                                             kerberosAuthenticationDetails=kerberosAuthenticationDetails
)

conn <- connect(connectionDetails)
result <- querySql(conn, "SELECT COUNT(*) FROM person")
print(result)
disconnect(conn)
```
