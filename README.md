ClickHouse JDBC bridge
=====

This is a JDBC bridge for ClickHouse. It stands for proxying SQL queries to external databases from ClickHouse.
Inside ClickHouse this functionality is used in following cases:

1. Table function jdbc('jdbc connection string', 'database', 'table')
2. Table engine JDBC('jdbc connection string', 'database', 'table')
3. (Not implemented yet) Dictionary source

## How it works?
### Overview
A bridge application starts, and looks for jar-files into given `--driver-path` directory.
Within each JAR file found, bridge will try to find implementations of `java.sql.Driver`, and make them 
available for usage. 
After driver is registered, and server started, you may send queries from ClickHouse:
```
select * from jdbc('jdbc:mysql://host.port/?user=user&password=passord', 'schema', 'table')
```
for convenience reasons, `jdbc:` part of connection string can be ommitted:
```
select * from jdbc('mysql://host.port/?user=user&password=passord', 'schema', 'table')
```

#### Named datasources
As we can see, exposing credentials in query is not very secure. 

To hide user/password, and/or to setup particular settings for JDBC driver, there is an "datasource" or "named connection" subsystem.
It is similar to ODBC - in separate file you have to specify settings, and give it an alias.
You may create a file, containing aliases for particular datasource:

 ```properties
# An example file with connections
# Format:
# datasource.$alias=[jdbc:]$DSN
datasource.mysql-localhost=mysql://localhost:3306/?user=root&password=root&useSSL=false
 ```
E.g. each connection alias starts with `connection.` keyword, then goes alias value, then - connection string.
When applied, you may use the following from ClickHouse:
```roomsql
SELECT * FROM jdbc('datasource://mysql-localhost', 'schema', 'table')
 ```
 
### Data types notes
Currently, bridge is able to map a limited subset of JDBC java.sql.Types into ClickHouse data types.
All the types supports nullability.
Native database unsigned types are not supported yet. E.g. they would be transformed to signed ClickHouse data types.
Table of conversion:

| JDBC data type | ClickHouse data type |
|----------------|----------------------|
| TINYINT        | Int8 |
| SMALLINT       | Int16 |
| INTEGER        | Int32 |
| BIGINT         | Int64 |
| FLOAT          | Float32 |
| REAL           | Float32 |
| DOUBLE         | Float64 |
| TIMESTAMP      | DateTime |
| TIME           | DateTime |
| DATE           | Date |
| BIT            | UInt8 |
| BOOLEAN        | UInt8 |
| CHAR           | String |
| VARCHAR        | String |
| LONGVARCHAR    | String |

## Building and installing bridge
Prerequisites:
1. JDK 1.8+
2. Maven

Building script:
```
mvn clean package
```

Once build finished, you'll find `target/jdbc.bridge-1.0.jar`, ready to work.

### Installation
```
sudo dpkg -i target/clickhouse-jdbc-bridge.deb
```
Start the service:
```
sudo service clickhouse-jdbc-bridge start
```
Configuration:
`/etc/clickhouse-jdbc-bridge/defaults` - main configuration params
`/etc/clickhouse-jdbc-bridge/datasources.properties` - list of datasources

## Testing
Bridge was tested against following JDBC drivers:
1. H2 (in-memory)
2. SQLite
3. MySQL
4. PostgreSQL

To launch the tests:
```
mvn test
```

By default, tests would be launched with H2 and SQLite (cause they does not require external service).
To launch with MySQL and/or PostgreSQL integration test, add following instructions:
```
mvn test \
-Ddatasource.mysql='jdbc:mysql://localhost:3306/test?user=root&password=root&useSSL=false&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC' \
-Ddatasource.postgresql='jdbc:postgresql://localhost:5432/test?user=root&password=root'
```

If you have `clickhouse-local` binary installed, you may specify it's full path, to launch validation of RowBinary protocol implementation:
```
mvn test -Dclickhouse.local.bin=/usr/bin/clickhouse-local
```

Without this option, all the tests would be marked incomplete.
## Running bridge
```
java -jar jdbc.bridge-1.0.jar --help

Usage: /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java -jar /home/krash/dev/java/clickhouse-jdbc-bridge/target/jdbc.bridge-1.0.jar [options]
  Options:
    --datasources
      File, containing specifications for connections
    --driver-path
      Path to directory, containing JDBC drivers
    --err-log-path
      Where to redirect STDERR
    --help
      Show help message
    --http-port
      Port to listen on
      Default: 9019
    --listen-host
      Host to listen on
      Default: localhost
    --log-level
      Log level
      Default: DEBUG
    --log-path
      Where to write logs
```
Options meaning:

`--datasources` a .properties file, containing so-called "aliases" for connections (see below)

`--driver-path` a path to a directory, containing jar-files with JDBC driver's implementation. Bridge will automatically 
find implementations of `java.sql.Driver` in each jar, and make it available for usage

`--err-log-path` path to file, where to redirect STDERR

`--http-port` a port to bind to

`--listen-host` a host to bind to

`--log-level` logging level (INFO, DEBUG, etc.)

`--log-path` where to write logs, produced by logging facility 

## Clickhouse to bringe port
By default, `clickhouse-jdbc-bridge` uses `localhost:9019`. You can specify the host and port in `config.xml` like this:

```xml
<jdbc_bridge>
    <host>127.0.0.1</host>
    <port>9119</port>
</jdbc_bridge>
```

