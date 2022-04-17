# ClickHouse JDBC Bridge Docker Image

## What is ClickHouse JDBC Bridge?
It's a stateless proxy passing queries from ClickHouse to external datasources. With this extension, you can use [JDBC table function](https://clickhouse.tech/docs/en/sql-reference/table-functions/jdbc/) and/or corresponding [table engine](https://clickhouse.tech/docs/en/engines/table-engines/integrations/jdbc/) on ClickHouse server to query arbitrary datasources in real time.

## Versions
The latest tag points to the latest release from `master` branch. Branch tags like `2.0` point to the latest release of the corresponding branch. Full version tags like `2.0.0` point to the corresponding release.

## How to use this image

### Start JDBC bridge
```bash
docker run -d --name ch-jdbc-bridge -p9019:9019 \
    -e MAVEN_REPO_URL="https://repo1.maven.org/maven2" \
    -e JDBC_DRIVERS="org/mariadb/jdbc/mariadb-java-client/2.7.4/mariadb-java-client-2.7.4.jar,org/postgresql/postgresql/42.2.24/postgresql-42.2.24.jar" clickhouse/jdbc-bridge
```
If you prefer to use JDBC drivers and named datasources on host, you can use the following commands:
```bash
wget -P drivers \
    https://repo1.maven.org/maven2/org/mariadb/jdbc/mariadb-java-client/2.7.4/mariadb-java-client-2.7.4.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.24/postgresql-42.2.24.jar
wget -P datasources \
    https://raw.githubusercontent.com/ClickHouse/clickhouse-jdbc-bridge/master/misc/quick-start/jdbc-bridge/config/datasources/mariadb10.json \
    https://raw.githubusercontent.com/ClickHouse/clickhouse-jdbc-bridge/master/misc/quick-start/jdbc-bridge/config/datasources/postgres13.json
# please edit datasources/*.json to connect to your database servers
docker run -d --name ch-jdbc-bridge -p9019:9019 -v `pwd`/drivers:/app/drivers \
    -v `pwd`/datasources:/app/config/datasources clickhouse/jdbc-bridge
```

### Configure ClickHouse server
By default, ClickHouse assumes JDBC bridge is available at `localhost:9019`. You can customize the host and port in `/etc/clickhouse-server/config.xml` like below:
```xml
<clickhouse>
    ...
    <jdbc_bridge>
        <host>localhost</host>
        <port>9019</port>
    </jdbc_bridge>
    ...
</clickhouse>
```

### Issue query on ClickHouse
Once you started JDBC bridge and configured ClickHouse server accordingly, you should be able to run queries like below on ClickHouse:
```sql
-- show all named datasources
select * from jdbc('', 'show datasources')
-- query against adhoc datasource, NOT recommended for security reason
select * from jdbc('jdbc:mariadb://localhost:3306/test?useSSL=false&user=root&password=root', 'select 1')
-- query against named datasource with inline schema and adhoc query
select * from jdbc('mariadb10', 'num UInt8', 'select 1 as num')
-- scripting
select * from jdbc('script', '[1,2,3]')
```

## Configuration

Container exposes 9019 port for ClickHouse integration and monitoring.

In order to customize the container, please refer to directory structure and supported environment variables shown below.
```bash
/app                        # work directory
  |
  |-- drivers               # JDBC drivers
  |-- config                
  |     |
  |     |-- datasources     # named datasources
  |     |-- schemas         # named schemas
  |     |-- queries         # named queries
  |
  |-- logs                  # application logs
  |-- scripts               # saved queries/scripts
```

Environment Variable | Java System Property | Default Value | Remark
-- | -- | -- | --
CONFIG_DIR | jdbc-bridge.config.dir | config | Configuration   directory
SERIAL_MODE | jdbc-bridge.serial.mode | false | Whether run query in serial mode or not
CUSTOM_DRIVER_LOADER | jdbc-bridge.driver.loader | true | Whether use custom driver class loader   or not
DATASOURCE_CONFIG_DIR | jdbc-bridge.datasource.config.dir | datasources | Directory   for named datasources
DEFAULT_VALUE | jdbc-bridge.type.default | false | Whether support default expression in   column definition or not
DRIVER_DIR | jdbc-bridge.driver.dir | drivers | Directory   for drivers needed to connect to datasources
HTTPD_CONFIG_FILE | jdbc-bridge.httpd.config.file | httpd.json | HTTP server configuration file
JDBC_BRIDGE_JVM_OPTS | - | - | JVM arguments
JDBC_DRIVERS | - | - | Comma separated JDBC driver download path on Maven repository
MAVEN_REPO_URL | - | https://repo1.maven.org/maven2 | Base URL of Maven repository
QUERY_CONFIG_DIR | jdbc-bridge.query.config.dir | queries | Directory   for named queries
SCHEMA_CONFIG_DIR | jdbc-bridge.schema.config.dir | schemas | Directory for named schemas
SERVER_CONFIG_FILE | jdbc-bridge.server.config.file | server.json | JDBC   bridge server configuration file
VERTX_CONFIG_FILE | jdbc-bridge.vertx.config.file | vertx.json | Vert.x configuration file


## License

View [license information](https://github.com/ClickHouse/clickhouse-jdbc-bridge/blob/master/LICENSE) for the software contained in this image.
