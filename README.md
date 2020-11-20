# clickhouse-jdbc-bridge

JDBC bridge for ClickHouse. It acts as a stateless proxy passing queries to external datasources from ClickHouse. With this extension, you can run distributed query on ClickHouse across multiple datasources in real time.


## Overview

![Overview](https://user-images.githubusercontent.com/4270380/97794387-39e33200-1c34-11eb-819f-3a0fd097f6be.png)


## Known Issues / Limitation

**Caution: this is not ready for production use, as it's still under development and lack of testing.**

* ClickHouse server should be patched or you may run into issues like `JDBC bridge is not running` and `Timeout: connect timed out`
* Pushdown is not supported
* Query may execute twice because of type inferring
* Complex data types like Array and Tuple are not supported
* Mutation is not fully supported - only insertion in simple cases
* Scripting is experimental


## Performance

Below is a rough performance comparison to help you understand overhead caused by JDBC bridge. MariaDB, ClickHouse and JDBC bridge are running on separated KVMs. [JMeter](https://jmeter.apache.org/) is used to simulate 20 concurrent users to issue same query multiple times(100 - 10,000) via various JDBC drivers after warm-up. You may refer [this](misc/perf-test) to setup test environment run tests by yourself.

![Performance Comparison](https://user-images.githubusercontent.com/4270380/95696036-1861dc80-0c6c-11eb-9919-fa74c304daaf.png)


## Quick Start

* Java CLI

    ```bash
    java -jar clickhouse-jdbc-bridge-<version>.jar
    ```

* Docker CLI

    It's simple to get started using all-in-one docker image:
    ```bash
    # start container in background
    docker run --rm -d --name ch-server yandex/clickhouse-all-in-one

    # enter container to add datasource and issue query
    docker exec -it ch-server bash

    cp /etc/clickhouse-jdbc-bridge/config/datasources/datasource.json.example \
        /etc/clickhouse-jdbc-bridge/config/datasources/ch-server.json
    
    # you're supposed to see "ch-server        1" returned from ClickHouse
    clickhouse-client --query="select * from jdbc('self?datasource_column', 'select 1')"
    ```

    Alternatively, if you prefer the hard way ;)
    ```bash
    # create a network for ClickHouse and JDBC brigde, so that they can communicate with each other
    docker network create ch-net --attachable
    # start the two containers
    docker run --rm -d --network ch-net --name jdbc-bridge --hostname jdbc-bridge yandex/clickhouse-jdbc-bridge
    docker run --rm -d --network ch-net --name ch-server --hostname ch-server \
        --entrypoint /bin/bash yandex/clickhouse-server -c \
        "echo '<yandex><jdbc_bridge><host>jdbc-bridge</host><port>9019</port></jdbc_bridge></yandex>' \
            > /etc/clickhouse-server/config.d/jdbc_bridge_config.xml && /entrypoint.sh"
    # add named datasource and query
    docker exec -it jdbc-bridge cp /app/config/datasources/datasource.json.example \
        /app/config/datasources/ch-server.json
    docker exec -it jdbc-bridge cp /app/config/queries/query.json.example \
        /app/config/queries/show-query-logs.json
    # issue below query, and you'll see "ch-server        1" returned from ClickHouse
    docker exec -it ch-server clickhouse-client \
        --query="select * from jdbc('self?datasource_column', 'select 1')"
    ```

* Docker Compose

    ```bash
    git clone https://github.com/ClickHouse/clickhouse-jdbc-bridge.git
    cd clickhouse-jdbc-bridge/misc/quick-start
    docker-compose up -d
    ...
    docker-compose ps

               Name                         Command               State              Ports
    --------------------------------------------------------------------------------------------------
    quick-start_ch-server_1         /entrypoint.sh                   Up      8123/tcp, 9000/tcp, 9009/tcp
    quick-start_db-mariadb10_1      docker-entrypoint.sh mysqld      Up      3306/tcp
    quick-start_db-mysql5_1         docker-entrypoint.sh mysqld      Up      3306/tcp, 33060/tcp
    quick-start_db-mysql8_1         docker-entrypoint.sh mysqld      Up      3306/tcp, 33060/tcp
    quick-start_db-postgres13_1     docker-entrypoint.sh postgres    Up      5432/tcp
    quick-start_jdbc-bridge_1       /sbin/my_init                    Up      9019/tcp

    # issue below query, and you'll see "ch-server        1" returned
    docker-compose run ch-server clickhouse-client --query="select * from jdbc('self?datasource_column', 'select 1')"
    ```


## Usage

* Data Source
    ```sql
    -- show datasources and usage
    select * from jdbc('', 'show datasources')
    -- access named datasource
    select * from jdbc('ch-server', 'select 1')
    -- Besides named datasource, you can also use JDBC connection string.
    -- However it's not recommended for security reason, and it does not work by default.
    -- In order to use it, you'll have to explicitly set environment variable CUSTOM_DRIVER_LOADER
    -- or Java system property jdbc-bridge.driver.loader to false
    select *
    from jdbc('jdbc:clickhouse://localhost:8123/system?compress=false&ssl=false&user=default', 'select 1')
    ```

* Adhoc Query
    ```sql
    select * from jdbc('ch-server', 'system', 'select * from query_log where user != ''default''')

    select * from jdbc('ch-server', 'select * from query_log where user != ''default''')

    select * from jdbc('ch-server', 'select * from system.query_log where user != ''default''')
    ```

* Table Query
    ```sql
    select * from jdbc('ch-server', 'system', 'query_log')

    select * from jdbc('ch-server', 'query_log')
    ```

* Saved Query
    ```sql
    select * from jdbc('ch-server', 'scripts/show-query-logs.sql')
    ```

* Named Query
    ```sql
    select * from jdbc('ch-server', 'show-query-logs')
    ```

* Scripting
    ```sql
    select * from jdbc('script', '[1,2,3]')

    select * from jdbc('script', 'js', '[1,2,3]')

    select * from jdbc('script', 'scripts/one-two-three.js')
    ```

* Schema-based Query

    **Not implemented yet :p**

* Query Parameters
    ```sql
    select *
    from jdbc('ch-server?datasource_column&max_rows=1&fetch_size=1&one=1&two=2',
        'select {{one}} union all select {{ two }}')
    ```
    Query result:
    ```bash
    ┌─datasource─┬─1─┐
    │ ch-server  │ 1 │
    └────────────┴───┘
    ```

* JDBC Table
    ```sql
    drop table if exists system.test;
    create table system.test (
        a String,
        b UInt8
    ) engine=JDBC('ch-server', '', 'select user as a, is_initial_query as b from system.processes');
    ```

* JDBC Dictionary
    ```sql
    drop dictionary if exists system.dict_test;
    create dictionary system.dict_test
    (
        b UInt64 DEFAULT 0,
        a String
    ) primary key b
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'test' DB 'system'))
    LIFETIME(MIN 82800 MAX 86400)
    LAYOUT(FLAT());
    ```

* Mutation
    ```sql
    drop table if exists system.test_table;
    create table system.test_table (
        a String,
        b UInt8
    ) engine=Memory();

    drop table if exists system.jdbc_table;
    create table system.jdbc_table (
        a String,
        b UInt8
    ) engine=JDBC('ch-server', 'system', 'test_table');

    insert into system.jdbc_table(a, b) values('a', 1);

    select * from system.test_table;
    ```
    Query result:
    ```bash
    ┌─a─┬─b─┐
    │ a │ 1 │
    └───┴───┘
    ```

* Monitoring

    You can use [Prometheus](https://prometheus.io/) to monitor metrics exposed by JDBC brige.
    ```bash
    curl -v http://jdbc-bridge:9019/metrics
    ```


## Configuration

* JDBC Driver

    By default, all JDBC drivers should be placed under `drivers` directory. You can override that by customizing `driverUrls` in datasource configuration file. For example:
    ```json
    {
        "testdb": {
            "driverUrls": [
                "drivers/mariadb",
                "D:\\drivers\\mariadb",
                "/mnt/d/drivers/mariadb",
                "https://repo1.maven.org/maven2/org/mariadb/jdbc/mariadb-java-client/2.7.0/mariadb-java-client-2.7.0.jar"
            ],
            "driverClassName": "org.mariadb.jdbc.Driver",
            ...
        }
    }
    ```

* Named Data Source

    By default, named datasource is defined in configuration file in JSON format under `config/datasources` directory. You may check examples at [misc/quick-start/jdbc-bridge/config/datasources](misc/quick-start/jdbc-bridge/config/datasources). If you use modern editors like VSCode, you may find it's helpful to use [JSON schema](docker/config/datasource-schema.json) for validation and auto-complete.

* Saved Query

    Saved queries are under `scripts` directory by default. For example: [show-query-logs.sql](docker/scripts/show-query-logs.sql).

* Named Query

    Similar as named datasource, named queries are JSON configuration files under `config/queries`. You may refer to examples at [misc/quick-start/jdbc-bridge/config/datasources](misc/quick-start/jdbc-bridge/config/queries).

* Logging

    You can customize logging configuration in [log4j.properties](docker/log4j.properties).

* Vert.x

    If you're familiar with [Vert.x](https://vertx.io/). You can customize its configuration by changing `config/httpd.json` and `config/vertx.json`.

* Query Parameters

    All supported query parameters can be found at [here](src/main/java/ru/yandex/clickhouse/jdbcbridge/core/QueryPamraters.java). `datasource_column=true` can be simplied as `datasource_column`, for example:
    ```sql
    select * from jdbc('ch-server?datasource_column=true', 'select 1')

    select * from jdbc('ch-server?datasource_column', 'select 1')
    ```

* Timeout

    Couple of timeout settings you should be aware of:
    1. datasource timeout, for example: `max_execution_time` in MySQL and ClickHouse
    2. JDBC driver timeout, for example: `connectTimeout` and `socketTimeout` in [MySQL Connector/J](https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-configuration-properties.html)
    3. Vertx timeout - see `config/server.json` and `config/vertx.json`
    4. Client(ClickHouse JDBC driver) timeout - see timeout settings in ClickHouse JDBC driver


## Migration

* Upgrade to 2.x

    2.x is a complete re-write not fully compatible with older version. You'll have to re-define your datasources and update your queries accordingly.


## Build

You can use Maven to build ClickHouse JDBC bridge, for examples:
```bash
git clone https://github.com/ClickHouse/clickhouse-jdbc-bridge.git
cd clickhouse-jdbc-bridge
# compile and run unit tests
mvn clean test
# release shaded jar, rpm and debian packages
mvn -Prelease -Drevision=2.0.0 clean package
```

In order to build docker images:
```bash
git clone https://github.com/ClickHouse/clickhouse-jdbc-bridge.git
cd clickhouse-jdbc-bridge
docker build --squash --build-arg revision=2.0.0 -t yandex/clickhouse-jdbc-bridge .
# or if you want to build the all-ine-one image
docker build --squash --build-arg revision=20.9.3 -f all-in-one.Dockerfile -t yandex/clickhouse-all-in-one .
```

## Develop

JDBC bridge is extensible. You may take [ConfigDataSource](src/main/java/ru/yandex/clickhouse/jdbcbridge/impl/ConfigDataSource.java) and [ScriptDataSource](src/main/java/ru/yandex/clickhouse/jdbcbridge/impl/ScriptDataSource.java) as examples to create your own extension.

An extension for JDBC bridge is basically a Java class with 3 optional parts:

1. Extension Name

    By default, extension class name will be treated as name for the extension. However, you can declare a static member in your extension class to override that, for instance:
    ```java
    public static final String EXTENSION_NAME = "myExtension";
    ```

2. Initialize Method

    Initialize method will be called once and only once at the time when loading your extension, for example:
    ```java
    public static void initialize(ExtensionManager manager) {
        ...
    }
    ```

3. Instantiation Method
    In order to create instance of your extension, in general you should define a static method like below so that JDBC bridge knows how(besides walking through all possible constructors):
    ```java
    public static MyExtension newInstance(Object... args) {
        ...
    }
    ```

Assume your extension class is `com.mycompany.MyExtension`, you can load it into JDBC bridge by:

* put your extension package(e.g. my-extension.jar) and required dependencies under `extensions` directory

* update `server.json` by adding your extension, for example
```json
    ...
    "extensions": [
        ...
        {
            "class": "com.mycompany.MyExtension"
        }
    ]
    ...
```
Note: order of the extension matters. The first `NamedDataSource` extension will be set as default for all named datasources.


## FAQ

Coming soon
