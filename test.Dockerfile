#
# This dockerfile will create a deployable docker image out of a locally built jar, e.g.:
# `mvn -DskipTests clean package && docker build -f test.Dockerfile -t clickhouse-jdbc-bridge:$(git rev-parse --short head) .`
#
ARG revision=latest
ARG platform=linux/amd64
FROM --platform=$platform adoptopenjdk/openjdk8-openj9:jre8u322-b06_openj9-0.30.0-ubuntu

# Labels
LABEL app_name="ClickHouse JDBC Bridge"

# Environment variables
ENV JDBC_BRIDGE_HOME=/app
WORKDIR $JDBC_BRIDGE_HOME

COPY LICENSE NOTICE pom.xml ${JDBC_BRIDGE_HOME}/
COPY --chown=root:root docker/ $JDBC_BRIDGE_HOME/

# Update system and install additional packages for troubleshooting
RUN apt-get update \
	&& DEBIAN_FRONTEND=noninteractive apt-get install -qq --allow-unauthenticated apache2-utils \
		apt-transport-https curl htop iftop iptraf iputils-ping jq lsof net-tools tzdata wget \
	&& apt-get clean \
	&& wget -P $JDBC_BRIDGE_HOME/drivers \
		https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.3.2-patch8/clickhouse-jdbc-0.3.2-patch8-all.jar \
		https://repo1.maven.org/maven2/org/mariadb/jdbc/mariadb-java-client/3.0.4/mariadb-java-client-3.0.4.jar \
		https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar \
		https://repo1.maven.org/maven2/org/neo4j/neo4j-jdbc-driver/4.0.5/neo4j-jdbc-driver-4.0.5.jar \
		https://repo1.maven.org/maven2/com/amazon/opendistroforelasticsearch/client/opendistro-sql-jdbc/1.13.0.0/opendistro-sql-jdbc-1.13.0.0.jar \
		https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.4/postgresql-42.3.4.jar \
		https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/3.36.0.3/sqlite-jdbc-3.36.0.3.jar \
		https://repo1.maven.org/maven2/io/trino/trino-jdbc/377/trino-jdbc-377.jar \
	&& rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

ARG version=2.1.0-SNAPSHOT
COPY target/clickhouse-jdbc-bridge-${version}-shaded.jar /app/clickhouse-jdbc-bridge-shaded.jar

RUN chmod +x $JDBC_BRIDGE_HOME/*.sh \
	&& mkdir -p $JDBC_BRIDGE_HOME/logs /usr/local/lib/java \
	&& ln -s $JDBC_BRIDGE_HOME/logs /var/log/clickhouse-jdbc-bridge \
	&& ln -s $JDBC_BRIDGE_HOME /etc/clickhouse-jdbc-bridge \
  && ln -s $JDBC_BRIDGE_HOME/clickhouse-jdbc-bridge-shaded.jar /usr/local/lib/java/clickhouse-jdbc-bridge-shaded.jar

EXPOSE 9019

VOLUME ["${JDBC_BRIDGE_HOME}/drivers", "${JDBC_BRIDGE_HOME}/extensions", "${JDBC_BRIDGE_HOME}/logs", "${JDBC_BRIDGE_HOME}/scripts"]

CMD "./docker-entrypoint.sh"
