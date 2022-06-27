#
# Copyright (C) 2019-2022, Zhichun Wu
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
FROM adoptopenjdk/openjdk8-openj9:jre8u322-b06_openj9-0.30.0-ubuntu

ARG revision=latest
ARG repository=ClickHouse/clickhouse-jdbc-bridge

# Maintainer
LABEL maintainer="zhicwu@gmail.com"

# Environment variables
ENV JDBC_BRIDGE_HOME=/app JDBC_BRIDGE_VERSION=${revision} \
	JDBC_BRIDGE_REL_URL=https://github.com/${repository}/releases/download

# Labels
LABEL app_name="ClickHouse JDBC Bridge" app_version="$JDBC_BRIDGE_VERSION"

# Update system and install additional packages for troubleshooting
RUN apt-get update \
	&& DEBIAN_FRONTEND=noninteractive apt-get install -y --allow-unauthenticated apache2-utils \
		apt-transport-https curl htop iftop iptraf iputils-ping jq lsof net-tools tzdata wget \
	&& apt-get clean \
	&& if [ "$revision" = "latest" ] ; then export JDBC_BRIDGE_VERSION=$(curl -s https://repo1.maven.org/maven2/ru/yandex/clickhouse/clickhouse-jdbc-bridge/maven-metadata.xml | grep '<latest>' | sed -e 's|^.*>\(.*\)<.*$|\1|'); else export JDBC_BRIDGE_VERSION=${revision}; fi \
	&& wget -P $JDBC_BRIDGE_HOME/drivers \
		https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.3.2-patch8/clickhouse-jdbc-0.3.2-patch8-all.jar \
		https://repo1.maven.org/maven2/org/mariadb/jdbc/mariadb-java-client/3.0.4/mariadb-java-client-3.0.4.jar \
		https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar \
		https://repo1.maven.org/maven2/org/neo4j/neo4j-jdbc-driver/4.0.5/neo4j-jdbc-driver-4.0.5.jar \
		https://repo1.maven.org/maven2/com/amazon/opendistroforelasticsearch/client/opendistro-sql-jdbc/1.13.0.0/opendistro-sql-jdbc-1.13.0.0.jar \
		https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.4/postgresql-42.3.4.jar \
		https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/3.36.0.3/sqlite-jdbc-3.36.0.3.jar \
		https://repo1.maven.org/maven2/io/trino/trino-jdbc/377/trino-jdbc-377.jar \
	&& export JDBC_BRIDGE_REL_URL=$JDBC_BRIDGE_REL_URL/v$JDBC_BRIDGE_VERSION \
	&& wget -q -P $JDBC_BRIDGE_HOME $JDBC_BRIDGE_REL_URL/LICENSE $JDBC_BRIDGE_REL_URL/NOTICE \
		$JDBC_BRIDGE_REL_URL/clickhouse-jdbc-bridge-${JDBC_BRIDGE_VERSION}-shaded.jar \
	&& ln -s $JDBC_BRIDGE_HOME/clickhouse-jdbc-bridge-${JDBC_BRIDGE_VERSION}-shaded.jar $JDBC_BRIDGE_HOME/clickhouse-jdbc-bridge-shaded.jar \
	&& rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY --chown=root:root docker/ $JDBC_BRIDGE_HOME

RUN chmod +x $JDBC_BRIDGE_HOME/*.sh \
	&& mkdir -p $JDBC_BRIDGE_HOME/logs /usr/local/lib/java \
	&& ln -s $JDBC_BRIDGE_HOME/logs /var/log/clickhouse-jdbc-bridge \
	&& ln -s $JDBC_BRIDGE_HOME/clickhouse-jdbc-bridge-${JDBC_BRIDGE_VERSION}-shaded.jar \
		/usr/local/lib/java/clickhouse-jdbc-bridge-shaded.jar \
	&& ln -s $JDBC_BRIDGE_HOME /etc/clickhouse-jdbc-bridge

WORKDIR $JDBC_BRIDGE_HOME

EXPOSE 9019

VOLUME ["${JDBC_BRIDGE_HOME}/drivers", "${JDBC_BRIDGE_HOME}/extensions", "${JDBC_BRIDGE_HOME}/logs", "${JDBC_BRIDGE_HOME}/scripts"]

CMD "./docker-entrypoint.sh"
