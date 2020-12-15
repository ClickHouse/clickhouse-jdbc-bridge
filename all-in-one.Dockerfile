#
# Copyright (C) 2019-2020, Zhichun Wu
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

# docker build --squash --build-arg revision=20.8 -f all-in-one.Dockerfile -t yandex/clickhouse-all-in-one:20.8 .
ARG revision=20.8

#
# Stage 1/2: Build
#
FROM maven:3-openjdk-8 as builder

ARG revision

COPY LICENSE NOTICE pom.xml /app/
COPY docker /app/docker/
COPY misc /app/misc/
COPY src /app/src/

WORKDIR /app

RUN apt-get update \
	&& apt-get install -y --no-install-recommends dpkg fakeroot rpm \
	&& mvn -Prelease -Drevision=${revision} package


#
# Stage 2/2: Pack
#
FROM yandex/clickhouse-server:${revision}

# Maintainer
LABEL maintainer="zhicwu@gmail.com"

COPY --from=builder /app/target/clickhouse-jdbc-bridge*.deb /

# DEBIAN_FRONTEND=noninteractive 
RUN apt-get update \
	&& apt-get install -y --no-install-recommends --allow-unauthenticated \
		apache2-utils apt-transport-https curl htop iftop iptraf iputils-ping jq lsof net-tools tzdata wget \
	&& apt-get install -y --no-install-recommends /*.deb \
	&& apt-get clean \
	&& rm -rf /*.deb /var/lib/apt/lists/* /tmp/* /var/tmp/* \
	&& wget -q -P /etc/clickhouse-jdbc-bridge/drivers/clickhouse \
		https://repo1.maven.org/maven2/net/jpountz/lz4/lz4/1.3.0/lz4-1.3.0.jar \
		https://repo1.maven.org/maven2/org/slf4j/slf4j-api/1.7.30/slf4j-api-1.7.30.jar \
		https://repo1.maven.org/maven2/ru/yandex/clickhouse/clickhouse-jdbc/0.2.4/clickhouse-jdbc-0.2.4-shaded.jar \
	&& wget -q -P /etc/clickhouse-jdbc-bridge/drivers/mariadb \
		https://repo1.maven.org/maven2/org/mariadb/jdbc/mariadb-java-client/2.7.0/mariadb-java-client-2.7.0.jar \
	&& wget -q -P /etc/clickhouse-jdbc-bridge/drivers/mysql5 \
		https://repo1.maven.org/maven2/mysql/mysql-connector-java/5.1.49/mysql-connector-java-5.1.49.jar \
	&& wget -q -P /etc/clickhouse-jdbc-bridge/drivers/mysql8 \
		https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.21/mysql-connector-java-8.0.21.jar \
	&& wget -q -P /etc/clickhouse-jdbc-bridge/drivers/postgres \
		https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.17/postgresql-42.2.17.jar \
	&& sed -i -e 's|\(^[[:space:]]*\)\(exec.*clickhouse-server.*$\)|\1exec clickhouse-jdbc-bridge\&\n\1\2|' /entrypoint.sh \
	&& echo '{\n\
  "$schema": "../datasource-schema.json",\n\
  "self": {\n\
    "driverUrls": [ "drivers/clickhouse" ],\n\
    "driverClassName": "ru.yandex.clickhouse.ClickHouseDriver",\n\
    "jdbcUrl": "jdbc:clickhouse://localhost:8123/system?ssl=false",\n\
    "username": "default",\n\
    "password": "",\n\
    "maximumPoolSize": 5\n\
  }\n\
}' > /etc/clickhouse-jdbc-bridge/config/datasources/self.json
