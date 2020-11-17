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

# docker build --squash --build-arg revision=2.0.0 -t yandex/clickhouse-jdbc-bridge .
ARG revision=2.0.0-SNAPSHOT

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

RUN mvn -Drevision=${revision} package


#
# Stage 2/2: Pack
#
FROM adoptopenjdk/openjdk8-openj9:jre8u265-b01_openj9-0.21.0-ubuntu

ARG revision

# Maintainer
LABEL maintainer="zhicwu@gmail.com"

# Environment variables
ENV JDBC_BRIDGE_HOME=/app JDBC_BRIDGE_VERSION=${revision}

# Labels
LABEL app_name="ClickHouse JDBC Bridge" app_version="$JDBC_BRIDGE_VERSION"

# Update system and install additional packages for troubleshooting
RUN apt-get update \
	&& DEBIAN_FRONTEND=noninteractive apt-get install -y --allow-unauthenticated apache2-utils \
		apt-transport-https curl htop iftop iptraf iputils-ping jq lsof net-tools tzdata wget \
	&& apt-get clean \
	&& rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY --from=builder /app/LICENSE /app/NOTICE \
	/app/target/clickhouse-jdbc-bridge-$JDBC_BRIDGE_VERSION.jar $JDBC_BRIDGE_HOME/
COPY --chown=root:root docker/ $JDBC_BRIDGE_HOME

RUN chmod +x $JDBC_BRIDGE_HOME/*.sh \
	&& mkdir -p $JDBC_BRIDGE_HOME/logs /usr/local/lib/java \
	&& ln -s $JDBC_BRIDGE_HOME/logs /var/log/clickhouse-jdbc-bridge \
	&& ln -s $JDBC_BRIDGE_HOME/clickhouse-jdbc-bridge-$JDBC_BRIDGE_VERSION.jar \
		/usr/local/lib/java/clickhouse-jdbc-bridge.jar \
	&& ln -s $JDBC_BRIDGE_HOME /etc/clickhouse-jdbc-bridge

WORKDIR $JDBC_BRIDGE_HOME

EXPOSE 9019

VOLUME ["${JDBC_BRIDGE_HOME}/drivers", "${JDBC_BRIDGE_HOME}/extensions", "${JDBC_BRIDGE_HOME}/logs", "${JDBC_BRIDGE_HOME}/scripts"]

CMD "./docker-entrypoint.sh"

HEALTHCHECK --start-period=5m --interval=30s --timeout=5s \
	CMD curl --connect-timeout 3 --no-keepalive -f http://localhost:9019/ping || exit 1
