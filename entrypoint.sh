#!/usr/bin/env sh
java -jar clickhouse-jdbc-bridge-1.0.2.jar --driver-path /opt/jdbc-drivers --http-port "${BRIDGE_BIND_PORT}" --listen-host "${BRIDGE_BIND_HOST}" --log-level "${LOG_LEVEL}"