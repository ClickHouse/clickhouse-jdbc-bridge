#!/bin/bash

set -e

start_server() {
	# change work directory explicitly
	cd $JDBC_BRIDGE_HOME

	if [ "$(echo ${CUSTOM_DRIVER_LOADER:="true"} | tr '[:upper:]' '[:lower:]')" != "true" ]; then
		local classpath="./clickhouse-jdbc-bridge-$JDBC_BRIDGE_VERSION.jar:$(echo $(ls ${DRIVER_DIR:="drivers"}/*.jar) | tr ' ' ':'):."
		java -XX:+UseContainerSupport -XX:+IdleTuningCompactOnIdle -XX:+IdleTuningGcOnIdle \
			-Xdump:none -Xdump:tool:events=systhrow+throw,filter=*OutOfMemoryError,exec="kill -9 %pid" \
			-Dlog4j.configuration=file:///$JDBC_BRIDGE_HOME/log4j.properties -Dnashorn.args=--language=es6 \
			${JDBC_BRIDGE_JVM_OPTS:=""} -cp $classpath ru.yandex.clickhouse.jdbcbridge.JdbcBridgeVerticle
	else
		java -XX:+UseContainerSupport -XX:+IdleTuningCompactOnIdle -XX:+IdleTuningGcOnIdle \
			-Xdump:none -Xdump:tool:events=systhrow+throw,filter=*OutOfMemoryError,exec="kill -9 %pid" \
			-Dlog4j.configuration=file:///$JDBC_BRIDGE_HOME/log4j.properties -Dnashorn.args=--language=es6 \
			${JDBC_BRIDGE_JVM_OPTS:=""} -jar clickhouse-jdbc-bridge-$JDBC_BRIDGE_VERSION.jar
	fi
}

if [ $# -eq 0 ]; then
	start_server
else
	exec "$@"
fi
