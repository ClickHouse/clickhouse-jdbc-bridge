#!/bin/bash

set -e

start_server() {
	local base_url="${MAVEN_REPO_URL:="https://repo1.maven.org/maven2"}"
	local driver_dir="$JDBC_BRIDGE_HOME/drivers"
	local jdbc_drivers="${JDBC_DRIVERS:=""}"

	# change work directory explicitly
	cd $JDBC_BRIDGE_HOME

	if [ "$jdbc_drivers" != "" ] && [ "$(ls -A $driver_dir)" == "" ]; then
		echo "Downloading JDBC drivers to directory [$driver_dir]..."
		for i in $(echo "$jdbc_drivers" | sed "s/,/ /g"); do
			if [ "$i" != "" ]; then
				echo "  => [$base_url/$i]..."
				wget -q -P "$driver_dir" "$base_url/$i"
			fi
		done
	fi

	if [ "$(echo ${CUSTOM_DRIVER_LOADER:="true"} | tr '[:upper:]' '[:lower:]')" != "true" ]; then
		local classpath="./clickhouse-jdbc-bridge-shaded.jar:$(echo $(ls ${DRIVER_DIR:="drivers"}/*.jar) | tr ' ' ':'):."
		java -XX:+UseContainerSupport -XX:+IdleTuningCompactOnIdle -XX:+IdleTuningGcOnIdle \
			-Xdump:none -Xdump:tool:events=systhrow+throw,filter=*OutOfMemoryError,exec="kill -9 %pid" \
			-Djava.util.logging.config.file=$JDBC_BRIDGE_HOME/logging.properties -Dnashorn.args=--language=es6 \
			${JDBC_BRIDGE_JVM_OPTS:=""} -cp $classpath com.clickhouse.jdbcbridge.JdbcBridgeVerticle
	else
		java -XX:+UseContainerSupport -XX:+IdleTuningCompactOnIdle -XX:+IdleTuningGcOnIdle \
			-Xdump:none -Xdump:tool:events=systhrow+throw,filter=*OutOfMemoryError,exec="kill -9 %pid" \
			-Djava.util.logging.config.file=$JDBC_BRIDGE_HOME/logging.properties -Dnashorn.args=--language=es6 \
			${JDBC_BRIDGE_JVM_OPTS:=""} -jar clickhouse-jdbc-bridge-shaded.jar
	fi
}

if [ $# -eq 0 ]; then
	start_server
else
	exec "$@"
fi
