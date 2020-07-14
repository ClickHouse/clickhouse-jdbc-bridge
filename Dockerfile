FROM openjdk:8
ENV BRIDGE_BIND_HOST 0.0.0.0
ENV LOG_LEVEL INFO
ENV BRIDGE_BIND_PORT 9019
RUN wget https://github.com/long2ice/clickhouse-jdbc-bridge/releases/download/v1.0.2/clickhouse-jdbc-bridge-1.0.2.jar
COPY ./jdbc-drivers /opt/jdbc-drivers
COPY entrypoint.sh ./
RUN chmod +x entrypoint.sh
ENTRYPOINT ["./entrypoint.sh"]