package ru.yandex.clickhouse.jdbcbridge.db.clickhouse.db;

import org.jooq.SQLDialect;

/**
 * Created by krash on 19.11.18.
 */
public class PostgresqlIntegrationTest extends ExternalDBTest {
    @Override
    protected String getEnvPropertyName() {
        return "datasource.postgresql";
    }

    @Override
    protected SQLDialect getDialect() {
        return SQLDialect.POSTGRES;
    }
}
