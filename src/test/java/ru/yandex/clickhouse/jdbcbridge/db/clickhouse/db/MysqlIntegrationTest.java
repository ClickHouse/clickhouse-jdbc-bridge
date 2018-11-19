package ru.yandex.clickhouse.jdbcbridge.db.clickhouse.db;

import org.jooq.SQLDialect;

/**
 * Created by krash on 19.11.18.
 */
public class MysqlIntegrationTest extends ExternalDBTest {

    @Override
    protected SQLDialect getDialect() {
        return SQLDialect.MYSQL;
    }

    @Override
    protected String getEnvPropertyName() {
        return "datasource.mysql";
    }
}
