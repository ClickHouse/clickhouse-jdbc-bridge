package ru.yandex.clickhouse.jdbcbridge.db.clickhouse.db;

import static org.junit.Assume.assumeNotNull;

import org.jooq.SQLDialect;
import ru.yandex.clickhouse.jdbcbridge.db.clickhouse.ClickHouseConverterIntegrationTestNew;

import java.util.Properties;

/**
 * Created by krash on 19.11.18.
 */
public abstract class ExternalDBTest extends ClickHouseConverterIntegrationTestNew {

    @Override
    protected ConnectionSpec getConnectionSpec() {
        String databaseUri = System.getProperty(getEnvPropertyName(), null);
        assumeNotNull(databaseUri);
        return new ConnectionSpec(databaseUri, new Properties());
    }

    protected abstract String getEnvPropertyName();
}
