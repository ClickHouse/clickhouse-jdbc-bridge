package ru.yandex.clickhouse.jdbcbridge.db.clickhouse.db;

import lombok.extern.slf4j.Slf4j;
import org.jooq.CreateTableFinalStep;
import org.jooq.SQLDialect;
import ru.yandex.clickhouse.jdbcbridge.db.clickhouse.ClickHouseConverterIntegrationTestNew;

import java.util.Properties;

/**
 * Created by krash on 16.11.18.
 */
@Slf4j
public class SQLiteIntegrationTest extends ClickHouseConverterIntegrationTestNew {

    @Override
    protected ConnectionSpec getConnectionSpec() {
        return new ConnectionSpec("jdbc:sqlite::memory:", new Properties());
    }

    @Override
    protected SQLDialect getDialect() {
        return SQLDialect.SQLITE;
    }

    @Override
    protected void createTable(CreateTableFinalStep step) throws Exception {
        String sql = step.getSQL().replaceAll(" global ", " ");
        log.info(sql);
        step.configuration().connectionProvider().acquire().createStatement().execute(sql);
    }
}
