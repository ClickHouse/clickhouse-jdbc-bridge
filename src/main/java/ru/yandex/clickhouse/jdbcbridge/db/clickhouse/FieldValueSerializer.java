package ru.yandex.clickhouse.jdbcbridge.db.clickhouse;

import ru.yandex.clickhouse.util.ClickHouseRowBinaryStream;

import java.io.IOException;

/**
 * Created by krash on 26.09.18.
 */
@FunctionalInterface
public interface FieldValueSerializer<T> {

    /**
     * Serialize non-null value into stream
     */
    void accept(T t, ClickHouseRowBinaryStream stream) throws IOException;
}
