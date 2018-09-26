package ru.yandex.clickhouse.jdbcbridge.db.clickhouse;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Interface for extracting the data from ResultSet
 * Created by krash on 26.09.18.
 */
@FunctionalInterface
public interface FieldValueExtractor<R> {

    /**
     * Extracts value from resultset
     */
    R apply(ResultSet resultSet, Integer integer) throws SQLException;
}
