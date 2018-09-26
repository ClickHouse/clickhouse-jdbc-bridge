package ru.yandex.clickhouse.jdbcbridge.db.clickhouse;

import ru.yandex.clickhouse.util.ClickHouseRowBinaryStream;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by krash on 26.09.18.
 */
public class ClickHouseFieldSerializer<T> {

    private final boolean canBeNull;
    private final FieldValueExtractor<T> extractor;
    private final FieldValueSerializer<T> serializer;

    public ClickHouseFieldSerializer(boolean fieldIsNullable, FieldValueExtractor<T> extractor, FieldValueSerializer<T> serializer) {
        canBeNull = fieldIsNullable;
        this.extractor = extractor;
        this.serializer = serializer;
    }

//    protected abstract T extract(ResultSet resultSet, int position) throws SQLException;
//
//    protected abstract void write(T value, ClickHouseRowBinaryStream stream) throws IOException;

    public void serialize(ResultSet resultSet, int position, ClickHouseRowBinaryStream stream) throws SQLException, IOException {
        T value = extractor.apply(resultSet, position);
        if (canBeNull) {
            final boolean isNull = resultSet.wasNull() || null == value;
            stream.writeByte((byte) (isNull ? 1 : 0));
            if (isNull) {
                return;
            }
        }
        serializer.accept(value, stream);
    }
}
