package ru.yandex.clickhouse.jdbcbridge.db.clickhouse;

import ru.yandex.clickhouse.util.ClickHouseRowBinaryStream;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Created by krash on 26.09.18.
 */
public class ClickHouseRowSerializer {

    private final Collection<ClickHouseFieldSerializer> serializers = new ArrayList<>();

    public void serialize(ResultSet resultSet, ClickHouseRowBinaryStream stream) throws IOException, SQLException {
        int i = 0;
        for (ClickHouseFieldSerializer serializer : serializers) {
            serializer.serialize(resultSet, ++i, stream);
        }
    }

    private void add(ClickHouseFieldSerializer fieldSerializer) {
        serializers.add(fieldSerializer);
    }

    public static ClickHouseRowSerializer create(ResultSetMetaData meta) throws SQLException {

        ClickHouseRowSerializer serializer = new ClickHouseRowSerializer();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            int precision = meta.getPrecision(i);
            int scale = meta.getScale(i);
            ExtractorConverter<?> ser = ClickHouseConverter.getSerializerBySQLType(meta.getColumnType(i), precision, scale);
            boolean isNullable = meta.isNullable(i) == ResultSetMetaData.columnNullable;
            ClickHouseFieldSerializer<?> fieldSerializer = new ClickHouseFieldSerializer<>(isNullable, ser);
            serializer.add(fieldSerializer);
        }
        return serializer;
    }

}
