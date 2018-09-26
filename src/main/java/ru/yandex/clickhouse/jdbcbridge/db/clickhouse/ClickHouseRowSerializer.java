package ru.yandex.clickhouse.jdbcbridge.db.clickhouse;

import lombok.Data;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryStream;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by krash on 26.09.18.
 */
@Data
public class ClickHouseRowSerializer {

    Collection<ClickHouseFieldSerializer> serializers = new ArrayList<>();

    public void serialize(ResultSet resultSet, ClickHouseRowBinaryStream stream) throws IOException, SQLException {
        int i = 0;

        for(ClickHouseFieldSerializer serializer : serializers) {
            serializer.serialize(resultSet, ++i, stream);
        }
    }
    
}
