package ru.yandex.clickhouse.jdbcbridge.db.clickhouse;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * This test checks converter functionality over expected set of
 * SQL primitives to be converted
 * Created by krash on 26.09.18.
 */
@RunWith(Parameterized.class)
public class ClickHouseConverterKnownTypes {

    private final int expectedSQLType;

    public ClickHouseConverterKnownTypes(int sqlType) {
        expectedSQLType = sqlType;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.stream(new int[]{
                Types.BIGINT,
                Types.INTEGER,
                Types.SMALLINT,
                Types.TINYINT,
                Types.FLOAT,
                Types.DOUBLE,
                Types.REAL,
                Types.DATE,
                Types.TIME,
                Types.TIMESTAMP,
                Types.BIT,
                Types.BOOLEAN
        }).mapToObj(i -> new Object[]{i}).collect(Collectors.toList());
    }

    @Test
    public void getBySQLType() throws Exception {
        ClickHouseConverter.getBySQLType(expectedSQLType, 0,0);
    }
}