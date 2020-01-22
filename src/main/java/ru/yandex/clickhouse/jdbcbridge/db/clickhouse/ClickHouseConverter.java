package ru.yandex.clickhouse.jdbcbridge.db.clickhouse;

import static java.sql.Types.*;
import static ru.yandex.clickhouse.jdbcbridge.db.clickhouse.ClickHouseDataType.*;

import ru.yandex.clickhouse.ClickHouseUtil;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;

/**
 * This utility converts SQL datatypes to appropriate in ClickHouse
 * Created by krash on 26.09.18.
 */
public class ClickHouseConverter {

    private static final Map<Integer, MappingInstruction> MAP;

    public static final int DECIMAL32 = -10001;
    public static final int DECIMAL64 = -10002;
    public static final int DECIMAL128 = -10003;

    static {
        Map<Integer, MappingInstruction> map = new HashMap<>();
        map.put(TINYINT, new MappingInstruction<>(Int8, ResultSet::getInt, (i, s) -> s.writeInt8(i)));
        map.put(SMALLINT, new MappingInstruction<>(Int16, ResultSet::getInt, (i, s) -> s.writeInt16(i)));
        map.put(INTEGER, new MappingInstruction<>(Int32, ResultSet::getInt, (i, s) -> s.writeInt32(i)));
        map.put(BIGINT, new MappingInstruction<>(Int64, ResultSet::getLong, (i, s) -> s.writeInt64(i)));

        map.put(FLOAT, new MappingInstruction<>(Float32, ResultSet::getFloat, (i, s) -> s.writeFloat32(i)));
        map.put(REAL, new MappingInstruction<>(Float32, ResultSet::getFloat, (i, s) -> s.writeFloat32(i)));
        map.put(DOUBLE, new MappingInstruction<>(Float64, ResultSet::getFloat, (i, s) -> s.writeFloat64(i)));

        map.put(TIMESTAMP, new MappingInstruction<>(DateTime, ResultSet::getTimestamp, (i, s) -> s.writeDateTime(i)));
        map.put(TIME, new MappingInstruction<>(DateTime, ResultSet::getTime, (i, s) -> s.writeDateTime(i)));
        map.put(DATE, new MappingInstruction<>(Date, ResultSet::getDate, (i, s) -> s.writeDate(i)));

        map.put(BIT, new MappingInstruction<>(UInt8, ResultSet::getBoolean, (i, s) -> s.writeUInt8(i)));
        map.put(BOOLEAN, new MappingInstruction<>(UInt8, ResultSet::getBoolean, (i, s) -> s.writeUInt8(i)));

        // @todo test
        map.put(CHAR, new MappingInstruction<>(String, ResultSet::getString, (i, s) -> s.writeString(i)));
        map.put(VARCHAR, new MappingInstruction<>(String, ResultSet::getString, (i, s) -> s.writeString(i)));
        map.put(LONGVARCHAR, new MappingInstruction<>(String, ResultSet::getString, (i, s) -> s.writeString(i)));
        map.put(NVARCHAR, new MappingInstruction<>(String, ResultSet::getString, (i, s) -> s.writeString(i)));

        MAP = Collections.unmodifiableMap(map);
    }

    public static ClickHouseDataType getBySQLType(int type, int precision, int scale) throws SQLException {
        return getInstructionBySQLType(type, precision, scale).clickHouseDataType;
    }

    public static ExtractorConverter<?> getSerializerBySQLType(int type, int precision, int scale) throws SQLException {
        return getInstructionBySQLType(type, precision, scale).serializer;
    }


    private static MappingInstruction<?> getInstructionBySQLType(int sqlType, int precision, int scale) throws SQLException {

        // for decimal types
        if (sqlType == NUMERIC){
            if (precision <=9 ){
                return new MappingInstruction<>(Decimal, ResultSet::getBigDecimal, (i, s) -> s.writeDecimal32(i, scale));
            }else if (precision <= 18){
                return new MappingInstruction<>(Decimal, ResultSet::getBigDecimal, (i, s) -> s.writeDecimal64(i, scale));
            }else {
                return new MappingInstruction<>(Decimal, ResultSet::getBigDecimal, (i, s) -> s.writeDecimal128(i, scale));
            }
        }

        MappingInstruction<?> instruction = MAP.get(sqlType);
        if (null == instruction) {
            // try to infer name of constant
            String typeName = "unknown";
            Field[] fields = Types.class.getDeclaredFields();
            for (Field field : fields) {
                try {
                    Integer fieldValue = (Integer) field.get(null);
                    if (sqlType == fieldValue) {
                        typeName = field.getName();
                    }

                } catch (Exception e) {
                    // do nothing here
                }
            }
            throw new SQLException("Can not map SQL type " + sqlType + " (" + typeName + ") to ClickHouse");
        }
        return instruction;
    }

    /**
     * Retrieves the columns DDL in format, readable by ClickHouse
     */
    public String getColumnsDDL(ResultSetMetaData meta) throws SQLException {
        StringBuilder builder = new StringBuilder("columns format version: 1\n");

        builder.append(meta.getColumnCount());
        builder.append(" columns:\n");
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            int precision = meta.getPrecision(i);
            int type = meta.getColumnType(i);
            int scale = meta.getScale(i);

            boolean nullable = ResultSetMetaData.columnNullable == meta.isNullable(i);
            builder.append(ClickHouseUtil.quoteIdentifier(meta.getColumnName(i)));
            builder.append(" ");
            builder.append(getBySQLType(type, precision, scale).getName(nullable,meta.getPrecision(i), meta.getScale(i)));
            builder.append('\n');
        }
        return builder.toString();
    }

    public static String getCLIStructure(ResultSetMetaData meta) throws SQLException {
        List<String> list = new ArrayList<>();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            int precision = meta.getPrecision(i);
            int type = meta.getColumnType(i);
            int scale = meta.getScale(i);
            boolean nullable = ResultSetMetaData.columnNullable == meta.isNullable(i);
            list.add(meta.getColumnName(i) + " " + getBySQLType(type, precision, scale).getName(nullable, meta.getPrecision(i), meta.getScale(i)));
        }
        return java.lang.String.join(", ", list);
    }

    private static class MappingInstruction<T> {
        private final ClickHouseDataType clickHouseDataType;
        private final ExtractorConverter<T> serializer;

        public MappingInstruction(ClickHouseDataType type, FieldValueExtractor<T> extractor, FieldValueSerializer<T> serializer) {
            this.clickHouseDataType = type;
            this.serializer = new ExtractorConverter<>(extractor, serializer);
        }
    }
}
