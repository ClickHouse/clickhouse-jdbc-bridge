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

    private static final Map<TypeWithScaleAndPrecision, MappingInstruction> MAP;

    public static final int DECIMAL32 = -10001;
    public static final int DECIMAL64 = -10002;
    public static final int DECIMAL128 = -10003;
    public static final int DECIMAL32MAXPRECISION = 9;
    public static final int DECIMAL64MAXPRECISION = 18;
    public static final int DECIMAL128MAXPRECISION = 38;

    static class TypeWithScaleAndPrecision{
        int type;
        Integer scale;
        Integer precision;

        public TypeWithScaleAndPrecision(int type, Integer scale, Integer precision) {
            this.type = type;
            this.scale = scale;
            this.precision = precision;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TypeWithScaleAndPrecision that = (TypeWithScaleAndPrecision) o;
            if (type == NUMERIC){
            return type == that.type &&
                    Objects.equals(scale, that.scale) &&
                    Objects.equals(precision, that.precision);}
            else{
                return type == that.type;
            }
        }

        @Override
        public int hashCode() {
            if (type == NUMERIC) {
                return Objects.hash(type, scale, precision);
            }else{
                return Objects.hash(type);
            }
        }
    }

    static {
        Map<TypeWithScaleAndPrecision, MappingInstruction> map = new HashMap<>();
        map.put(new TypeWithScaleAndPrecision(TINYINT,null,null), new MappingInstruction<>(Int8, ResultSet::getInt, (i, s) -> s.writeInt8(i)));
        map.put(new TypeWithScaleAndPrecision(SMALLINT,null,null), new MappingInstruction<>(Int16, ResultSet::getInt, (i, s) -> s.writeInt16(i)));
        map.put(new TypeWithScaleAndPrecision(INTEGER,null,null), new MappingInstruction<>(Int32, ResultSet::getInt, (i, s) -> s.writeInt32(i)));
        map.put(new TypeWithScaleAndPrecision(BIGINT,null,null), new MappingInstruction<>(Int64, ResultSet::getLong, (i, s) -> s.writeInt64(i)));

        map.put(new TypeWithScaleAndPrecision(FLOAT,null,null), new MappingInstruction<>(Float32, ResultSet::getFloat, (i, s) -> s.writeFloat32(i)));
        map.put(new TypeWithScaleAndPrecision(REAL,null,null), new MappingInstruction<>(Float32, ResultSet::getFloat, (i, s) -> s.writeFloat32(i)));
        map.put(new TypeWithScaleAndPrecision(DOUBLE,null,null), new MappingInstruction<>(Float64, ResultSet::getFloat, (i, s) -> s.writeFloat64(i)));
        map.put(new TypeWithScaleAndPrecision(NUMERIC,null,null), new MappingInstruction<>(Decimal, ResultSet::getFloat, (i, s) -> s.writeFloat64(i)));

        map.put(new TypeWithScaleAndPrecision(TIMESTAMP,null,null), new MappingInstruction<>(DateTime, ResultSet::getTimestamp, (i, s) -> s.writeDateTime(i)));
        map.put(new TypeWithScaleAndPrecision(TIME,null,null), new MappingInstruction<>(DateTime, ResultSet::getTime, (i, s) -> s.writeDateTime(i)));
        map.put(new TypeWithScaleAndPrecision(DATE,null,null), new MappingInstruction<>(Date, ResultSet::getDate, (i, s) -> s.writeDate(i)));

        map.put(new TypeWithScaleAndPrecision(BIT,null,null), new MappingInstruction<>(UInt8, ResultSet::getBoolean, (i, s) -> s.writeUInt8(i)));
        map.put(new TypeWithScaleAndPrecision(BOOLEAN,null,null), new MappingInstruction<>(UInt8, ResultSet::getBoolean, (i, s) -> s.writeUInt8(i)));

        // @todo test
        map.put(new TypeWithScaleAndPrecision(CHAR,null,null), new MappingInstruction<>(String, ResultSet::getString, (i, s) -> s.writeString(i)));
        map.put(new TypeWithScaleAndPrecision(VARCHAR,null,null), new MappingInstruction<>(String, ResultSet::getString, (i, s) -> s.writeString(i)));
        map.put(new TypeWithScaleAndPrecision(LONGVARCHAR,null,null), new MappingInstruction<>(String, ResultSet::getString, (i, s) -> s.writeString(i)));
        map.put(new TypeWithScaleAndPrecision(NVARCHAR,null,null), new MappingInstruction<>(String, ResultSet::getString, (i, s) -> s.writeString(i)));

        for (int precision = 1 ; precision <= DECIMAL128MAXPRECISION ; precision++ ){
            for (int scale = 0 ; scale <= precision ; scale++ ){
                int finalScale = scale;
                if (precision <=DECIMAL32MAXPRECISION ){
                    map.put(new TypeWithScaleAndPrecision(NUMERIC,scale,precision), new MappingInstruction<>(Decimal, ResultSet::getBigDecimal, (i, s) -> s.writeDecimal32(i, finalScale)));
                }else if (precision <= DECIMAL64MAXPRECISION){
                    map.put(new TypeWithScaleAndPrecision(NUMERIC,scale,precision), new MappingInstruction<>(Decimal, ResultSet::getBigDecimal, (i, s) -> s.writeDecimal64(i, finalScale)));
                }else{
                    map.put(new TypeWithScaleAndPrecision(NUMERIC,scale,precision), new MappingInstruction<>(Decimal, ResultSet::getBigDecimal, (i, s) -> s.writeDecimal128(i, finalScale)));
                }

            }
        }

        MAP = Collections.unmodifiableMap(map);
    }

    public static ClickHouseDataType getBySQLType(int type, int precision, int scale) throws SQLException {
        return getInstructionBySQLType(type, precision, scale).clickHouseDataType;
    }

    public static ExtractorConverter<?> getSerializerBySQLType(int type, int precision, int scale) throws SQLException {
        return getInstructionBySQLType(type, precision, scale).serializer;
    }


    private static MappingInstruction<?> getInstructionBySQLType(int sqlType, int precision, int scale) throws SQLException {

        MappingInstruction<?> instruction = MAP.get(new TypeWithScaleAndPrecision(sqlType,scale,precision));
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
