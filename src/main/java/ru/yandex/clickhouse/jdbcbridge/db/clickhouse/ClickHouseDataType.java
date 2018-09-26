package ru.yandex.clickhouse.jdbcbridge.db.clickhouse;

/**
 * Enumeration of ClickHouse data types
 * Created by krash on 26.09.18.
 */
public enum ClickHouseDataType {
    // Unsigned
    UInt8,
    UInt16,
    UInt32,
    UInt64,

    // Signed
    Int8,
    Int16,
    Int32,
    Int64,

    // Floating point
    Float32,
    Float64,

    // Date time
    DateTime,
    Date,

    // Misc
    String;

    public String getName(boolean isNullable) {
        String retval = toString();
        if (isNullable) {
            retval = "Nullable(" + retval + ")";
        }
        return retval;
    }
}
