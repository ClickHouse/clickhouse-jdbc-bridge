/**
 * Copyright 2019-2022, Zhichun Wu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.clickhouse.jdbcbridge.core;

// https://clickhouse.tech/docs/en/sql-reference/data-types/
public enum DataType {
    // Boolean
    Bool(1, 4, 0), // interpreted as Int8, recommend to use UInt8(0 or 1) instead

    // Signed
    Int8(1, 4, 0), Int16(2, 6, 0), Int32(4, 11, 0), Int64(8, 20, 0), Int128(16, 20, 0), Int256(32, 40, 0),

    // Unsigned
    UInt8(1, 3, 0), UInt16(2, 5, 0), UInt32(4, 10, 0), UInt64(8, 19, 0), UInt128(16, 20, 0), UInt256(32, 39, 0),

    // Floating point
    Float32(4, 8, 8), Float64(16, 17, 17),

    // Date time
    Date(4, 10, 0), DateTime(8, 19, 0), DateTime64(16, 38, 18),

    // Decimals
    Decimal(32, 76, 76), Decimal32(4, 9, 9), Decimal64(8, 18, 18), Decimal128(16, 38, 38), Decimal256(32, 76, 76),

    // Misc
    Enum(1, 4, 0), Enum8(1, 4, 0), Enum16(2, 6, 0), IPv4(4, 10, 0), IPv6(16, 0, 0), FixedStr(0, 0, 0), Str(0, 0, 0),
    UUID(16, 20, 0);

    // TODO: support complex types:
    // Array, Tuple, Nested, AggregateFunction, SimpleAggregateFunction

    public static final String ALIAS_BOOLEAN = "Boolean";
    public static final String ALIAS_STRING = "String";
    public static final String ALIAS_FIXED_STRING = "FixedString";

    public static final int DEFAULT_DECIMAL_PRECISON = 10;
    public static final int DEFAULT_DECIMAL_SCALE = 4;
    public static final int DEFAULT_DECIMAL32_PRECISON = 9;
    public static final int DEFAULT_DECIMAL32_SCALE = 2;
    public static final int DEFAULT_DECIMAL64_PRECISON = 18;
    public static final int DEFAULT_DECIMAL64_SCALE = 4;
    public static final int DEFAULT_DECIMAL128_PRECISON = 38;
    public static final int DEFAULT_DECIMAL128_SCALE = 8;
    public static final int DEFAULT_DECIMAL256_PRECISON = 76;
    public static final int DEFAULT_DECIMAL256_SCALE = 16;

    // https://clickhouse.tech/docs/en/sql-reference/data-types/decimal/
    public static final int MAX_PRECISON = 76;

    public static final boolean DEFAULT_NULLABLE = true;
    public static final int DEFAULT_LENGTH = 0;
    public static final int DEFAULT_PRECISION = 0;
    public static final int DEFAULT_SCALE = 0;

    public static final int MAX_DATETIME64_PRECISION = 38; // 19 + 1 + 18
    public static final int MAX_DATETIME64_SCALE = 18;
    public static final int DEFAULT_DATETIME64_PRECISION = 23; // 19 + 1 + 3
    // Tick size (precision): 10-precision seconds
    public static final int DEFAULT_DATETIME64_SCALE = 3;

    /**
     * Replacement of {@link #valueOf(String)}.
     * 
     * @param value case-insensitive string representation of the data type
     * @return data type
     */
    public static DataType from(String value) {
        DataType t = Str;

        if (value == null || ALIAS_STRING.equalsIgnoreCase(value)) {
            t = Str;
        } else if (ALIAS_BOOLEAN.equalsIgnoreCase(value)) {
            t = Bool;
        } else if (ALIAS_FIXED_STRING.equalsIgnoreCase(value)) {
            t = FixedStr;
        } else {
            for (DataType d : DataType.values()) {
                if (d.name().equalsIgnoreCase(value)) {
                    t = d;
                    break;
                }
            }
        }

        return t;
    }

    private final int length;
    private final int precision;
    private final int scale;

    private DataType(int length, int precision, int scale) {
        this.length = length < 0 ? DEFAULT_LENGTH : length;
        this.precision = precision < 0 ? DEFAULT_PRECISION : precision;
        this.scale = scale < 0 ? DEFAULT_SCALE : (scale > this.precision ? this.precision : scale);
    }

    /**
     * Get length in byte.
     * 
     * @return length in byte, zero stands for unlimited
     */
    public int getLength() {
        return length;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }
}
