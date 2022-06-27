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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.JDBCType;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * This interface defines converters to map from one data type to another.
 * 
 * @since 2.0
 */
public interface DataTypeConverter {
    static final String M_TYPE_TEXT = "type text";
    static final String M_TYPE_NUMBER = "type number";
    static final String M_TYPE_DATE = "type date";
    static final String M_TYPE_DATETIME = "type datetime";

    static final String M_FACET_INT8 = "Int8.Type";
    static final String M_FACET_INT16 = "Int16.Type";
    static final String M_FACET_INT32 = "Int32.Type";
    static final String M_FACET_INT64 = "Int64.Type";
    static final String M_FACET_SINGLE = "Single.Type";
    static final String M_FACET_DOUBLE = "Double.Type";

    @SuppressWarnings("unchecked")
    default <T> T as(Class<T> type, Object value) {
        final Object result;

        if (Boolean.class.equals(type)) {
            if (value instanceof Boolean) {
                result = value;
            } else if (value instanceof Number) {
                result = ((Number) value).intValue() != 0;
            } else {
                result = Boolean.parseBoolean(String.valueOf(value));
            }
        } else if (Byte.class.equals(type)) {
            if (value instanceof Boolean) {
                result = (boolean) value ? (byte) 0 : (byte) 1;
            } else if (value instanceof Number) {
                result = ((Number) value).byteValue();
            } else {
                result = Byte.parseByte(String.valueOf(value));
            }
        } else if (Short.class.equals(type)) {
            if (value instanceof Boolean) {
                result = (boolean) value ? (short) 0 : (short) 1;
            } else if (value instanceof Number) {
                result = ((Number) value).shortValue();
            } else {
                result = Short.parseShort(String.valueOf(value));
            }
        } else if (Integer.class.equals(type)) {
            if (value instanceof Boolean) {
                result = (boolean) value ? 0 : 1;
            } else if (value instanceof Number) {
                result = ((Number) value).intValue();
            } else {
                result = Integer.parseInt(String.valueOf(value));
            }
        } else if (Long.class.equals(type)) {
            if (value instanceof Boolean) {
                result = (boolean) value ? 0L : 1L;
            } else if (value instanceof Number) {
                result = ((Number) value).longValue();
            } else {
                result = Long.parseLong(String.valueOf(value));
            }
        } else if (Float.class.equals(type)) {
            if (value instanceof Boolean) {
                result = (boolean) value ? 0.0F : 1.0F;
            } else if (value instanceof Number) {
                result = ((Number) value).floatValue();
            } else {
                result = Float.parseFloat(String.valueOf(value));
            }
        } else if (Double.class.equals(type)) {
            if (value instanceof Boolean) {
                result = (boolean) value ? 0.0D : 1.0D;
            } else if (value instanceof Number) {
                result = ((Number) value).doubleValue();
            } else {
                result = Double.parseDouble(String.valueOf(value));
            }
        } else if (BigInteger.class.equals(type)) {
            if (value instanceof Boolean) {
                result = (boolean) value ? BigInteger.ZERO : BigInteger.ONE;
            } else if (value instanceof BigInteger) {
                result = value;
            } else {
                result = new BigInteger(String.valueOf(value));
            }
        } else if (BigDecimal.class.equals(type)) {
            if (value instanceof Boolean) {
                result = (boolean) value ? BigDecimal.ZERO : BigDecimal.ONE;
            } else if (value instanceof BigDecimal) {
                result = value;
            } else {
                result = new BigDecimal(String.valueOf(value));
            }
        } else if (Date.class.equals(type)) {
            if (value instanceof Boolean) {
                result = new Date((boolean) value ? 0L : 1L);
            } else if (value instanceof Number) {
                result = new Date(((Number) value).longValue());
            } else if (value instanceof String) {
                String str = (String) value;
                int len = str.length();
                if (len == 10) {
                    result = java.sql.Date.valueOf(LocalDate.parse(str, DateTimeFormatter.ISO_LOCAL_DATE));
                } else if (len >= 10 && len <= 16) {
                    result = java.sql.Date.valueOf(LocalDate.parse(str, DateTimeFormatter.ISO_DATE));
                } else if (len == 19) {
                    result = java.sql.Timestamp
                            .valueOf(LocalDateTime.parse(str, DateTimeFormatter.ISO_LOCAL_DATE_TIME));
                } else if (len > 19) {
                    result = java.sql.Timestamp.valueOf(LocalDateTime.parse(str, DateTimeFormatter.ISO_DATE_TIME));
                } else {
                    result = java.sql.Date.valueOf(LocalDate.parse(str, DateTimeFormatter.BASIC_ISO_DATE));
                }
            } else {
                result = value;
            }
        } else if (String.class.equals(type)) {
            result = String.valueOf(value);
        } else {
            result = value;
        }

        return (T) result;
    }

    DataType from(JDBCType jdbcType, String typeName, int precision, int scale, boolean signed);

    DataType from(Object javaObject);

    default String toPowerQueryType(DataType type) {
        return toMType(type);
    }

    default String toMType(DataType type) {
        String mType = M_TYPE_TEXT;

        switch (type) {
            case Bool:
            case Int8:
                mType = M_FACET_INT8;
                break;
            case UInt8:
            case Int16:
                mType = M_FACET_INT16;
                break;
            case UInt16:
            case Int32:
                mType = M_FACET_INT32;
                break;
            case UInt32:
            case Int64:
                mType = M_FACET_INT64;
                break;
            case Float32:
                mType = M_FACET_SINGLE;
                break;
            case Float64:
                mType = M_FACET_DOUBLE;
                break;
            case UInt64:
            case Decimal:
            case Decimal32:
            case Decimal64:
            case Decimal128:
            case Decimal256:
                mType = M_TYPE_NUMBER;
                break;
            case Date:
                mType = M_TYPE_DATE;
                break;
            case DateTime:
            case DateTime64:
                mType = M_TYPE_DATETIME;
                break;
            default:
                break;
        }

        return mType;
    }
}
