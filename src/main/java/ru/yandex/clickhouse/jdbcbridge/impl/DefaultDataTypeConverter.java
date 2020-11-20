/**
 * Copyright 2019-2020, Zhichun Wu
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
package ru.yandex.clickhouse.jdbcbridge.impl;

import java.sql.JDBCType;

import ru.yandex.clickhouse.jdbcbridge.core.DataType;
import ru.yandex.clickhouse.jdbcbridge.core.DataTypeConverter;

/**
 * This class is default implementation of DataTypeConvert.
 * 
 * @since 2.0
 */
public class DefaultDataTypeConverter implements DataTypeConverter {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(DataTypeConverter.class);

    @Override
    public DataType from(JDBCType jdbcType, boolean signed, boolean useDateTime) {
        DataType type = DataType.Str;

        switch (jdbcType) {
            case BIT:
            case BOOLEAN:
                type = DataType.UInt8;
                break;
            case TINYINT:
                type = signed ? DataType.Int8 : DataType.UInt8;
                break;
            case SMALLINT:
                type = signed ? DataType.Int16 : DataType.UInt16;
                break;
            case INTEGER:
                type = signed ? DataType.Int32 : DataType.UInt32;
                break;
            case BIGINT:
                type = signed ? DataType.Int64 : DataType.UInt64;
                break;
            case REAL:
            case FLOAT:
                type = DataType.Float32;
                break;
            case DOUBLE:
                type = DataType.Float64;
                break;
            case NUMERIC:
            case DECIMAL:
                type = DataType.Decimal;
                break;
            case CHAR:
            case NCHAR:
            case VARCHAR:
            case NVARCHAR:
            case LONGVARCHAR:
            case LONGNVARCHAR:
            case NULL:
                type = DataType.Str;
                break;
            case DATE:
                type = DataType.Date;
                break;
            case TIME:
            case TIMESTAMP:
            case TIME_WITH_TIMEZONE:
            case TIMESTAMP_WITH_TIMEZONE:
                type = useDateTime ? DataType.DateTime : DataType.DateTime64;
                break;
            default:
                log.warn("Unsupported JDBC type [{}], which will be treated as [{}]", jdbcType.name(), type.name());
                break;
        }

        return type;
    }

    @Override
    public DataType from(Object javaObject) {
        final DataType type;

        if (javaObject == null) {
            type = DataType.Str;
        } else if (javaObject instanceof Byte) {
            type = DataType.Int8;
        } else if (javaObject instanceof Short) {
            type = DataType.Int16;
        } else if (javaObject instanceof Integer) {
            type = DataType.Int32;
        } else if (javaObject instanceof Long) {
            type = DataType.Int64;
        } else if (javaObject instanceof Float) {
            type = DataType.Float32;
        } else if (javaObject instanceof Double) {
            type = DataType.Float64;
        } else {
            type = DataType.Str;
        }

        return type;
    }
}
