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

import java.sql.JDBCType;

/**
 * This defines a mapping from specific JDBC/native type to {@link DataType}.
 * Native type is case-sensitive. {@code "*"} represents any type.
 * 
 * @since 2.0
 */
public class DataTypeMapping {
    public static final String ANY_NATIVE_TYPE = "*";

    private final JDBCType fromJdbcType;
    private final String fromNativeType;
    private final DataType toType;

    private static JDBCType parse(String fromJdbcType) {
        JDBCType jdbcType = JDBCType.OTHER;

        if (fromJdbcType != null) {
            try {
                fromJdbcType = fromJdbcType.trim().toUpperCase();
                jdbcType = JDBCType.valueOf(fromJdbcType);
            } catch (RuntimeException e) {
            }
        }

        return jdbcType;
    }

    private static JDBCType parse(int fromJdbcType) {
        JDBCType jdbcType = JDBCType.OTHER;

        try {
            jdbcType = JDBCType.valueOf(fromJdbcType);
        } catch (RuntimeException e) {
        }

        return jdbcType;
    }

    public DataTypeMapping(String fromJdbcType, String fromNativeType, String toType) {
        this(parse(fromJdbcType), fromNativeType, DataType.from(toType));
    }

    public DataTypeMapping(int fromJdbcType, String fromNativeType, DataType toType) {
        this(parse(fromJdbcType), fromNativeType, toType);
    }

    public DataTypeMapping(JDBCType fromJdbcType, String fromNativeType, DataType toType) {
        this.fromJdbcType = fromJdbcType;
        this.fromNativeType = ANY_NATIVE_TYPE.equals(fromNativeType) ? ANY_NATIVE_TYPE : fromNativeType;
        this.toType = toType;
    }

    public JDBCType getSourceJdbcType() {
        return fromJdbcType;
    }

    public String getSourceNativeType() {
        return fromNativeType;
    }

    public DataType getMappedType() {
        return toType;
    }

    public boolean accept(JDBCType jdbcType, String nativeType) {
        return fromNativeType != null ? (ANY_NATIVE_TYPE == fromNativeType || fromNativeType.equals(nativeType))
                : fromJdbcType == jdbcType;
    }
}
