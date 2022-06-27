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

import static com.clickhouse.jdbcbridge.core.DataType.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Objects;
import java.util.TimeZone;

import io.vertx.core.json.JsonObject;

/**
 * This class defines a parameter with data type and default value.
 * 
 * @since 2.0
 */
public class TypedParameter<T> {
    private final Class<T> type;
    private final DataType chType;
    private final String name;
    private final T defaultValue;

    private T value;

    public TypedParameter(Class<T> type, String name, T defaultValue) {
        this(type, null, name, defaultValue, defaultValue);
    }

    public TypedParameter(Class<T> type, DataType chType, String name, T defaultValue) {
        this(type, chType, name, defaultValue, defaultValue);
    }

    public TypedParameter(Class<T> type, String name, T defaultValue, T value) {
        this(type, null, name, defaultValue, value);
    }

    public TypedParameter(Class<T> type, DataType chType, String name, T defaultValue, T value) {
        this.type = Objects.requireNonNull(type);

        if (!type.isPrimitive() && type != BigInteger.class && type != BigDecimal.class && type != Double.class
                && type != Float.class && type != Long.class && type != Integer.class && type != Short.class
                && type != Character.class && type != Byte.class && type != Boolean.class && type != String.class) {
            throw new IllegalArgumentException("Only primitive types, string and big decimal are supported!");
        }

        this.name = Objects.requireNonNull(name);
        this.defaultValue = Objects.requireNonNull(defaultValue);

        if (chType != null) {
            this.chType = chType;
        } else {
            if (defaultValue instanceof BigDecimal) {
                this.chType = DataType.Decimal;
            } else if (defaultValue instanceof Float) {
                this.chType = DataType.Float32;
            } else if (defaultValue instanceof Double) {
                this.chType = DataType.Float64;
            } else if (defaultValue instanceof Long) {
                this.chType = DataType.UInt64;
            } else if (defaultValue instanceof Number) {
                this.chType = DataType.Int32;
            } else {
                this.chType = DataType.Str;
            }
        }

        this.value = value;
    }

    public String getName() {
        return this.name;
    }

    public Class<T> getType() {
        return this.type;
    }

    public T getDefaultValue() {
        return this.defaultValue;
    }

    public T getValue() {
        return this.value;
    }

    public TypedParameter<T> merge(TypedParameter<T> p) {
        if (p != null) {
            this.value = p.value;
        }

        return this;
    }

    public TypedParameter<T> merge(JsonObject p) {
        return this.merge(p, this.name);
    }

    @SuppressWarnings("unchecked")
    public TypedParameter<T> merge(JsonObject p, String name) {
        name = name == null ? this.name : name;

        if (p != null) {
            final T newValue;

            if (this.type.isAssignableFrom(BigDecimal.class)) {
                Double innerValue = p.getDouble(this.name);
                newValue = innerValue == null ? null : (T) BigDecimal.valueOf(innerValue.doubleValue());
            } else if (this.type.isAssignableFrom(Double.class)) {
                newValue = (T) p.getDouble(name);
            } else if (this.type.isAssignableFrom(Float.class)) {
                newValue = (T) p.getFloat(name);
            } else if (this.type.isAssignableFrom(Long.class)) {
                newValue = (T) p.getLong(name);
            } else if (this.type.isAssignableFrom(Boolean.class)) {
                newValue = (T) p.getBoolean(name);
            } else if (this.defaultValue instanceof Number) {
                newValue = (T) p.getInteger(name);
            } else {
                newValue = (T) p.getString(name);
            }

            if (newValue != null) {
                this.value = newValue;
            }
        }

        return this;
    }

    public TypedParameter<T> merge(Object v) {
        return merge(v == null ? (String) null : String.valueOf(v));
    }

    @SuppressWarnings("unchecked")
    public TypedParameter<T> merge(String v) {
        if (v != null) {
            if (this.type.isAssignableFrom(BigDecimal.class)) {
                this.value = (T) BigDecimal.valueOf(Double.valueOf(v).doubleValue());
            } else if (this.type.isAssignableFrom(Double.class)) {
                this.value = (T) Double.valueOf(v);
            } else if (this.type.isAssignableFrom(Float.class)) {
                this.value = (T) Float.valueOf(v);
            } else if (this.type.isAssignableFrom(Long.class)) {
                this.value = (T) Long.valueOf(v);
            } else if (this.type.isAssignableFrom(Boolean.class)) {
                this.value = (T) Boolean.valueOf(v);
            } else if (this.defaultValue instanceof Number) {
                this.value = (T) Integer.valueOf(v);
            } else {
                this.value = (T) v;
            }
        }

        return this;
    }

    public TypedParameter<T> writeValueTo(ByteBuffer buffer, int precision, int scale, TimeZone timezone) {
        switch (this.chType) {
            case Bool:
            case Int8:
                buffer.writeInt8((Integer) this.value);
                break;
            case Int16:
                buffer.writeInt16((Integer) this.value);
                break;
            case Int32:
                buffer.writeInt32((Integer) this.value);
                break;
            case Int64:
                buffer.writeInt64((Long) this.value);
                break;
            case UInt8:
                buffer.writeUInt8((Integer) this.value);
                break;
            case Date:
            case UInt16:
                buffer.writeUInt16((Integer) this.value);
                break;
            case UInt32:
                buffer.writeUInt32((Long) this.value);
                break;
            case DateTime:
                buffer.writeDateTime((Long) this.value, timezone);
                break;
            case DateTime64:
                buffer.writeDateTime64((Long) this.value, 0, this.chType.getScale(), timezone);
                break;
            case UInt64:
                buffer.writeUInt64((Long) this.value);
                break;
            case Float32:
                buffer.writeFloat32((Float) this.value);
                break;
            case Float64:
                buffer.writeFloat64((Double) this.value);
                break;
            case Decimal:
                precision = precision <= 0 || precision > MAX_PRECISON ? DEFAULT_DECIMAL_PRECISON : precision;
                buffer.writeDecimal((BigDecimal) this.value, precision,
                        scale <= 0 ? DEFAULT_DECIMAL_SCALE : (scale > precision ? precision : scale));
                break;
            case Decimal32:
                precision = precision <= 0 || precision > MAX_PRECISON ? DEFAULT_DECIMAL32_PRECISON : precision;
                buffer.writeDecimal32((BigDecimal) this.value,
                        scale <= 0 ? DEFAULT_DECIMAL32_SCALE : (scale > precision ? precision : scale));
                break;
            case Decimal64:
                precision = precision <= 0 || precision > MAX_PRECISON ? DEFAULT_DECIMAL64_PRECISON : precision;
                buffer.writeDecimal64((BigDecimal) this.value,
                        scale <= 0 ? DEFAULT_DECIMAL64_SCALE : (scale > precision ? precision : scale));
                break;
            case Decimal128:
                precision = precision <= 0 || precision > MAX_PRECISON ? DEFAULT_DECIMAL128_PRECISON : precision;
                buffer.writeDecimal128((BigDecimal) this.value,
                        scale <= 0 ? DEFAULT_DECIMAL128_SCALE : (scale > precision ? precision : scale));
                break;
            case Decimal256:
                precision = precision <= 0 || precision > MAX_PRECISON ? DEFAULT_DECIMAL256_PRECISON : precision;
                buffer.writeDecimal256((BigDecimal) this.value,
                        scale <= 0 ? DEFAULT_DECIMAL256_SCALE : (scale > precision ? precision : scale));
                break;
            case Str:
            default:
                buffer.writeString((String) this.value);
                break;
        }

        return this;
    }

    public String toKeyValuePairString() {
        return new StringBuilder().append(this.name).append('=').append(this.value).toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((chType == null) ? 0 : chType.hashCode());
        result = prime * result + ((defaultValue == null) ? 0 : defaultValue.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TypedParameter<?> other = (TypedParameter<?>) obj;
        if (chType != other.chType)
            return false;
        if (defaultValue == null) {
            if (other.defaultValue != null)
                return false;
        } else if (!defaultValue.equals(other.defaultValue))
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (type == null) {
            if (other.type != null)
                return false;
        } else if (!type.equals(other.type))
            return false;
        if (value == null) {
            if (other.value != null)
                return false;
        } else if (!value.equals(other.value))
            return false;
        return true;
    }
}