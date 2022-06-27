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

import static com.clickhouse.jdbcbridge.core.Utils.EMPTY_STRING;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import io.vertx.core.json.JsonObject;

/**
 * This class defines default value of all supported ClickHouse data types.
 * 
 * @since 2.0
 */
public class DefaultValues {
    public static final String DEFAULT_UUID = "00000000-0000-0000-0000-000000000000";

    // Boolean
    public final TypedParameter<Integer> Bool;

    // Signed
    public final TypedParameter<Integer> Int8;
    public final TypedParameter<Integer> Int16;
    public final TypedParameter<Integer> Int32;
    public final TypedParameter<Long> Int64;
    public final TypedParameter<BigInteger> Int128;
    public final TypedParameter<BigInteger> Int256;

    // Unsigned
    public final TypedParameter<Integer> UInt8;
    public final TypedParameter<Integer> UInt16;
    public final TypedParameter<Long> UInt32;
    public final TypedParameter<Long> UInt64;
    public final TypedParameter<BigInteger> UInt128;
    public final TypedParameter<BigInteger> UInt256;

    // Floating point
    public final TypedParameter<Float> Float32;
    public final TypedParameter<Double> Float64;

    // Date time
    public final TypedParameter<Integer> Date;
    public final TypedParameter<Long> Datetime;
    public final TypedParameter<Long> Datetime64;

    // Decimals
    public final TypedParameter<BigDecimal> Decimal;
    public final TypedParameter<BigDecimal> Decimal32;
    public final TypedParameter<BigDecimal> Decimal64;
    public final TypedParameter<BigDecimal> Decimal128;
    public final TypedParameter<BigDecimal> Decimal256;

    // Misc
    public final TypedParameter<Integer> Enum;
    public final TypedParameter<Integer> Enum8;
    public final TypedParameter<Integer> Enum16;
    public final TypedParameter<Integer> IPv4; // 0.0.0.0
    public final TypedParameter<String> IPv6;
    public final TypedParameter<String> FixedStr;
    public final TypedParameter<String> Str;
    public final TypedParameter<String> UUID;

    private final Map<String, TypedParameter<?>> types = new TreeMap<>();

    public DefaultValues() {
        Utils.addTypedParameter(types, this.Bool = new TypedParameter<>(Integer.class, DataType.Bool.name(), 0));

        Utils.addTypedParameter(types, this.Int8 = new TypedParameter<>(Integer.class, DataType.Int8.name(), 0));
        Utils.addTypedParameter(types, this.Int16 = new TypedParameter<>(Integer.class, DataType.Int16.name(), 0));
        Utils.addTypedParameter(types, this.Int32 = new TypedParameter<>(Integer.class, DataType.Int32.name(), 0));
        Utils.addTypedParameter(types, this.Int64 = new TypedParameter<>(Long.class, DataType.Int64.name(), 0L));
        Utils.addTypedParameter(types,
                this.Int128 = new TypedParameter<>(BigInteger.class, DataType.Int128.name(), BigInteger.ZERO));
        Utils.addTypedParameter(types,
                this.Int256 = new TypedParameter<>(BigInteger.class, DataType.Int256.name(), BigInteger.ZERO));

        Utils.addTypedParameter(types, this.UInt8 = new TypedParameter<>(Integer.class, DataType.UInt8.name(), 0));
        Utils.addTypedParameter(types, this.UInt16 = new TypedParameter<>(Integer.class, DataType.UInt16.name(), 0));
        Utils.addTypedParameter(types, this.UInt32 = new TypedParameter<>(Long.class, DataType.UInt32.name(), 0L));
        Utils.addTypedParameter(types, this.UInt64 = new TypedParameter<>(Long.class, DataType.UInt64.name(), 0L));
        Utils.addTypedParameter(types,
                this.UInt128 = new TypedParameter<>(BigInteger.class, DataType.UInt128.name(), BigInteger.ZERO));
        Utils.addTypedParameter(types,
                this.UInt256 = new TypedParameter<>(BigInteger.class, DataType.UInt256.name(), BigInteger.ZERO));

        Utils.addTypedParameter(types, this.Float32 = new TypedParameter<>(Float.class, DataType.Float32.name(), 0.0F));
        Utils.addTypedParameter(types, this.Float64 = new TypedParameter<>(Double.class, DataType.Float64.name(), 0.0));

        Utils.addTypedParameter(types, this.Date = new TypedParameter<>(Integer.class, DataType.Date.name(), 1));
        Utils.addTypedParameter(types, this.Datetime = new TypedParameter<>(Long.class, DataType.DateTime.name(), 1L));
        Utils.addTypedParameter(types,
                this.Datetime64 = new TypedParameter<>(Long.class, DataType.DateTime64.name(), 1000L));

        Utils.addTypedParameter(types,
                this.Decimal = new TypedParameter<>(BigDecimal.class, DataType.Decimal.name(), BigDecimal.ZERO));
        Utils.addTypedParameter(types,
                this.Decimal32 = new TypedParameter<>(BigDecimal.class, DataType.Decimal32.name(), BigDecimal.ZERO));
        Utils.addTypedParameter(types,
                this.Decimal64 = new TypedParameter<>(BigDecimal.class, DataType.Decimal64.name(), BigDecimal.ZERO));
        Utils.addTypedParameter(types,
                this.Decimal128 = new TypedParameter<>(BigDecimal.class, DataType.Decimal128.name(), BigDecimal.ZERO));
        Utils.addTypedParameter(types,
                this.Decimal256 = new TypedParameter<>(BigDecimal.class, DataType.Decimal256.name(), BigDecimal.ZERO));

        Utils.addTypedParameter(types, this.Enum = new TypedParameter<>(Integer.class, DataType.Enum.name(), 0));
        Utils.addTypedParameter(types, this.Enum8 = new TypedParameter<>(Integer.class, DataType.Enum8.name(), 0));
        Utils.addTypedParameter(types, this.Enum16 = new TypedParameter<>(Integer.class, DataType.Enum16.name(), 0));
        Utils.addTypedParameter(types, this.IPv4 = new TypedParameter<>(Integer.class, DataType.IPv4.name(), 0));
        Utils.addTypedParameter(types,
                this.IPv6 = new TypedParameter<>(String.class, DataType.IPv6.name(), EMPTY_STRING));
        Utils.addTypedParameter(types,
                this.FixedStr = new TypedParameter<>(String.class, DataType.FixedStr.name(), EMPTY_STRING));
        Utils.addTypedParameter(types,
                this.Str = new TypedParameter<>(String.class, DataType.Str.name(), EMPTY_STRING));
        Utils.addTypedParameter(types,
                this.UUID = new TypedParameter<>(String.class, DataType.UUID.name(), DEFAULT_UUID));
    }

    public DefaultValues(JsonObject... params) {
        this();

        for (JsonObject p : params) {
            merge(p);
        }
    }

    public DefaultValues merge(JsonObject p) {
        if (p != null) {
            for (Entry<String, Object> entry : p) {
                String key = entry.getKey();
                String name = DataType.from(key).name();
                TypedParameter<?> tp = this.types.get(name);
                if (tp != null) {
                    tp.merge(p, key);
                }
            }
        }

        return this;
    }

    public TypedParameter<?> getTypedValue(DataType type) {
        TypedParameter<?> p = this.types.get(type.name());
        if (p == null) {
            throw new IllegalArgumentException("unsupported type: " + type.name());
        }
        return p;
    }

    public String asJsonString() {
        JsonObject obj = new JsonObject();

        for (Map.Entry<String, TypedParameter<?>> t : this.types.entrySet()) {
            Object value = t.getValue().getDefaultValue();
            obj.put(t.getKey(), value == null ? null : String.valueOf(value));
        }

        return obj.toString();
    }
}
