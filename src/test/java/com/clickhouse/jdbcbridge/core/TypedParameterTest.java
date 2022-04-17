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

import static org.testng.Assert.*;

import java.math.BigDecimal;

import org.testng.annotations.Test;

import io.vertx.core.json.JsonObject;

public class TypedParameterTest {
    @Test(groups = { "unit" })
    public void testBigDecimalParam() {
        String paramName = "pName";
        BigDecimal value = BigDecimal.valueOf(1.0D);
        BigDecimal defaultValue = BigDecimal.valueOf(2.0D);
        BigDecimal newValue = BigDecimal.valueOf(100.0D);
        BigDecimal jsonValue = BigDecimal.valueOf(7.0D);

        TypedParameter<BigDecimal> param = new TypedParameter<>(BigDecimal.class, paramName, defaultValue, value);
        TypedParameter<BigDecimal> newParam = new TypedParameter<>(BigDecimal.class, paramName, defaultValue, newValue);

        JsonObject json = new JsonObject("{\"" + paramName + "\": " + jsonValue + "}");
        JsonObject nullJson = new JsonObject("{\"" + paramName + "\": null}");
        JsonObject emptyJson = new JsonObject("{}");

        assertEquals(param.getName(), paramName);
        assertEquals(param.getDefaultValue(), defaultValue);
        assertEquals(param.getValue(), value);

        assertEquals(param.merge(String.valueOf(defaultValue)).getValue(), defaultValue);

        assertEquals(param.merge(newParam).getValue(), newValue);
        assertEquals(param.merge((TypedParameter) null).getValue(), newValue);
        assertEquals(param.merge((JsonObject) null).getValue(), newValue);

        assertEquals(param.merge(emptyJson).getValue(), newValue);
        assertEquals(param.merge(nullJson).getValue(), newValue);
        assertEquals(param.merge(json).getValue(), jsonValue);
    }

    @Test(groups = { "unit" })
    public void testDoubleParam() {
        String paramName = "pName";
        double value = 1.0D;
        double defaultValue = 2.0D;
        double newValue = 100.0D;
        double jsonValue = 7.0D;

        TypedParameter<Double> param = new TypedParameter<>(Double.class, paramName, defaultValue, value);
        TypedParameter<Double> newParam = new TypedParameter<>(Double.class, paramName, defaultValue, newValue);

        JsonObject json = new JsonObject("{\"" + paramName + "\": " + jsonValue + "}");
        JsonObject nullJson = new JsonObject("{\"" + paramName + "\": null}");
        JsonObject emptyJson = new JsonObject("{}");

        assertEquals(param.getName(), paramName);
        assertEquals(param.getDefaultValue().doubleValue(), defaultValue);
        assertEquals(param.getValue().doubleValue(), value);

        assertEquals(param.merge(String.valueOf(defaultValue)).getValue().doubleValue(), defaultValue);

        assertEquals(param.merge(newParam).getValue().doubleValue(), newValue);
        assertEquals(param.merge((TypedParameter) null).getValue(), newValue);
        assertEquals(param.merge((JsonObject) null).getValue().doubleValue(), newValue);

        assertEquals(param.merge(emptyJson).getValue().doubleValue(), newValue);
        assertEquals(param.merge(nullJson).getValue().doubleValue(), newValue);
        assertEquals(param.merge(json).getValue().doubleValue(), jsonValue);
    }

    @Test(groups = { "unit" })
    public void testFloatParam() {
        String paramName = "pName";
        float value = 1.0f;
        float defaultValue = 2.0f;
        float newValue = 100.0f;
        float jsonValue = 7.0f;

        TypedParameter<Float> param = new TypedParameter<>(Float.class, paramName, defaultValue, value);
        TypedParameter<Float> newParam = new TypedParameter<>(Float.class, paramName, defaultValue, newValue);

        JsonObject json = new JsonObject("{\"" + paramName + "\": " + jsonValue + "}");
        JsonObject nullJson = new JsonObject("{\"" + paramName + "\": null}");
        JsonObject emptyJson = new JsonObject("{}");

        assertEquals(param.getName(), paramName);
        assertEquals(param.getDefaultValue().floatValue(), defaultValue);
        assertEquals(param.getValue().floatValue(), value);

        assertEquals(param.merge(String.valueOf(defaultValue)).getValue().floatValue(), defaultValue);

        assertEquals(param.merge(newParam).getValue().floatValue(), newValue);
        assertEquals(param.merge((TypedParameter) null).getValue(), newValue);
        assertEquals(param.merge((JsonObject) null).getValue().floatValue(), newValue);

        assertEquals(param.merge(emptyJson).getValue().floatValue(), newValue);
        assertEquals(param.merge(nullJson).getValue().floatValue(), newValue);
        assertEquals(param.merge(json).getValue().floatValue(), jsonValue);
    }

    @Test(groups = { "unit" })
    public void testLongParam() {
        String paramName = "pName";
        Long value = 1L;
        Long defaultValue = 2L;
        Long newValue = 100L;
        Long jsonValue = 7L;

        TypedParameter<Long> param = new TypedParameter<>(Long.class, paramName, defaultValue, value);
        TypedParameter<Long> newParam = new TypedParameter<>(Long.class, paramName, defaultValue, newValue);

        JsonObject json = new JsonObject("{\"" + paramName + "\": " + jsonValue + "}");
        JsonObject nullJson = new JsonObject("{\"" + paramName + "\": null}");
        JsonObject emptyJson = new JsonObject("{}");

        assertEquals(param.getName(), paramName);
        assertEquals(param.getDefaultValue(), defaultValue);
        assertEquals(param.getValue(), value);

        assertEquals(param.merge(String.valueOf(defaultValue)).getValue(), defaultValue);

        assertEquals(param.merge(newParam).getValue(), newValue);
        assertEquals(param.merge((TypedParameter) null).getValue(), newValue);
        assertEquals(param.merge((JsonObject) null).getValue(), newValue);

        assertEquals(param.merge(emptyJson).getValue(), newValue);
        assertEquals(param.merge(nullJson).getValue(), newValue);
        assertEquals(param.merge(json).getValue(), jsonValue);
    }

    @Test(groups = { "unit" })
    public void testIntegerParam() {
        String paramName = "pName";
        Integer value = 1;
        Integer defaultValue = 2;
        Integer newValue = 100;
        Integer jsonValue = 7;

        TypedParameter<Integer> param = new TypedParameter<>(Integer.class, paramName, defaultValue, value);
        TypedParameter<Integer> newParam = new TypedParameter<>(Integer.class, paramName, defaultValue, newValue);
        TypedParameter<Short> shortParam = new TypedParameter<>(Short.class, paramName, defaultValue.shortValue(),
                newValue.shortValue());
        TypedParameter<Byte> byteParam = new TypedParameter<>(Byte.class, paramName, defaultValue.byteValue(),
                newValue.byteValue());

        JsonObject json = new JsonObject("{\"" + paramName + "\": " + jsonValue + "}");
        JsonObject nullJson = new JsonObject("{\"" + paramName + "\": null}");
        JsonObject emptyJson = new JsonObject("{}");

        assertEquals(param.getName(), paramName);
        assertEquals(param.getDefaultValue(), defaultValue);
        assertEquals(param.getValue(), value);

        assertEquals(param.merge(String.valueOf(defaultValue)).getValue(), defaultValue);

        assertEquals(shortParam.getDefaultValue(), (Short) defaultValue.shortValue());
        assertEquals(shortParam.getValue(), (Short) newValue.shortValue());
        assertEquals(byteParam.getDefaultValue(), (Byte) defaultValue.byteValue());
        assertEquals(byteParam.getValue(), (Byte) newValue.byteValue());

        assertEquals(param.merge(newParam).getValue(), newValue);
        assertEquals(param.merge((TypedParameter) null).getValue(), newValue);
        assertEquals(param.merge((JsonObject) null).getValue(), newValue);

        assertEquals(param.merge(emptyJson).getValue(), newValue);
        assertEquals(param.merge(nullJson).getValue(), newValue);
        assertEquals(param.merge(json).getValue(), jsonValue);
    }

    @Test(groups = { "unit" })
    public void testBooleanParam() {
        String paramName = "pName";
        Boolean value = true;
        Boolean defaultValue = false;
        Boolean newValue = false;
        Boolean jsonValue = true;

        TypedParameter<Boolean> param = new TypedParameter<>(Boolean.class, paramName, defaultValue, value);
        TypedParameter<Boolean> newParam = new TypedParameter<>(Boolean.class, paramName, defaultValue, newValue);

        JsonObject json = new JsonObject("{\"" + paramName + "\": " + jsonValue + "}");
        JsonObject nullJson = new JsonObject("{\"" + paramName + "\": null}");
        JsonObject emptyJson = new JsonObject("{}");

        assertEquals(param.getName(), paramName);
        assertEquals(param.getDefaultValue(), defaultValue);
        assertEquals(param.getValue(), value);

        assertEquals(param.merge(String.valueOf(defaultValue)).getValue(), defaultValue);

        assertEquals(param.merge(newParam).getValue(), newValue);
        assertEquals(param.merge((TypedParameter) null).getValue(), newValue);
        assertEquals(param.merge((JsonObject) null).getValue(), newValue);

        assertEquals(param.merge(emptyJson).getValue(), newValue);
        assertEquals(param.merge(nullJson).getValue(), newValue);
        assertEquals(param.merge(json).getValue(), jsonValue);
    }

    @Test(groups = { "unit" })
    public void testStringParam() {
        String paramName = "pName";
        String value = "value";
        String defaultValue = "defaultValue";
        String newValue = "newValue";
        String jsonValue = "jsonValue";

        TypedParameter<String> param = new TypedParameter<>(String.class, paramName, defaultValue, value);
        TypedParameter<String> newParam = new TypedParameter<>(String.class, paramName, defaultValue, newValue);
        TypedParameter<Character> charParam = new TypedParameter<>(Character.class, paramName, 'A', 'B');

        JsonObject json = new JsonObject("{\"" + paramName + "\": \"" + jsonValue + "\"}");
        JsonObject nullJson = new JsonObject("{\"" + paramName + "\": null}");
        JsonObject emptyJson = new JsonObject("{}");

        assertEquals(param.getName(), paramName);
        assertEquals(param.getDefaultValue(), defaultValue);
        assertEquals(param.getValue(), value);

        assertEquals(param.merge(String.valueOf(defaultValue)).getValue(), defaultValue);

        assertEquals(charParam.getDefaultValue(), (Character) 'A');
        assertEquals(charParam.getValue(), (Character) 'B');

        assertEquals(param.merge(newParam).getValue(), newValue);
        assertEquals(param.merge((TypedParameter) null).getValue(), newValue);
        assertEquals(param.merge((JsonObject) null).getValue(), newValue);

        assertEquals(param.merge(emptyJson).getValue(), newValue);
        assertEquals(param.merge(nullJson).getValue(), newValue);
        assertEquals(param.merge(json).getValue(), jsonValue);
    }
}