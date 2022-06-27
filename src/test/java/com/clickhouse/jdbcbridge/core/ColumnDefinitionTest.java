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

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.testng.annotations.Test;

import io.vertx.core.json.JsonObject;

public class ColumnDefinitionTest {
    @Test(groups = { "unit" })
    public void testConstructor() {
        String weirdName = "123`abc!$";
        DataType weirdType = DataType.DateTime;
        boolean nullableOrNot = false;
        ColumnDefinition c = new ColumnDefinition(weirdName, weirdType, nullableOrNot, 5, 3, 1);

        assertEquals(c.getName(), weirdName);
        assertEquals(c.getType(), weirdType);
        assertEquals(c.isNullable(), nullableOrNot);
        assertEquals(c.getLength(), weirdType.getLength());
        assertEquals(c.getPrecision(), 3);
        assertEquals(c.getScale(), 1);
        assertNull(c.getTimeZone());
        assertEquals(c.getValue(), Long.valueOf(1L));

        c = new ColumnDefinition(weirdName, DataType.FixedStr, nullableOrNot, 5, 3, 1);
        assertEquals(c.getName(), weirdName);
        assertEquals(c.getType(), DataType.FixedStr);
        assertEquals(c.isNullable(), nullableOrNot);
        assertEquals(c.getLength(), 5);
        assertEquals(c.getPrecision(), DataType.DEFAULT_PRECISION);
        assertEquals(c.getScale(), DataType.DEFAULT_SCALE);
        assertNull(c.getTimeZone());
        assertEquals(c.getValue(), "");

        Map<String, Integer> options = new LinkedHashMap<>();
        options.put("A1", 0);
        options.put("B2", 1);
        options.put("C3", 2);
        for (DataType t : new DataType[] { DataType.Enum, DataType.Enum8, DataType.Enum16 }) {
            c = new ColumnDefinition(weirdName, t, nullableOrNot, 5, 3, 1, "TZ", "2", options);
            assertEquals(c.getValue(), 2);
            assertEquals(c.getOptions(), options);
        }
    }

    @Test(groups = { "unit" })
    public void testParseOptions() {
        Map<String, Integer> options = new LinkedHashMap<>();
        options.put("A1", 0);
        options.put("B2", 1);
        options.put("C3", 2);
        assertEquals(ColumnDefinition.parseOptions("'A1'=0,'B2'=1,'C3'=2"), options);
        assertEquals(ColumnDefinition.parseOptions(" 'A1'= 0, 'B2' =1, 'C3' = 2"), options);
        assertEquals(ColumnDefinition.parseOptions("'A1','B2','C3'"), options);
        assertEquals(ColumnDefinition.parseOptions(" 'A1', 'B2' , 'C3'"), options);
        assertEquals(ColumnDefinition.parseOptions("A1,B2,C3"), options);
        assertEquals(ColumnDefinition.parseOptions(" A1, B2 , C3 "), options);
        assertEquals(ColumnDefinition.parseOptions(new Object[] { "A1", "B2", "C3" }), options);

        options.clear();
        options.put("A''\"\' 1=2", 3);
        options.put("'B''\" 2\' = 2 ", 4);
        assertEquals(ColumnDefinition.parseOptions("'A\\'\\'\"\\' 1=2'= 3, '\\'B\\'\\'\" 2\\' = 2 '=4"),
                options);

        // complex types
        options.clear();
        options.put("AAA", 0);
        assertEquals(ColumnDefinition.parseOptions(Collections.singleton("AAA")), options);
        assertEquals(ColumnDefinition.parseOptions(new HashMap<Object, Object>() {
            {
                put(null, null);
                put("A", null);
                put("AAA", "0");
            }
        }), options);
    }

    @Test(groups = { "unit" })
    public void testFromJson() {
        String name = "column1";
        DataType type = DataType.Str;
        boolean nullable = true;

        ColumnDefinition c = new ColumnDefinition(name, type, nullable, 0, 0, 0);

        JsonObject json = new JsonObject();
        assertEquals(ColumnDefinition.fromJson(json).getName(), ColumnDefinition.DEFAULT_NAME);
        assertEquals(ColumnDefinition.fromJson(json).getType(), ColumnDefinition.DEFAULT_TYPE);
        assertEquals(ColumnDefinition.fromJson(json).isNullable(), DataType.DEFAULT_NULLABLE);
        json.put("name", "");
        assertEquals(ColumnDefinition.fromJson(json).getName(), "");
        json.put("name", name);
        assertEquals(ColumnDefinition.fromJson(json).getName(), c.getName());
        json.put("type", DataType.Date.name());
        assertEquals(ColumnDefinition.fromJson(json).getType(), DataType.Date);
        json.put("type", type.name());
        assertEquals(ColumnDefinition.fromJson(json).getType(), c.getType());
        json.put("nullable", !nullable);
        assertEquals(ColumnDefinition.fromJson(json).isNullable(), !nullable);
        json.put("nullable", nullable);
        assertEquals(ColumnDefinition.fromJson(json), c);

        ColumnDefinition intCol = ColumnDefinition.fromJson(
                new JsonObject("{\"name\":\"int_col\",\"type\":\"Int32\",\"nullable\":false,\"value\":\"610000\"}"));
        ColumnDefinition strCol = ColumnDefinition.fromJson(
                new JsonObject("{\"name\":\"str_col\",\"type\":\"String\",\"nullable\":false,\"value\":\"Chengdu\"}"));
        ColumnDefinition enumCol = ColumnDefinition.fromJson(new JsonObject(
                "{\"name\":\"enum_col\",\"type\":\"Enum\",\"nullable\":false,\"value\":\"2\",\"options\": {\"A\": 1, \"B\": 2}}"));
        assertEquals(intCol.getValue(), Integer.valueOf("610000"));
        assertEquals(strCol.getValue(), "Chengdu");
        assertEquals(enumCol.getValue(), 2);
        assertEquals(enumCol.getOptions().size(), 2);
    }

    @Test(groups = { "unit" })
    public void testFromString() {
        String name = "column`1";
        DataType type = DataType.Str;
        boolean nullable = true;

        ColumnDefinition c = new ColumnDefinition(name, type, nullable, 0, 0, 0);
        String declarationWithQuote = "`column``1` Nullable(String)";
        String declarationWithoutQuote = "column`1 Nullable(String)";
        String declarationWithEscapedQuote = "`column\\`1` Nullable(String)";

        assertEquals(ColumnDefinition.fromString(declarationWithQuote), c);
        assertEquals(ColumnDefinition.fromString(declarationWithoutQuote), c);
        assertEquals(ColumnDefinition.fromString(declarationWithEscapedQuote), c);

        assertEquals(ColumnDefinition.fromString("cloumn1").getType(), type);
        assertEquals(ColumnDefinition.fromString("cloumn1 ").getType(), type);
        assertEquals(ColumnDefinition.fromString("cloumn1 String").getType(), type);
        assertEquals(ColumnDefinition.fromString("cloumn1 String").isNullable(), false);

        // now try some weird names
        String weirdName = "``cl`o``u`mn``";
        assertEquals(ColumnDefinition.fromString("`````cl``o````u``mn`````").getName(), weirdName);

        assertEquals(ColumnDefinition.fromString("f Decimal(19, 4)").toString(), "`f` Decimal(19,4)");
        assertEquals(ColumnDefinition.fromString("f Nullable(Decimal(19, 4))").toString(),
                "`f` Nullable(Decimal(19,4))");

        assertEquals(ColumnDefinition.fromString("d DateTime").toString(), "`d` DateTime");
        assertEquals(ColumnDefinition.fromString("d DateTime DEFAULT 1").toString(),
                ColumnDefinition.DEFAULT_VALUE_SUPPORT ? "`d` DateTime DEFAULT 1" : "`d` DateTime");
        assertEquals(ColumnDefinition.fromString("d String DEFAULT '1'").toString(),
                ColumnDefinition.DEFAULT_VALUE_SUPPORT ? "`d` String DEFAULT '1'" : "`d` String");
        assertEquals(ColumnDefinition.fromString("d Nullable(String) DEFAULT '1\\'2'").toString(),
                ColumnDefinition.DEFAULT_VALUE_SUPPORT ? "`d` Nullable(String) DEFAULT '1\\'2'"
                        : "`d` Nullable(String)");
        assertEquals(ColumnDefinition.fromString("`d 3` Nullable(String) DEFAULT '1\\'2'").toString(),
                ColumnDefinition.DEFAULT_VALUE_SUPPORT ? "`d 3` Nullable(String) DEFAULT '1\\'2'"
                        : "`d 3` Nullable(String)");
        assertEquals(ColumnDefinition.fromString("d Nullable(String) DEFAULT null").toString(),
                "`d` Nullable(String)");
        assertEquals(ColumnDefinition.fromString("d Nullable(DateTime)").toString(), "`d` Nullable(DateTime)");
        assertEquals(ColumnDefinition.fromString("d Nullable(DateTime) DEFAULT 1").toString(),
                ColumnDefinition.DEFAULT_VALUE_SUPPORT ? "`d` Nullable(DateTime) DEFAULT 1"
                        : "`d` Nullable(DateTime)");
        assertEquals(ColumnDefinition.fromString("d DateTime('Asia/Chongqing')").toString(),
                "`d` DateTime('Asia/Chongqing')");
        assertEquals(ColumnDefinition.fromString("d Nullable(DateTime('Asia/Chongqing'))").toString(),
                "`d` Nullable(DateTime('Asia/Chongqing'))");
        assertEquals(ColumnDefinition.fromString("d Nullable(DateTime64(2, 'Asia/Chongqing')) DEFAULT 1")
                .toString(),
                ColumnDefinition.DEFAULT_VALUE_SUPPORT
                        ? "`d` Nullable(DateTime64(2,'Asia/Chongqing')) DEFAULT 1"
                        : "`d` Nullable(DateTime64(2,'Asia/Chongqing'))");

        assertEquals(ColumnDefinition.fromString("d Nullable(Enum('A'=1, 'B'=2,'C'=3)) DEFAULT 2").toString(),
                ColumnDefinition.DEFAULT_VALUE_SUPPORT
                        ? "`d` Nullable(Enum('A'=1,'B'=2,'C'=3)) DEFAULT 2"
                        : "`d` Nullable(Enum('A'=1,'B'=2,'C'=3))");
    }

    @Test(groups = { "unit" })
    public void testDecimals() {
        ColumnDefinition c = new ColumnDefinition("d", DataType.Decimal, true, 0, 10, 3);
        assertEquals(c.toString(), "`d` Nullable(Decimal(10,3))");

        c = new ColumnDefinition("d", DataType.Decimal32, true, 0, 10, 3);
        assertEquals(c.toString(), "`d` Nullable(Decimal32(3))");

        c = new ColumnDefinition("d", DataType.Decimal64, true, 0, 10, 3);
        assertEquals(c.toString(), "`d` Nullable(Decimal64(3))");

        c = new ColumnDefinition("d", DataType.Decimal128, true, 0, 10, 3);
        assertEquals(c.toString(), "`d` Nullable(Decimal128(3))");

        // incorrect scale
        c = new ColumnDefinition("d", DataType.Decimal, true, 0, 10, 50);
        assertEquals(c.toString(), "`d` Nullable(Decimal(10,10))");

        c = new ColumnDefinition("d", DataType.Decimal32, true, 0, 10, 50);
        assertEquals(c.toString(), "`d` Nullable(Decimal32(9))");

        c = new ColumnDefinition("d", DataType.Decimal64, true, 0, 10, 50);
        assertEquals(c.toString(), "`d` Nullable(Decimal64(18))");

        c = new ColumnDefinition("d", DataType.Decimal128, true, 0, 10, 50);
        assertEquals(c.toString(), "`d` Nullable(Decimal128(38))");

        String types = "`d` Nullable(Decimal(7,3))";
        assertEquals(ColumnDefinition.fromString(types).toString(), types);
        types = "`d` Nullable(Decimal64(8))";
        assertEquals(ColumnDefinition.fromString(types).toString(), types);
    }
}