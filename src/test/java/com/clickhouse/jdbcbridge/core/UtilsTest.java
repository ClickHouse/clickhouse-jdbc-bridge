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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import org.testng.annotations.Test;

import io.vertx.core.json.JsonObject;

public class UtilsTest {
    private void assertArrayEquals(Object[][] actual, Object[][] expected) {
        if (actual == expected) {
            return;
        }

        // use assertNull wthis assumes expected is always non-null
        assertNotNull(actual);

        assertEquals(actual.length, expected.length, "different row count");
        for (int i = 0; i < actual.length; i++) {
            Object[] a = actual[i];
            Object[] e = expected[i];

            if (a != e) { // not identical instance
                assertEquals(a.length, e.length, "different column count at row #" + i);
                for (int j = 0; j < a.length; j++) {
                    assertEquals(a[j], e[j], "different cell at row #" + i + " column #" + j);
                }
            }
        }
    }

    @Test(groups = { "unit" })
    public void testIndexOfKeyword() {
        assertEquals(Utils.indexOfKeyword("a UInt32 Default", null, true), -1);
        assertEquals(Utils.indexOfKeyword("a UInt32 Default", null, false), -1);
        assertEquals(Utils.indexOfKeyword(null, "keyword", true), -1);
        assertEquals(Utils.indexOfKeyword(null, "keyword", false), -1);
        assertEquals(Utils.indexOfKeyword(null, null, true), -1);
        assertEquals(Utils.indexOfKeyword(null, null, false), -1);

        assertEquals(Utils.indexOfKeyword("a UInt32 default", "default", true), 9);
        assertEquals(Utils.indexOfKeyword("a UInt32 Default", "default", false), -1);
        assertEquals(Utils.indexOfKeyword("a UInt32 Default", "default", true), 9);
        assertEquals(Utils.indexOfKeyword("a UInt32 Default", "default ", true), -1);

        assertEquals(Utils.indexOfKeyword("`default_field` UInt32", "default", true), -1);
        assertEquals(Utils.indexOfKeyword("`default_field` UInt32", "default", false), -1);

        assertEquals(Utils.indexOfKeyword("`default_field` Enum(1='default', 2='non-default')", "default",
                true), -1);
        assertEquals(Utils.indexOfKeyword("`default_field` Enum(1='default', 2='non-default')", "default",
                false), -1);

        assertEquals(
                Utils.indexOfKeyword("`default_field` Enum(1='default', 2='non-default') Default 1",
                        "default", true),
                51);
        assertEquals(
                Utils.indexOfKeyword("`default_field` Enum(1='default', 2='non-default') Default 1",
                        "default", false),
                -1);
    }

    @Test(groups = { "unit" })
    public void testUnescapeQuotes() {
        assertEquals(Utils.unescapeQuotes(null), "");
        assertEquals(Utils.unescapeQuotes(""), "");
        assertEquals(Utils.unescapeQuotes("'"), "'");
        assertEquals(Utils.unescapeQuotes("\\"), "\\");
        assertEquals(Utils.unescapeQuotes("''"), "'");
        assertEquals(Utils.unescapeQuotes("\\'"), "'");
        assertEquals(Utils.unescapeQuotes("\\\\"), "\\\\");

        assertEquals(Utils.unescapeQuotes("a''\\b''c'"), "a'\\b'c'");
        assertEquals(Utils.unescapeQuotes("a\\'b\\'c\\"), "a'b'c\\");
    }

    @Test(groups = { "unit" })
    public void testDigest() {
        JsonObject obj = new JsonObject("{\"b\": {\"x\": \"y\"}, \"a\": 1}");

        String nullString = null;
        String emptyString = "";
        String jsonString = obj.encode();

        assertEquals(Utils.digest(nullString), Utils.EMPTY_STRING);
        assertEquals(Utils.digest(emptyString), Utils.EMPTY_STRING);
        assertEquals(Utils.digest(jsonString),
                "4fe27ca694db4391c6b2f199b24b64b5d632f8f94a4992e95350ee05e9150d04feba17d7486ff487e7fd2f5f7884d1c34d0816305c971bc22cb63c9e24e085bd");

        assertEquals(Utils.digest(obj), Utils.digest(jsonString));
        obj.remove("a");
        obj.put("a", 1);
        assertEquals(Utils.digest(obj), Utils.digest(jsonString));
        obj.put("a", "1");
        assertNotEquals(Utils.digest(obj), Utils.digest(jsonString));
    }

    @Test(groups = { "unit" })
    public void testSplitByChar() {
        String str = "a=1&b=2&& c=3&";
        char delimiter = '&';

        assertEquals(Utils.splitByChar(str, delimiter), Utils.splitByChar(str, delimiter, true));

        List<String> matches = Utils.splitByChar(str, delimiter, false);
        assertEquals(String.join(String.valueOf(delimiter), matches), str);
    }

    @Test(groups = { "unit" })
    public void testLoadJson() {
        JsonObject expected = new JsonObject("{\"b\": {\"x\": \"y\"}, \"a\": 1}");

        JsonObject obj = Utils.loadJsonFromFile("src/test/resources/test.json");
        assertEquals(obj.encode(), expected.encode());

        // now try a file does not exist...
        obj = Utils.loadJsonFromFile("src/test/resources/file_does_not_exist.json");
        assertNotNull(obj);
    }

    @Test(groups = { "unit" })
    public void testApplyVariables() {
        String nullTemplate = null;
        String emptyTemplate = "";
        String templateWithoutVariable = "test template without any variable";
        String var0Template = Utils.VARIABLE_PREFIX + Utils.VARIABLE_SUFFIX;
        String var1Template = "template: {{ var #1 }}";
        String var2Template = "{{var2}}";
        String var3Template = "template: {{{{var3}}}}";
        String var4Template = "{{var2}} {{ {{var3 {{var #1}}";

        Map<String, String> nullMap = null;
        Map<String, String> emptyMap = new HashMap<String, String>();
        Map<String, String> varsMap = new HashMap<String, String>();
        varsMap.put("var #1", "value 1");
        varsMap.put("var2", "value 2");
        varsMap.put("{{var3", "value 3");

        assertEquals(Utils.applyVariables(nullTemplate, nullMap), emptyTemplate);
        assertEquals(Utils.applyVariables(nullTemplate, emptyMap), emptyTemplate);
        assertEquals(Utils.applyVariables(emptyTemplate, nullMap), emptyTemplate);
        assertEquals(Utils.applyVariables(emptyTemplate, emptyMap), emptyTemplate);
        assertEquals(Utils.applyVariables(nullTemplate, varsMap), emptyTemplate);
        assertEquals(Utils.applyVariables(emptyTemplate, varsMap), emptyTemplate);
        assertEquals(Utils.applyVariables(Utils.VARIABLE_PREFIX, varsMap), Utils.VARIABLE_PREFIX);

        assertEquals(Utils.applyVariables(emptyTemplate, nullMap), emptyTemplate);
        assertEquals(Utils.applyVariables(emptyTemplate, emptyMap), emptyTemplate);
        assertEquals(Utils.applyVariables(templateWithoutVariable, varsMap), templateWithoutVariable);

        assertEquals(Utils.applyVariables(var0Template, varsMap), var0Template);
        assertEquals(Utils.applyVariables(var1Template, varsMap), "template: value 1");
        assertEquals(Utils.applyVariables(var2Template, varsMap), "value 2");
        assertEquals(Utils.applyVariables(var3Template, varsMap), "template: value 3}}");
        assertEquals(Utils.applyVariables(var4Template, varsMap), "value 2 {{ {{var3 value 1");
    }

    @Test(groups = { "unit" })
    public void testSingleJavaObjectToArrays() {
        String[] singleColumn = new String[] { "result" };
        String[] multiColumns = new String[] { "a", "b" };

        // primitive types
        assertArrayEquals(Utils.toObjectArrays('C'), new Object[][] { new Object[] { 'C' } });
        assertArrayEquals(Utils.toObjectArrays('C', singleColumn), new Object[][] { new Object[] { 'C' } });
        assertArrayEquals(Utils.toObjectArrays('C', multiColumns),
                new Object[][] { new Object[] { 'C', null } });

        assertArrayEquals(Utils.toObjectArrays((boolean) false),
                new Object[][] { new Object[] { (boolean) false } });
        assertArrayEquals(Utils.toObjectArrays((boolean) false, singleColumn),
                new Object[][] { new Object[] { (boolean) false } });
        assertArrayEquals(Utils.toObjectArrays((boolean) false, multiColumns),
                new Object[][] { new Object[] { (boolean) false, null } });

        assertArrayEquals(Utils.toObjectArrays((byte) 5), new Object[][] { new Object[] { (byte) 5 } });
        assertArrayEquals(Utils.toObjectArrays((byte) 5, singleColumn),
                new Object[][] { new Object[] { (byte) 5 } });
        assertArrayEquals(Utils.toObjectArrays((byte) 5, multiColumns),
                new Object[][] { new Object[] { (byte) 5, null } });

        assertArrayEquals(Utils.toObjectArrays((short) 5), new Object[][] { new Object[] { (short) 5 } });
        assertArrayEquals(Utils.toObjectArrays((short) 5, singleColumn),
                new Object[][] { new Object[] { (short) 5 } });
        assertArrayEquals(Utils.toObjectArrays((short) 5, multiColumns),
                new Object[][] { new Object[] { (short) 5, null } });

        assertArrayEquals(Utils.toObjectArrays((int) 5), new Object[][] { new Object[] { (int) 5 } });
        assertArrayEquals(Utils.toObjectArrays((int) 5, singleColumn),
                new Object[][] { new Object[] { (int) 5 } });
        assertArrayEquals(Utils.toObjectArrays((int) 5, multiColumns),
                new Object[][] { new Object[] { (int) 5, null } });

        assertArrayEquals(Utils.toObjectArrays((long) 5), new Object[][] { new Object[] { (long) 5 } });
        assertArrayEquals(Utils.toObjectArrays((long) 5, singleColumn),
                new Object[][] { new Object[] { (long) 5 } });
        assertArrayEquals(Utils.toObjectArrays((long) 5, multiColumns),
                new Object[][] { new Object[] { (long) 5, null } });

        assertArrayEquals(Utils.toObjectArrays((float) 5.5), new Object[][] { new Object[] { (float) 5.5 } });
        assertArrayEquals(Utils.toObjectArrays((float) 5.5, singleColumn),
                new Object[][] { new Object[] { (float) 5.5 } });
        assertArrayEquals(Utils.toObjectArrays((float) 5.5, multiColumns),
                new Object[][] { new Object[] { (float) 5.5, null } });

        assertArrayEquals(Utils.toObjectArrays((double) 5.55),
                new Object[][] { new Object[] { (double) 5.55 } });
        assertArrayEquals(Utils.toObjectArrays((double) 5.55, singleColumn),
                new Object[][] { new Object[] { (double) 5.55 } });
        assertArrayEquals(Utils.toObjectArrays((double) 5.55, multiColumns),
                new Object[][] { new Object[] { (double) 5.55, null } });

        // string
        assertArrayEquals(Utils.toObjectArrays("5x5x"), new Object[][] { new Object[] { "5x5x" } });
        assertArrayEquals(Utils.toObjectArrays("5x5x", singleColumn),
                new Object[][] { new Object[] { "5x5x" } });
        assertArrayEquals(Utils.toObjectArrays("5x5x", multiColumns),
                new Object[][] { new Object[] { "5x5x", null } });

        // null & empty
        assertArrayEquals(Utils.toObjectArrays(null), new Object[0][0]);
        assertArrayEquals(Utils.toObjectArrays(null, singleColumn), new Object[0][0]);
        assertArrayEquals(Utils.toObjectArrays(null, multiColumns), new Object[0][0]);

        assertArrayEquals(Utils.toObjectArrays(new Object[0]), new Object[0][0]);
        assertArrayEquals(Utils.toObjectArrays(new Object[0], singleColumn), new Object[0][0]);
        assertArrayEquals(Utils.toObjectArrays(new Object[0], multiColumns), new Object[0][0]);

        // object
        Object value = new Object();
        assertArrayEquals(Utils.toObjectArrays(value), new Object[][] { new Object[] { value } });
        assertArrayEquals(Utils.toObjectArrays(value, singleColumn), new Object[][] { new Object[] { value } });
        assertArrayEquals(Utils.toObjectArrays(value, multiColumns),
                new Object[][] { new Object[] { value, null } });

        // enumeration & iterable
        assertArrayEquals(Utils.toObjectArrays(Collections.emptyEnumeration()), new Object[0][0]);
        assertArrayEquals(Utils.toObjectArrays(Collections.emptyEnumeration(), singleColumn), new Object[0][0]);
        assertArrayEquals(Utils.toObjectArrays(Collections.emptyEnumeration(), multiColumns), new Object[0][0]);

        assertArrayEquals(Utils.toObjectArrays(Collections.enumeration(Collections.singletonList(5))),
                new Object[][] { new Object[] { 5 } });
        assertArrayEquals(
                Utils.toObjectArrays(Collections.enumeration(Collections.singletonList(5)),
                        singleColumn),
                new Object[][] { new Object[] { 5 } });
        assertArrayEquals(
                Utils.toObjectArrays(Collections.enumeration(Collections.singletonList(5)),
                        multiColumns),
                new Object[][] { new Object[] { 5, null } });

        assertArrayEquals(Utils.toObjectArrays(Collections.emptyList()), new Object[0][0]);
        assertArrayEquals(Utils.toObjectArrays(Collections.emptyList(), singleColumn), new Object[0][0]);
        assertArrayEquals(Utils.toObjectArrays(Collections.emptyList(), multiColumns), new Object[0][0]);

        assertArrayEquals(Utils.toObjectArrays(Collections.singletonList(5)),
                new Object[][] { new Object[] { 5 } });
        assertArrayEquals(Utils.toObjectArrays(Collections.singletonList(5), singleColumn),
                new Object[][] { new Object[] { 5 } });
        assertArrayEquals(Utils.toObjectArrays(Collections.singletonList(5), multiColumns),
                new Object[][] { new Object[] { 5, null } });

        assertArrayEquals(Utils.toObjectArrays(Collections.emptySet()), new Object[0][0]);
        assertArrayEquals(Utils.toObjectArrays(Collections.emptySet(), singleColumn), new Object[0][0]);
        assertArrayEquals(Utils.toObjectArrays(Collections.emptySet(), multiColumns), new Object[0][0]);

        assertArrayEquals(Utils.toObjectArrays(Collections.singleton(5)),
                new Object[][] { new Object[] { 5 } });
        assertArrayEquals(Utils.toObjectArrays(Collections.singleton(5), singleColumn),
                new Object[][] { new Object[] { 5 } });
        assertArrayEquals(Utils.toObjectArrays(Collections.singleton(5), multiColumns),
                new Object[][] { new Object[] { 5, null } });

        // map
        value = Collections.emptyMap();
        assertArrayEquals(Utils.toObjectArrays(value), new Object[][] { new Object[] { value } });
        assertArrayEquals(Utils.toObjectArrays(value, singleColumn), new Object[][] { new Object[] { value } });
        assertArrayEquals(Utils.toObjectArrays(value, multiColumns),
                new Object[][] { new Object[] { null, null } });

        value = Collections.singletonMap("b", 6);
        assertArrayEquals(Utils.toObjectArrays(value), new Object[][] { new Object[] { value } });
        assertArrayEquals(Utils.toObjectArrays(value, singleColumn), new Object[][] { new Object[] { value } });
        assertArrayEquals(Utils.toObjectArrays(value, multiColumns),
                new Object[][] { new Object[] { null, 6 } });
    }

    @Test(groups = { "unit" })
    public void testMultipleJavaObjectsToArrays() {
        String[] singleColumn = new String[] { "result" };
        String[] multiColumns = new String[] { "a", "b" };

        // primitive types
        Object value = new char[] { 'C', 'd' };
        assertArrayEquals(Utils.toObjectArrays(value),
                new Object[][] { new Object[] { 'C' }, new Object[] { 'd' } });
        assertArrayEquals(Utils.toObjectArrays(value, singleColumn),
                new Object[][] { new Object[] { 'C' }, new Object[] { 'd' } });
        assertArrayEquals(Utils.toObjectArrays(value, multiColumns),
                new Object[][] { new Object[] { 'C', 'd' } });

        value = new boolean[] { false, true };
        assertArrayEquals(Utils.toObjectArrays(value),
                new Object[][] { new Object[] { false }, new Object[] { true } });
        assertArrayEquals(Utils.toObjectArrays(value, singleColumn),
                new Object[][] { new Object[] { false }, new Object[] { true } });
        assertArrayEquals(Utils.toObjectArrays(value, multiColumns),
                new Object[][] { new Object[] { false, true } });

        value = new byte[] { (byte) 5, (byte) 6 };
        assertArrayEquals(Utils.toObjectArrays(value),
                new Object[][] { new Object[] { (byte) 5 }, new Object[] { (byte) 6 } });
        assertArrayEquals(Utils.toObjectArrays(value, singleColumn),
                new Object[][] { new Object[] { (byte) 5 }, new Object[] { (byte) 6 } });
        assertArrayEquals(Utils.toObjectArrays(value, multiColumns),
                new Object[][] { new Object[] { (byte) 5, (byte) 6 } });

        value = new short[] { (short) 5, (short) 6 };
        assertArrayEquals(Utils.toObjectArrays(value),
                new Object[][] { new Object[] { (short) 5 }, new Object[] { (short) 6 } });
        assertArrayEquals(Utils.toObjectArrays(value, singleColumn),
                new Object[][] { new Object[] { (short) 5 }, new Object[] { (short) 6 } });
        assertArrayEquals(Utils.toObjectArrays(value, multiColumns),
                new Object[][] { new Object[] { (short) 5, (short) 6 } });

        value = new int[] { (int) 5, (int) 6 };
        assertArrayEquals(Utils.toObjectArrays(value),
                new Object[][] { new Object[] { (int) 5 }, new Object[] { (int) 6 } });
        assertArrayEquals(Utils.toObjectArrays(value, singleColumn),
                new Object[][] { new Object[] { (int) 5 }, new Object[] { (int) 6 } });
        assertArrayEquals(Utils.toObjectArrays(value, multiColumns),
                new Object[][] { new Object[] { (int) 5, (int) 6 } });

        value = new long[] { (long) 5, (long) 6 };
        assertArrayEquals(Utils.toObjectArrays(value),
                new Object[][] { new Object[] { (long) 5 }, new Object[] { (long) 6 } });
        assertArrayEquals(Utils.toObjectArrays(value, singleColumn),
                new Object[][] { new Object[] { (long) 5 }, new Object[] { (long) 6 } });
        assertArrayEquals(Utils.toObjectArrays(value, multiColumns),
                new Object[][] { new Object[] { (long) 5, (long) 6 } });

        value = new float[] { (float) 5.5, (float) 6.6 };
        assertArrayEquals(Utils.toObjectArrays(value),
                new Object[][] { new Object[] { (float) 5.5 }, new Object[] { (float) 6.6 } });
        assertArrayEquals(Utils.toObjectArrays(value, singleColumn),
                new Object[][] { new Object[] { (float) 5.5 }, new Object[] { (float) 6.6 } });
        assertArrayEquals(Utils.toObjectArrays(value, multiColumns),
                new Object[][] { new Object[] { (float) 5.5, (float) 6.6 } });

        value = new double[] { (double) 5.55, (double) 6.66 };
        assertArrayEquals(Utils.toObjectArrays(value),
                new Object[][] { new Object[] { (double) 5.55 }, new Object[] { (double) 6.66 } });
        assertArrayEquals(Utils.toObjectArrays(value, singleColumn),
                new Object[][] { new Object[] { (double) 5.55 }, new Object[] { (double) 6.66 } });
        assertArrayEquals(Utils.toObjectArrays(value, multiColumns),
                new Object[][] { new Object[] { (double) 5.55, (double) 6.66 } });

        // string
        value = new String[] { "5x5x", "x6x6" };
        assertArrayEquals(Utils.toObjectArrays(value),
                new Object[][] { new Object[] { "5x5x" }, new Object[] { "x6x6" } });
        assertArrayEquals(Utils.toObjectArrays(value, singleColumn),
                new Object[][] { new Object[] { "5x5x" }, new Object[] { "x6x6" } });
        assertArrayEquals(Utils.toObjectArrays(value, multiColumns),
                new Object[][] { new Object[] { "5x5x", "x6x6" } });

        // null & empty
        value = new Object[] { null, null };
        assertArrayEquals(Utils.toObjectArrays(value),
                new Object[][] { new Object[] { null }, new Object[] { null } });
        assertArrayEquals(Utils.toObjectArrays(value, singleColumn),
                new Object[][] { new Object[] { null }, new Object[] { null } });
        assertArrayEquals(Utils.toObjectArrays(value, multiColumns),
                new Object[][] { new Object[] { null, null } });

        assertArrayEquals(Utils.toObjectArrays(new Object[0][0]), new Object[0][0]);
        assertArrayEquals(Utils.toObjectArrays(new Object[0][0], singleColumn), new Object[0][0]);
        assertArrayEquals(Utils.toObjectArrays(new Object[0][0], multiColumns), new Object[0][0]);

        // object
        value = new Object[] { new Object(), new Object() };
        assertArrayEquals(Utils.toObjectArrays(value),
                new Object[][] { new Object[] { ((Object[]) value)[0] },
                        new Object[] { ((Object[]) value)[1] } });
        assertArrayEquals(Utils.toObjectArrays(value, singleColumn),
                new Object[][] { new Object[] { ((Object[]) value)[0] },
                        new Object[] { ((Object[]) value)[1] } });
        assertArrayEquals(Utils.toObjectArrays(value, multiColumns),
                new Object[][] { new Object[] { ((Object[]) value)[0], ((Object[]) value)[1] } });

        // enumeration & iterable
        value = new ArrayList<Integer>() {
            {
                add(5);
                add(6);
            }
        };
        assertArrayEquals(Utils.toObjectArrays(Collections.enumeration((List<?>) value)),
                new Object[][] { new Object[] { 5 }, new Object[] { 6 } });
        assertArrayEquals(Utils.toObjectArrays(Collections.enumeration((List<?>) value), singleColumn),
                new Object[][] { new Object[] { 5 }, new Object[] { 6 } });
        assertArrayEquals(Utils.toObjectArrays(Collections.enumeration((List<?>) value), multiColumns),
                new Object[][] { new Object[] { 5, 6 } });

        assertArrayEquals(Utils.toObjectArrays(value),
                new Object[][] { new Object[] { 5 }, new Object[] { 6 } });
        assertArrayEquals(Utils.toObjectArrays(value, singleColumn),
                new Object[][] { new Object[] { 5 }, new Object[] { 6 } });
        assertArrayEquals(Utils.toObjectArrays(value, multiColumns), new Object[][] { new Object[] { 5, 6 } });

        value = new HashSet<>(Arrays.asList(5, 6));
        assertArrayEquals(Utils.toObjectArrays(value),
                new Object[][] { new Object[] { 5 }, new Object[] { 6 } });
        assertArrayEquals(Utils.toObjectArrays(value, singleColumn),
                new Object[][] { new Object[] { 5 }, new Object[] { 6 } });
        assertArrayEquals(Utils.toObjectArrays(value, multiColumns), new Object[][] { new Object[] { 5, 6 } });

        // map
        value = new HashMap<String, Integer>() {
            {
                put("result", 2);
                put("results", 3);
                put("b", 5);
                put("c", 6);
            }
        };
        assertArrayEquals(Utils.toObjectArrays(value), new Object[][] { new Object[] { value } });
        assertArrayEquals(Utils.toObjectArrays(value, singleColumn), new Object[][] { new Object[] { value } });
        assertArrayEquals(Utils.toObjectArrays(value, multiColumns),
                new Object[][] { new Object[] { null, 5 } });

        // mixed & nested
        Object tmpValue = value;
        value = new Object[] { tmpValue, Collections.singleton(Arrays.asList("5", 6)) };
        assertArrayEquals(Utils.toObjectArrays(value),
                new Object[][] { new Object[] { tmpValue }, new Object[] { "5" } });
        assertArrayEquals(Utils.toObjectArrays(value, singleColumn),
                new Object[][] { new Object[] { tmpValue }, new Object[] { "5" } });
        assertArrayEquals(Utils.toObjectArrays(value, multiColumns),
                new Object[][] { new Object[] { null, 5 }, new Object[] { "5", 6 } });

        value = new Object[] { Arrays.asList(5, 6), Collections.singleton(Arrays.asList("5", 6)) };
        assertArrayEquals(Utils.toObjectArrays(value),
                new Object[][] { new Object[] { 5 }, new Object[] { "5" } });
        assertArrayEquals(Utils.toObjectArrays(value, singleColumn),
                new Object[][] { new Object[] { 5 }, new Object[] { "5" } });
        assertArrayEquals(Utils.toObjectArrays(value, multiColumns),
                new Object[][] { new Object[] { 5, 6 }, new Object[] { "5", 6 } });
    }

    @Test(groups = { "sit" })
    public void testBindingsToArrays() throws Exception {
        // this won't work in JDK 15+ as Nashron has been removed
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByExtension("js");

        String[] singleColumn = new String[] { "result" };
        String[] multiColumns = new String[] { "a", "b" };

        // primitive types
        assertArrayEquals(Utils.toObjectArrays(engine.eval("false")),
                new Object[][] { new Object[] { false } });
        assertArrayEquals(Utils.toObjectArrays(engine.eval("false"), singleColumn),
                new Object[][] { new Object[] { false } });
        assertArrayEquals(Utils.toObjectArrays(engine.eval("false"), multiColumns),
                new Object[][] { new Object[] { false, null } });
        assertArrayEquals(Utils.toObjectArrays(engine.eval("5")), new Object[][] { new Object[] { 5 } });
        assertArrayEquals(Utils.toObjectArrays(engine.eval("5"), singleColumn),
                new Object[][] { new Object[] { 5 } });
        assertArrayEquals(Utils.toObjectArrays(engine.eval("5"), multiColumns),
                new Object[][] { new Object[] { 5, null } });
        assertArrayEquals(Utils.toObjectArrays(engine.eval("5.5")), new Object[][] { new Object[] { 5.5 } });
        assertArrayEquals(Utils.toObjectArrays(engine.eval("5.5"), singleColumn),
                new Object[][] { new Object[] { 5.5 } });
        assertArrayEquals(Utils.toObjectArrays(engine.eval("5.5"), multiColumns),
                new Object[][] { new Object[] { 5.5, null } });

        assertArrayEquals(Utils.toObjectArrays(engine.eval("[false, 5, 5.5]")),
                new Object[][] { new Object[] { false }, new Object[] { 5 }, new Object[] { 5.5 } });
        assertArrayEquals(Utils.toObjectArrays(engine.eval("[false, 5, 5.5]"), singleColumn),
                new Object[][] { new Object[] { false }, new Object[] { 5 }, new Object[] { 5.5 } });
        assertArrayEquals(Utils.toObjectArrays(engine.eval("[false, 5, 5.5]"), multiColumns),
                new Object[][] { new Object[] { false, 5 } });
        assertArrayEquals(Utils.toObjectArrays(engine.eval("Java.to([false, true], \"boolean[]\")")),
                new Object[][] { new Object[] { false }, new Object[] { true } });
        assertArrayEquals(
                Utils.toObjectArrays(engine.eval("Java.to([false, true], \"boolean[]\")"),
                        singleColumn),
                new Object[][] { new Object[] { false }, new Object[] { true } });
        assertArrayEquals(
                Utils.toObjectArrays(engine.eval("Java.to([false, true], \"boolean[]\")"),
                        multiColumns),
                new Object[][] { new Object[] { false, true } });

        assertArrayEquals(Utils.toObjectArrays(engine.eval("Java.to([5,6], \"byte[]\")")),
                new Object[][] { new Object[] { (byte) 5 }, new Object[] { (byte) 6 } });
        assertArrayEquals(Utils.toObjectArrays(engine.eval("Java.to([5,6], \"byte[]\")"), singleColumn),
                new Object[][] { new Object[] { (byte) 5 }, new Object[] { (byte) 6 } });
        assertArrayEquals(Utils.toObjectArrays(engine.eval("Java.to([5,6], \"byte[]\")"), multiColumns),
                new Object[][] { new Object[] { (byte) 5, (byte) 6 } });
        assertArrayEquals(Utils.toObjectArrays(engine.eval("Java.to([5,6], \"short[]\")")),
                new Object[][] { new Object[] { (short) 5 }, new Object[] { (short) 6 } });
        assertArrayEquals(Utils.toObjectArrays(engine.eval("Java.to([5,6], \"short[]\")"), singleColumn),
                new Object[][] { new Object[] { (short) 5 }, new Object[] { (short) 6 } });
        assertArrayEquals(Utils.toObjectArrays(engine.eval("Java.to([5,6], \"short[]\")"), multiColumns),
                new Object[][] { new Object[] { (short) 5, (short) 6 } });
        assertArrayEquals(Utils.toObjectArrays(engine.eval("Java.to([5,6], \"int[]\")")),
                new Object[][] { new Object[] { (int) 5 }, new Object[] { (int) 6 } });
        assertArrayEquals(Utils.toObjectArrays(engine.eval("Java.to([5,6], \"int[]\")"), singleColumn),
                new Object[][] { new Object[] { (int) 5 }, new Object[] { (int) 6 } });
        assertArrayEquals(Utils.toObjectArrays(engine.eval("Java.to([5,6], \"int[]\")"), multiColumns),
                new Object[][] { new Object[] { (int) 5, (int) 6 } });
        assertArrayEquals(Utils.toObjectArrays(engine.eval("Java.to([5,6], \"long[]\")")),
                new Object[][] { new Object[] { (long) 5 }, new Object[] { (long) 6 } });
        assertArrayEquals(Utils.toObjectArrays(engine.eval("Java.to([5,6], \"long[]\")"), singleColumn),
                new Object[][] { new Object[] { (long) 5 }, new Object[] { (long) 6 } });
        assertArrayEquals(Utils.toObjectArrays(engine.eval("Java.to([5,6], \"long[]\")"), multiColumns),
                new Object[][] { new Object[] { (long) 5, (long) 6 } });
        assertArrayEquals(Utils.toObjectArrays(engine.eval("Java.to([5.5,6.6], \"float[]\")")),
                new Object[][] { new Object[] { (float) 5.5 }, new Object[] { (float) 6.6 } });
        assertArrayEquals(Utils.toObjectArrays(engine.eval("Java.to([5.5,6.6], \"float[]\")"), singleColumn),
                new Object[][] { new Object[] { (float) 5.5 }, new Object[] { (float) 6.6 } });
        assertArrayEquals(Utils.toObjectArrays(engine.eval("Java.to([5.5,6.6], \"float[]\")"), multiColumns),
                new Object[][] { new Object[] { (float) 5.5, (float) 6.6 } });
        assertArrayEquals(Utils.toObjectArrays(engine.eval("Java.to([5.55,6.66], \"double[]\")")),
                new Object[][] { new Object[] { (double) 5.55 }, new Object[] { (double) 6.66 } });
        assertArrayEquals(Utils.toObjectArrays(engine.eval("Java.to([5.55,6.66], \"double[]\")"), singleColumn),
                new Object[][] { new Object[] { (double) 5.55 }, new Object[] { (double) 6.66 } });
        assertArrayEquals(Utils.toObjectArrays(engine.eval("Java.to([5.55,6.66], \"double[]\")"), multiColumns),
                new Object[][] { new Object[] { (double) 5.55, (double) 6.66 } });

        // string
        assertArrayEquals(Utils.toObjectArrays(engine.eval("[\"5x5x\", \"x6x6\"]")),
                new Object[][] { new Object[] { "5x5x" }, new Object[] { "x6x6" } });
        assertArrayEquals(Utils.toObjectArrays(engine.eval("[\"5x5x\", \"x6x6\"]"), singleColumn),
                new Object[][] { new Object[] { "5x5x" }, new Object[] { "x6x6" } });
        assertArrayEquals(Utils.toObjectArrays(engine.eval("[\"5x5x\", \"x6x6\"]"), multiColumns),
                new Object[][] { new Object[] { "5x5x", "x6x6" } });

        // null & empty
        assertArrayEquals(Utils.toObjectArrays(engine.eval("null")), new Object[0][0]);
        assertArrayEquals(Utils.toObjectArrays(engine.eval("null"), singleColumn), new Object[0][0]);
        assertArrayEquals(Utils.toObjectArrays(engine.eval("null"), multiColumns), new Object[0][0]);
        assertArrayEquals(Utils.toObjectArrays(engine.eval("[]")), new Object[0][0]);
        assertArrayEquals(Utils.toObjectArrays(engine.eval("[]"), singleColumn), new Object[0][0]);
        assertArrayEquals(Utils.toObjectArrays(engine.eval("[]"), multiColumns), new Object[0][0]);
        assertArrayEquals(Utils.toObjectArrays(engine.eval("Java.to([], \"int[]\")")), new Object[0][0]);
        assertArrayEquals(Utils.toObjectArrays(engine.eval("Java.to([], \"int[]\")"), singleColumn),
                new Object[0][0]);
        assertArrayEquals(Utils.toObjectArrays(engine.eval("Java.to([], \"int[]\")"), multiColumns),
                new Object[0][0]);

        // enumeration & iterable
        engine.put("_list", new ArrayList<Integer>() {
            {
                add(5);
                add(6);
            }
        });
        assertArrayEquals(Utils.toObjectArrays(engine.eval("java.util.Collections.emptyEnumeration()")),
                new Object[0][0]);
        assertArrayEquals(
                Utils.toObjectArrays(engine.eval("java.util.Collections.emptyEnumeration()"),
                        singleColumn),
                new Object[0][0]);
        assertArrayEquals(
                Utils.toObjectArrays(engine.eval("java.util.Collections.emptyEnumeration()"),
                        multiColumns),
                new Object[0][0]);
        assertArrayEquals(Utils.toObjectArrays(engine.eval("java.util.Collections.enumeration(_list)")),
                new Object[][] { new Object[] { 5 }, new Object[] { 6 } });
        assertArrayEquals(
                Utils.toObjectArrays(engine.eval("java.util.Collections.enumeration(_list)"),
                        singleColumn),
                new Object[][] { new Object[] { 5 }, new Object[] { 6 } });
        assertArrayEquals(
                Utils.toObjectArrays(engine.eval("java.util.Collections.enumeration(_list)"),
                        multiColumns),
                new Object[][] { new Object[] { 5, 6 } });

        assertArrayEquals(Utils.toObjectArrays(engine.eval("java.util.Collections.emptyList()")),
                new Object[0][0]);
        assertArrayEquals(Utils.toObjectArrays(engine.eval("java.util.Collections.emptyList()"), singleColumn),
                new Object[0][0]);
        assertArrayEquals(Utils.toObjectArrays(engine.eval("java.util.Collections.emptyList()"), multiColumns),
                new Object[0][0]);
        assertArrayEquals(Utils.toObjectArrays(engine.eval("_list")),
                new Object[][] { new Object[] { 5 }, new Object[] { 6 } });
        assertArrayEquals(Utils.toObjectArrays(engine.eval("_list"), singleColumn),
                new Object[][] { new Object[] { 5 }, new Object[] { 6 } });
        assertArrayEquals(Utils.toObjectArrays(engine.eval("_list"), multiColumns),
                new Object[][] { new Object[] { 5, 6 } });

        // object & map
        Object value = engine.eval("var a={}; a.x=null; a.b=5; a");
        assertArrayEquals(Utils.toObjectArrays(value), new Object[][] { new Object[] { value } });
        assertArrayEquals(Utils.toObjectArrays(value, singleColumn), new Object[][] { new Object[] { value } });
        assertArrayEquals(Utils.toObjectArrays(value, multiColumns),
                new Object[][] { new Object[] { null, 5 } });

        engine.put("_map", new HashMap<String, Integer>() {
            {
                put("b", 5);
                put("c", 6);
            }
        });
        value = engine.eval("_map");
        assertArrayEquals(Utils.toObjectArrays(value), new Object[][] { new Object[] { value } });
        assertArrayEquals(Utils.toObjectArrays(value, singleColumn), new Object[][] { new Object[] { value } });
        assertArrayEquals(Utils.toObjectArrays(value, multiColumns),
                new Object[][] { new Object[] { null, 5 } });

        // mixed & nested
        engine.put("_obj", new Object());
        value = engine.eval("var a={}; a.a=_obj; a.b=5; a");
        assertArrayEquals(Utils.toObjectArrays(value), new Object[][] { new Object[] { value } });
        assertArrayEquals(Utils.toObjectArrays(value, singleColumn), new Object[][] { new Object[] { value } });
        assertArrayEquals(Utils.toObjectArrays(value, multiColumns),
                new Object[][] { new Object[] { engine.get("_obj"), 5 } });

        value = engine.eval("[[5,6]]");
        assertArrayEquals(Utils.toObjectArrays(value),
                new Object[][] { new Object[] { 5 }, new Object[] { 6 } });
        assertArrayEquals(Utils.toObjectArrays(value, singleColumn),
                new Object[][] { new Object[] { 5 }, new Object[] { 6 } });
        assertArrayEquals(Utils.toObjectArrays(value, multiColumns), new Object[][] { new Object[] { 5, 6 } });
    }

    @Test(groups = { "unit" })
    public void testJavaObjectToJsonString() {
        // primitive types
        assertEquals(Utils.toJsonString((boolean) false), "false");
        assertEquals(Utils.toJsonString((byte) 5), "5");
        assertEquals(Utils.toJsonString((short) 5), "5");
        assertEquals(Utils.toJsonString((int) 5), "5");
        assertEquals(Utils.toJsonString((long) 5), "5");
        assertEquals(Utils.toJsonString((float) 5.5), "5.5");
        assertEquals(Utils.toJsonString((double) 5.55), "5.55");

        assertEquals(Utils.toJsonString(new boolean[] { false, true }), "[false, true]");
        assertEquals(Utils.toJsonString(new byte[] { (byte) 5, (byte) 6 }), "[5, 6]");
        assertEquals(Utils.toJsonString(new short[] { (short) 5, (short) 6 }), "[5, 6]");
        assertEquals(Utils.toJsonString(new int[] { 5, 6 }), "[5, 6]");
        assertEquals(Utils.toJsonString(new long[] { 5, 6 }), "[5, 6]");
        assertEquals(Utils.toJsonString(new float[] { 5.5F, 6.6F }), "[5.5, 6.6]");
        assertEquals(Utils.toJsonString(new double[] { 5.55, 6.66 }), "[5.55, 6.66]");

        assertEquals(Utils.toJsonString(Boolean.TRUE), "true");
        assertEquals(Utils.toJsonString(Byte.valueOf((byte) 5)), "5");
        assertEquals(Utils.toJsonString(Short.valueOf((short) 5)), "5");
        assertEquals(Utils.toJsonString(Integer.valueOf(5)), "5");
        assertEquals(Utils.toJsonString(Long.valueOf(5)), "5");
        assertEquals(Utils.toJsonString(Float.valueOf("5.5")), "5.5");
        assertEquals(Utils.toJsonString(Double.valueOf(5.55D)), "5.55");

        assertEquals(Utils.toJsonString(new Boolean[] { false, true }), "[false, true]");
        assertEquals(Utils.toJsonString(new Byte[] { (byte) 5, (byte) 6 }), "[5, 6]");
        assertEquals(Utils.toJsonString(new Short[] { (short) 5, (short) 6 }), "[5, 6]");
        assertEquals(Utils.toJsonString(new Integer[] { 5, 6 }), "[5, 6]");
        assertEquals(Utils.toJsonString(new Long[] { 5L, 6L }), "[5, 6]");
        assertEquals(Utils.toJsonString(new Float[] { 5.5F, 6.6F }), "[5.5, 6.6]");
        assertEquals(Utils.toJsonString(new Double[] { 5.55, 6.66 }), "[5.55, 6.66]");

        // string
        assertEquals(Utils.toJsonString("5x5x"), "\"5x5x\"");
        assertEquals(Utils.toJsonString(new String[] { "5x5x", "6x6x" }), "[\"5x5x\",\"6x6x\"]");

        // null
        assertEquals(Utils.toJsonString((Object) null), "");
        assertEquals(Utils.toJsonString(new Object[] { null }), "[null]");

        // enumeration & iterable
        assertEquals(Utils.toJsonString(Collections.emptyEnumeration()), "[]");
        assertEquals(Utils.toJsonString(new ArrayList<Integer>() {
            {
                add(1);
                add(2);
                add(3);
            }
        }), "[1,2,3]");
        assertEquals(Utils.toJsonString(Collections.emptySet()), "[]");

        // map
        Map<Object, Integer> map = new TreeMap<>();
        assertEquals(Utils.toJsonString(map), "{}");
        map.put("a", 1);
        map.put("b", 2);
        map.put("\"", 3);
        assertEquals(Utils.toJsonString(map), "{\"\\\"\":3,\"a\":1,\"b\":2}");

        map = new HashMap<>();
        map.put(null, null);
        assertEquals(Utils.toJsonString(map), "{\"null\":null}");

        // mixed & nested
        Object obj = new Object();
        SortedMap<Object, Object> m = new TreeMap<>();
        Map<String, Object> mm = new HashMap<>();
        mm.put(null, Collections.emptyList());
        m.put("mm", mm);
        m.put("false", Collections.singletonList(obj));
        m.put(obj.toString(), new int[] { 1, 2, 3 });
        assertEquals(Utils.toJsonString(m),
                "{\"false\":[\"" + obj.toString() + "\"],\"" + obj.toString()
                        + "\":[1, 2, 3],\"mm\":{\"null\":[]}}");
    }

    @Test(groups = { "sit" })
    public void testBindingsToJsonString() throws Exception {
        // this won't work in JDK 15+ as Nashron has been removed
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByExtension("js");

        // primitive types
        assertEquals(Utils.toJsonString(engine.eval("false")), "false");
        assertEquals(Utils.toJsonString(engine.eval("5")), "5");
        assertEquals(Utils.toJsonString(engine.eval("5.5")), "5.5");

        assertEquals(Utils.toJsonString(engine.eval("Java.to([false, true], \"boolean[]\")")), "[false, true]");
        assertEquals(Utils.toJsonString(engine.eval("Java.to([5,6,7], \"byte[]\")")), "[5, 6, 7]");
        assertEquals(Utils.toJsonString(engine.eval("Java.to([5,6,7], \"short[]\")")), "[5, 6, 7]");
        assertEquals(Utils.toJsonString(engine.eval("Java.to([5,6,7], \"int[]\")")), "[5, 6, 7]");
        assertEquals(Utils.toJsonString(engine.eval("Java.to([5,6,7], \"long[]\")")), "[5, 6, 7]");
        assertEquals(Utils.toJsonString(engine.eval("Java.to([5.5,6.6,7.7], \"float[]\")")), "[5.5, 6.6, 7.7]");
        assertEquals(Utils.toJsonString(engine.eval("Java.to([5.55,6.66,7.77], \"double[]\")")),
                "[5.55, 6.66, 7.77]");

        assertEquals(Utils.toJsonString(engine.eval("Java.to([false, true], \"java.lang.Boolean[]\")")),
                "[false, true]");
        assertEquals(Utils.toJsonString(engine.eval("Java.to([5,6,7], \"java.lang.Byte[]\")")), "[5, 6, 7]");
        assertEquals(Utils.toJsonString(engine.eval("Java.to([5,6,7], \"java.lang.Short[]\")")), "[5, 6, 7]");
        assertEquals(Utils.toJsonString(engine.eval("Java.to([5,6,7], \"java.lang.Integer[]\")")), "[5, 6, 7]");
        assertEquals(Utils.toJsonString(engine.eval("Java.to([5,6,7], \"java.lang.Long[]\")")), "[5, 6, 7]");
        assertEquals(Utils.toJsonString(engine.eval("Java.to([5.5,6.6,7.7], \"java.lang.Float[]\")")),
                "[5.5, 6.6, 7.7]");
        assertEquals(Utils.toJsonString(engine.eval("Java.to([5.55,6.66,7.77], \"java.lang.Double[]\")")),
                "[5.55, 6.66, 7.77]");

        // string
        assertEquals(Utils.toJsonString(engine.eval("\"5x5x\"")), "\"5x5x\"");

        // object
        assertEquals(Utils
                .toJsonString(engine.eval("var a = {}; a.list=[1,2,3]; a.x=1; a.y=2, a.z=\"5x5x\"; a")),
                "{\"list\":[1,2,3],\"x\":1,\"y\":2,\"z\":\"5x5x\"}");

        // mixed & nested
        assertEquals(
                Utils.toJsonString(engine.eval(
                        "var a={},b={},c=null; a.b=b; b.c=c; a.list=Java.to([1,2,3], \"int[]\"); a.z=\"5x5x\"; a")),
                "{\"b\":{\"c\":null},\"list\":[1, 2, 3],\"z\":\"5x5x\"}");
    }
}