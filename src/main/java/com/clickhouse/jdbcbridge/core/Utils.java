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

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Stack;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import javax.script.Bindings;

import com.fasterxml.jackson.core.io.JsonStringEncoder;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.vertx.core.json.JsonObject;

/**
 * Helper class.
 * 
 * @since 2.0
 */
public final class Utils {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(Utils.class);

    private static final ClassLoader extClassLoader = new ExpandedUrlClassLoader(Utils.class.getClassLoader(),
            getConfiguration("extensions", "EXTENSION_DIR", "jdbc-bridge.extension.dir"));

    private static final MeterRegistry defaultRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

    private static final String MSG_DIGEST_ALGORTITHM = "SHA-512";

    private static final char[][] CLAUSE_CHARS = new char[][] { new char[] { '`', '`' }, new char[] { '\'', '\'' },
            new char[] { '(', ')' } };

    public static final String VARIABLE_PREFIX = "{{";
    public static final String VARIABLE_SUFFIX = "}}";

    public static final String EMPTY_STRING = "";
    public static final String NULL_STRING = "null";

    public static final int OBJECT_DEPTH_LIMIT = 10;

    public static final int U_INT8_MAX = (1 << 8) - 1;
    public static final int U_INT16_MAX = (1 << 16) - 1;
    public static final long U_INT32_MAX = (1L << 32) - 1;
    public static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);

    public static final long DATETIME_MAX = U_INT32_MAX * 1000L;

    public static final String DEFAULT_COLUMN_NAME = "results";

    private static boolean isRow(Object object) {
        boolean result = false;

        if (object != null) {
            Class<?> clazz = object.getClass();

            result = char[].class.equals(clazz) || boolean[].class.equals(clazz) || byte[].class.equals(clazz)
                    || short[].class.equals(clazz) || int[].class.equals(clazz) || long[].class.equals(clazz)
                    || float[].class.equals(clazz) || double[].class.equals(clazz) || byte[].class.equals(clazz)
                    || object instanceof Enumeration || object instanceof Iterable || clazz.isArray()
                    || object instanceof Map;
        }

        return result;
    }

    private static void addObjects(Object object, List<Object> list, boolean asColumn, String... columnNames) {
        if (object == null) {
            list.add(null);
            return;
        }

        Class<?> clazz = object.getClass();

        if (char[].class.equals(clazz)) {
            if (asColumn) {
                for (char v : (char[]) object) {
                    list.add(v);
                }
            } else {
                char[] values = (char[]) object;
                int len = values.length;
                if (len > 0) {
                    Object[] columns = new Object[len];
                    for (int i = 0; i < len; i++) {
                        columns[i] = values[i];
                    }
                    list.add(columns);
                }
            }
        } else if (boolean[].class.equals(clazz)) {
            if (asColumn) {
                for (boolean v : (boolean[]) object) {
                    list.add(v);
                }
            } else {
                boolean[] values = (boolean[]) object;
                int len = values.length;
                if (len > 0) {
                    Object[] columns = new Object[len];
                    for (int i = 0; i < len; i++) {
                        columns[i] = values[i];
                    }
                    list.add(columns);
                }
            }
        } else if (byte[].class.equals(clazz)) {
            if (asColumn) {
                for (byte v : (byte[]) object) {
                    list.add(v);
                }
            } else {
                byte[] values = (byte[]) object;
                int len = values.length;
                if (len > 0) {
                    Object[] columns = new Object[len];
                    for (int i = 0; i < len; i++) {
                        columns[i] = values[i];
                    }
                    list.add(columns);
                }
            }
        } else if (short[].class.equals(clazz)) {
            if (asColumn) {
                for (short v : (short[]) object) {
                    list.add(v);
                }
            } else {
                short[] values = (short[]) object;
                int len = values.length;
                if (len > 0) {
                    Object[] columns = new Object[len];
                    for (int i = 0; i < len; i++) {
                        columns[i] = values[i];
                    }
                    list.add(columns);
                }
            }
        } else if (int[].class.equals(clazz)) {
            if (asColumn) {
                for (int v : (int[]) object) {
                    list.add(v);
                }
            } else {
                int[] values = (int[]) object;
                int len = values.length;
                if (len > 0) {
                    Object[] columns = new Object[len];
                    for (int i = 0; i < len; i++) {
                        columns[i] = values[i];
                    }
                    list.add(columns);
                }
            }
        } else if (long[].class.equals(clazz)) {
            if (asColumn) {
                for (long v : (long[]) object) {
                    list.add(v);
                }
            } else {
                long[] values = (long[]) object;
                int len = values.length;
                if (len > 0) {
                    Object[] columns = new Object[len];
                    for (int i = 0; i < len; i++) {
                        columns[i] = values[i];
                    }
                    list.add(columns);
                }
            }
        } else if (float[].class.equals(clazz)) {
            if (asColumn) {
                for (float v : (float[]) object) {
                    list.add(v);
                }
            } else {
                float[] values = (float[]) object;
                int len = values.length;
                if (len > 0) {
                    Object[] columns = new Object[len];
                    for (int i = 0; i < len; i++) {
                        columns[i] = values[i];
                    }
                    list.add(columns);
                }
            }
        } else if (double[].class.equals(clazz)) {
            if (asColumn) {
                for (double v : (double[]) object) {
                    list.add(v);
                }
            } else {
                double[] values = (double[]) object;
                int len = values.length;
                if (len > 0) {
                    Object[] columns = new Object[len];
                    for (int i = 0; i < len; i++) {
                        columns[i] = values[i];
                    }
                    list.add(columns);
                }
            }
        } else if (object instanceof Enumeration) {
            Enumeration<?> e = (Enumeration<?>) object;

            if (asColumn) {
                while (e.hasMoreElements()) {
                    list.add(e.nextElement());
                }
            } else {
                List<Object> subList = new ArrayList<>(columnNames.length);
                boolean isFirst = true;
                boolean notRow = true;
                while (e.hasMoreElements()) {
                    Object value = e.nextElement();
                    if (isFirst) {
                        notRow = !isRow(value);
                        isFirst = false;
                    }

                    addObjects(value, notRow ? subList : list, notRow, columnNames);
                }

                int size = subList.size();
                if (size > 0) {
                    list.add(subList.toArray(new Object[size]));
                }
            }
        } else if (object instanceof Iterable) {
            if (asColumn) {
                for (Object o : (Iterable<?>) object) {
                    list.add(o);
                }
            } else {
                int columnsCount = columnNames.length;
                List<Object> subList = new ArrayList<>(columnsCount);
                boolean isFirst = true;
                boolean notRow = true;
                for (Object o : (Iterable<?>) object) {
                    if (isFirst) {
                        notRow = !isRow(o);
                        isFirst = false;
                    }

                    addObjects(o, notRow ? subList : list, notRow, columnNames);
                }

                int size = subList.size();
                if (size > 0) {
                    list.add(subList.toArray(new Object[size]));
                }
            }
        } else if (clazz.isArray()) {
            if (asColumn) {
                for (Object o : (Object[]) object) {
                    list.add(o);
                }
            } else {
                int columnsCount = columnNames.length;
                List<Object> subList = new ArrayList<>(columnsCount);
                boolean isFirst = true;
                boolean notRow = true;
                for (Object o : (Object[]) object) {
                    if (isFirst) {
                        notRow = !isRow(o);
                        isFirst = false;
                    }

                    addObjects(o, notRow ? subList : list, notRow, columnNames);
                }

                int size = subList.size();
                if (size > 0) {
                    list.add(subList.toArray(new Object[size]));
                }
            }
        } else if (object instanceof Bindings && isArray((Bindings) object)) { // special type of Map
            Bindings bindings = (Bindings) object;
            if (asColumn) {
                for (Object value : bindings.values()) {
                    list.add(value);
                }
            } else {
                int columnsCount = columnNames.length;
                List<Object> subList = new ArrayList<>(columnsCount);
                boolean isFirst = true;
                boolean notRow = true;
                for (Object value : bindings.values()) {
                    if (isFirst) {
                        notRow = !isRow(value);
                        isFirst = false;
                    }

                    addObjects(value, notRow ? subList : list, notRow, columnNames);
                }

                int size = subList.size();
                if (size > 0) {
                    list.add(subList.toArray(new Object[size]));
                }
            }
        } else if (object instanceof Map) {
            if (asColumn) {
                list.add(object);
            } else {
                if (columnNames.length == 1) {
                    list.add(new Object[] { object });
                } else {
                    Map<?, ?> m = (Map<?, ?>) object;
                    List<Object> subList = new ArrayList<>(columnNames.length);
                    for (String name : columnNames) {
                        subList.add(m.get(name));
                    }

                    int size = subList.size();
                    if (size > 0) {
                        list.add(subList.toArray(new Object[size]));
                    }
                }
            }
        } else {
            list.add(asColumn ? object : new Object[] { object });
        }
    }

    private static int checkDepth(Object obj, int depth) {
        depth++;

        if (depth > OBJECT_DEPTH_LIMIT) {
            throw new IllegalArgumentException(
                    "Too many levels to expand - please simplify the object hierarchy by limiting nested levels less than "
                            + OBJECT_DEPTH_LIMIT);
        }

        return depth;
    }

    private static void appendJsonString(Object object, StringBuilder json, int depth) {
        if (object == null) {
            json.append(NULL_STRING);
            return;
        }

        depth = checkDepth(object, depth);

        Class<?> clazz = object.getClass();

        if ((clazz.isPrimitive() && !char.class.equals(clazz)) || Boolean.class.equals(clazz)
                || object instanceof Number) {
            json.append(String.valueOf(object));
        } else if (char[].class.equals(clazz)) {
            char[] array = (char[]) object;
            json.append('[');

            JsonStringEncoder encoder = JsonStringEncoder.getInstance();
            for (int i = 0; i < array.length; i++) {
                if (i > 0) {
                    json.append(',');
                }

                json.append('"');
                encoder.quoteAsString(String.valueOf(array[i]), json);
                json.append('"');
            }
            json.append(']');
        } else if (Character[].class.equals(clazz)) {
            Character[] array = (Character[]) object;
            json.append('[');

            JsonStringEncoder encoder = JsonStringEncoder.getInstance();
            for (int i = 0; i < array.length; i++) {
                if (i > 0) {
                    json.append(',');
                }

                json.append('"');
                Character value = array[i];
                if (value == null) {
                    json.append(NULL_STRING);
                } else {
                    encoder.quoteAsString(String.valueOf(value), json);
                }
                json.append('"');
            }
            json.append(']');
        } else if (boolean[].class.equals(clazz)) {
            json.append(Arrays.toString((boolean[]) object));
        } else if (Boolean[].class.equals(clazz)) {
            json.append(Arrays.toString((Boolean[]) object));
        } else if (byte[].class.equals(clazz)) {
            json.append(Arrays.toString((byte[]) object));
        } else if (Byte[].class.equals(clazz)) {
            json.append(Arrays.toString((Byte[]) object));
        } else if (short[].class.equals(clazz)) {
            json.append(Arrays.toString((short[]) object));
        } else if (Short[].class.equals(clazz)) {
            json.append(Arrays.toString((Short[]) object));
        } else if (int[].class.equals(clazz)) {
            json.append(Arrays.toString((int[]) object));
        } else if (Integer[].class.equals(clazz)) {
            json.append(Arrays.toString((Integer[]) object));
        } else if (long[].class.equals(clazz)) {
            json.append(Arrays.toString((long[]) object));
        } else if (Long[].class.equals(clazz)) {
            json.append(Arrays.toString((Long[]) object));
        } else if (float[].class.equals(clazz)) {
            json.append(Arrays.toString((float[]) object));
        } else if (Float[].class.equals(clazz)) {
            json.append(Arrays.toString((Float[]) object));
        } else if (double[].class.equals(clazz)) {
            json.append(Arrays.toString((double[]) object));
        } else if (Double[].class.equals(clazz)) {
            json.append(Arrays.toString((Double[]) object));
        } else if (object instanceof Enumeration) {
            json.append('[');

            Enumeration<?> e = (Enumeration<?>) object;
            boolean isNotFirst = false;
            while (e.hasMoreElements()) {
                if (isNotFirst) {
                    json.append(',');
                } else {
                    isNotFirst = true;
                }

                appendJsonString(e.nextElement(), json, depth);
            }

            json.append(']');
        } else if (object instanceof Iterable) {
            json.append('[');

            boolean isNotFirst = false;
            for (Object o : (Iterable<?>) object) {
                if (isNotFirst) {
                    json.append(',');
                } else {
                    isNotFirst = true;
                }

                appendJsonString(o, json, depth);
            }

            json.append(']');
        } else if (clazz.isArray()) {
            Object[] array = (Object[]) object;
            json.append('[');
            for (int i = 0; i < array.length; i++) {
                if (i > 0) {
                    json.append(',');
                }

                Object value = array[i];
                if (value == null) {
                    json.append(NULL_STRING);
                } else {
                    appendJsonString(value, json, depth);
                }
            }
            json.append(']');
        } else if (object instanceof Map) {
            json.append('{');

            boolean isNotFirst = false;
            for (Map.Entry<?, ?> o : ((Map<?, ?>) object).entrySet()) {
                if (isNotFirst) {
                    json.append(',');
                } else {
                    isNotFirst = true;
                }

                json.append('"');
                JsonStringEncoder.getInstance().quoteAsString(String.valueOf(o.getKey()), json);
                json.append('"').append(':');

                appendJsonString(o.getValue(), json, depth);
            }

            json.append('}');
        } else {
            json.append('"');
            JsonStringEncoder.getInstance().quoteAsString(String.valueOf(object), json);
            json.append('"');
        }
    }

    private static void appendJsonString(Bindings bindings, StringBuilder json, int depth) {
        if (bindings == null) {
            json.append(NULL_STRING);
            return;
        }

        depth = checkDepth(bindings, depth);

        if (isArray(bindings)) {
            json.append('[');

            boolean isNotFirst = false;
            for (Object value : bindings.values()) {
                if (isNotFirst) {
                    json.append(',');
                } else {
                    isNotFirst = true;
                }

                if (value == null) {
                    json.append(NULL_STRING);
                } else if (value instanceof Bindings) {
                    appendJsonString(bindings, json, depth);
                } else {
                    appendJsonString(value, json, depth);
                }
            }

            json.append(']');
        } else {
            json.append('{');

            boolean isNotFirst = false;
            for (String key : bindings.keySet()) {
                if (isNotFirst) {
                    json.append(',');
                } else {
                    isNotFirst = true;
                }

                json.append('"');
                JsonStringEncoder.getInstance().quoteAsString(key, json);
                json.append('"').append(':');

                Object value = bindings.get(key);
                if (value == null) {
                    json.append(NULL_STRING);
                } else if (value instanceof Bindings) {
                    appendJsonString((Bindings) value, json, depth);
                } else {
                    appendJsonString(value, json, depth);
                }
            }

            json.append('}');
        }
    }

    public static String unescapeQuotes(String str) {
        if (str == null) {
            return EMPTY_STRING;
        }

        int len = str.length();
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            char ch = str.charAt(i);
            if (i + 1 < len) {
                char nextCh = str.charAt(i + 1);
                if ((ch == '\\' || ch == '\'') && nextCh == '\'') {
                    sb.append('\'');
                    i++;
                    continue;
                }
            }

            sb.append(ch);
        }

        return sb.toString();
    }

    public static boolean containsWhitespace(String str) {
        boolean result = false;

        if (str != null) {
            for (int i = 0; i < str.length(); i++) {
                char ch = str.charAt(i);
                if (Character.isWhitespace(ch)) {
                    result = true;
                    break;
                }
            }
        }

        return result;
    }

    public static int indexOfKeywordIgnoreCase(String statement, String keyword) {
        return indexOfKeyword(statement, keyword, true);
    }

    public static int indexOfKeyword(String statement, String keyword, boolean caseInsensitive) {
        if (statement == null || keyword == null) {
            return -1;
        }

        int index = -1;

        char[][] chars = CLAUSE_CHARS;
        Stack<Character> stack = new Stack<>();

        char lastChar = '\0';
        for (int i = 0, len = statement.length(), wnd = keyword.length(); i + wnd <= len; i++) {
            char actual = statement.charAt(i);
            for (int k = 0; k < chars.length; k++) {
                char[] chs = chars[k];

                if (actual == lastChar && stack.size() > 0) {
                    stack.pop();
                    lastChar = stack.size() > 0 ? stack.lastElement() : '\0';
                    break;
                } else if (actual == chs[0]) {
                    stack.push(lastChar = chs[1]);
                    break;
                }
            }

            if (stack.size() > 0) {
                continue;
            }

            boolean matched = true;
            for (int j = 0; j < wnd; j++) {
                actual = statement.charAt(i + j);
                char expected = keyword.charAt(j);

                if (actual != expected) {
                    if (caseInsensitive && Character.toUpperCase(actual) == Character.toUpperCase(expected)) {
                        continue;
                    } else {
                        matched = false;
                        break;
                    }
                }
            }

            if (matched) {
                index = i;
                break;
            }
        }

        return index;
    }

    public static final Object getDefaultMetricRegistry() {
        return defaultRegistry;
    }

    public static final String getValueOrEmptyString(String value) {
        return value == null || value.isEmpty() ? EMPTY_STRING : value;
    }

    public static String getConfiguration(String defaultValue, String environmentVariable, String systemProperty) {
        String value = systemProperty == null ? EMPTY_STRING
                : getValueOrEmptyString(System.getProperty(systemProperty));

        if (value.isEmpty() && environmentVariable != null) {
            value = getValueOrEmptyString(System.getenv(environmentVariable));
        }

        if (value.isEmpty() && defaultValue != null) {
            value = defaultValue;
        }

        return value;
    }

    public static Extension<?> loadExtension(String className) {
        return loadExtension(null, className);
    }

    public static Extension<?> loadExtension(Collection<String> libUrls, String className) {
        Extension<?> extension = null;

        final ClassLoader loader = new ExpandedUrlClassLoader(extClassLoader,
                libUrls == null || libUrls.size() == 0 ? new String[0] : libUrls.toArray(new String[libUrls.size()]));

        final Thread current = Thread.currentThread();
        final ClassLoader currentLoader = current.getContextClassLoader();

        current.setContextClassLoader(loader);
        try {
            extension = new Extension<>(loader.loadClass(className));
        } catch (ClassNotFoundException e) {
            log.warn("Not able to find extension class: " + className);
        } catch (Exception e) {
            log.warn("Failed to load extension: " + className, e);
        } finally {
            current.setContextClassLoader(currentLoader);
        }

        return extension;
    }

    public static <T> T loadService(Class<T> service) {
        return loadService(service, extClassLoader);
    }

    public static <T> T loadService(Class<T> service, ClassLoader loader) {
        if (loader == null) {
            loader = Thread.currentThread().getContextClassLoader();
        }

        return ServiceLoader.load(service, loader).iterator().next();
    }

    // limit this function to only Bindings instead of Map
    public static boolean isArray(Bindings objects) {
        boolean isArray = true;

        if (!objects.isEmpty()) {
            int index = 0;
            for (String key : objects.keySet()) {
                if (!String.valueOf(index++).equals(key)) {
                    isArray = false;
                    break;
                }
            }
        }

        return isArray;
    }

    public static String toJsonString(Object object) {
        if (object == null) {
            return EMPTY_STRING;
        }

        StringBuilder json = new StringBuilder();
        int depth = 0;

        if (object instanceof Bindings) {
            appendJsonString((Bindings) object, json, depth);
        } else {
            appendJsonString(object, json, depth);
        }

        return json.toString();
    }

    public static Object[][] toObjectArrays(Object object, String... columnNames) {
        if (object == null) {
            return new Object[0][0];
        }

        if (columnNames == null || columnNames.length == 0) {
            columnNames = new String[] { DEFAULT_COLUMN_NAME };
        }

        List<Object> list = new ArrayList<>();

        addObjects(object, list, false, columnNames);

        int rowsCount = list.size();
        final Object[][] values;

        if (columnNames.length == 1 && rowsCount == 1) {
            Object v = list.get(0);
            if (v instanceof Object[]) {
                Object[] objs = (Object[]) v;
                int len = objs.length;
                values = new Object[len][1];
                for (int i = 0; i < len; i++) {
                    values[i][0] = objs[i];
                }
            } else {
                values = new Object[1][1];
                values[0][0] = v;
            }
        } else {
            int columnsCount = columnNames.length;
            values = new Object[rowsCount][columnsCount];
            for (int i = 0; i < rowsCount; i++) {
                Object[] source = (Object[]) list.get(i);
                Object[] target = values[i];
                if (source != null) {
                    System.arraycopy(source, 0, target, 0, Math.min(source.length, target.length));
                }
            }
        }

        return values;
    }

    public static void checkArgument(byte[] value, int length) {
        if (value.length > length) {
            throw new IllegalArgumentException(
                    new StringBuilder().append("Given byte array should NOT greater than ").append(length).toString());
        }
    }

    public static void checkArgument(int value, int minValue) {
        if (value < minValue) {
            throw new IllegalArgumentException(new StringBuilder().append("Given value(").append(value)
                    .append(") should NOT less than ").append(minValue).toString());
        }
    }

    public static void checkArgument(long value, long minValue) {
        if (value < minValue) {
            throw new IllegalArgumentException(new StringBuilder().append("Given value(").append(value)
                    .append(") should NOT less than ").append(minValue).toString());
        }
    }

    public static void checkArgument(int value, int minValue, int maxValue) {
        if (value < minValue || value > maxValue) {
            throw new IllegalArgumentException(new StringBuilder().append("Given value(").append(value)
                    .append(") should between ").append(minValue).append(" and ").append(maxValue).toString());
        }
    }

    public static void checkArgument(long value, long minValue, long maxValue) {
        if (value < minValue || value > maxValue) {
            throw new IllegalArgumentException(new StringBuilder().append("Given value(").append(value)
                    .append(") should between ").append(minValue).append(" and ").append(maxValue).toString());
        }
    }

    public static void checkArgument(BigInteger value, BigInteger minValue) {
        if (value.compareTo(minValue) < 0) {
            throw new IllegalArgumentException(new StringBuilder().append("Given value(").append(value)
                    .append(") should greater than ").append(minValue).toString());
        }
    }

    public static void checkArgument(BigInteger value, BigInteger minValue, BigInteger maxValue) {
        if (value.compareTo(minValue) < 0 || value.compareTo(maxValue) > 0) {
            throw new IllegalArgumentException(new StringBuilder().append("Given value(").append(value)
                    .append(") should between ").append(minValue).append(" and ").append(maxValue).toString());
        }
    }

    public static List<String> splitByChar(String str, char delimiter) {
        return splitByChar(str, delimiter, true);
    }

    public static List<String> splitByChar(String str, char delimiter, boolean tokenize) {
        LinkedList<String> list = new LinkedList<>();

        if (str != null) {
            int startIndex = 0;

            for (int i = 0, length = str.length(); i <= length; i++) {
                if (i == length || str.charAt(i) == delimiter) {
                    if (tokenize && i >= startIndex) {
                        String matched = str.substring(startIndex, i).trim();
                        if (!matched.isEmpty()) {
                            list.add(matched);
                        }
                    } else {
                        list.add(str.substring(startIndex, i));
                    }

                    startIndex = Math.min(i + 1, length);
                }
            }
        }

        return list;
    }

    public static String digest(JsonObject o) {
        return digest(o == null ? (String) null : o.encode());
    }

    public static String digest(String s) {
        if (s == null || s.isEmpty()) {
            return EMPTY_STRING;
        }

        try {
            MessageDigest md = MessageDigest.getInstance(MSG_DIGEST_ALGORTITHM);
            return new BigInteger(1, md.digest(s.getBytes())).toString(16);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public static void addTypedParameter(Map<String, TypedParameter<?>> map, TypedParameter<?> p) {
        if (map != null && p != null) {
            map.put(p.getName(), p);
        }
    }

    public static boolean fileExists(String file) {
        boolean exists = false;

        try {
            exists = Files.exists(Paths.get(file));
        } catch (Exception e) {
            // ignore
        }

        return exists;
    }

    public static String loadTextFromFile(String file) {
        log.info("Loading text from file [{}]...", file);

        StringBuilder contentBuilder = new StringBuilder();
        try (Stream<String> stream = Files.lines(Paths.get(file), StandardCharsets.UTF_8)) {
            stream.forEach(s -> contentBuilder.append(s).append('\n'));
        } catch (Exception e) {
            log.warn("Failed to load text from file", e);
        }

        return contentBuilder.toString();
    }

    public static JsonObject loadJsonFromFile(String file) {
        log.info("Loading JSON from file [{}]...", file);

        JsonObject config = null;

        StringBuilder contentBuilder = new StringBuilder();
        try (Stream<String> stream = Files.lines(Paths.get(file), StandardCharsets.UTF_8)) {
            stream.forEach(s -> contentBuilder.append(s).append('\n'));
            config = new JsonObject(contentBuilder.toString());
        } catch (Exception e) {
            log.warn("Failed to load JSON from file " + file);
        }

        return config == null ? new JsonObject() : config;
    }

    public static String applyVariables(String template, UnaryOperator<String> operator) {
        if (template == null) {
            template = EMPTY_STRING;
        }

        if (operator == null) {
            return template;
        }

        StringBuilder sb = new StringBuilder();

        for (int i = 0, len = template.length(); i < len; i++) {
            int index = template.indexOf(VARIABLE_PREFIX, i);
            if (index != -1) {
                sb.append(template.substring(i, index));

                i = index;
                index = template.indexOf(VARIABLE_SUFFIX, i);

                if (index != -1) {
                    String variable = template.substring(i + VARIABLE_PREFIX.length(), index).trim();
                    String value = operator.apply(variable);
                    if (value == null) {
                        i += VARIABLE_PREFIX.length() - 1;
                        sb.append(VARIABLE_PREFIX);
                    } else {
                        i = index + VARIABLE_SUFFIX.length() - 1;
                        sb.append(value);
                    }
                } else {
                    sb.append(template.substring(i));
                    break;
                }
            } else {
                sb.append(template.substring(i));
                break;
            }
        }

        return sb.toString();
    }

    public static String applyVariables(String template, Map<String, String> variables) {
        return applyVariables(template, variables == null || variables.size() == 0 ? null : variables::get);
    }

    private Utils() {
    }
}