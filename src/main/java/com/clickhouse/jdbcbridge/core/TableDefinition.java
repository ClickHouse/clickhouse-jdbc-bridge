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

import static com.clickhouse.jdbcbridge.core.DataType.DEFAULT_LENGTH;
import static com.clickhouse.jdbcbridge.core.DataType.DEFAULT_PRECISION;
import static com.clickhouse.jdbcbridge.core.DataType.DEFAULT_SCALE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EmptyStackException;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Stack;

import javax.script.Bindings;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * This class represents a list of columns, as well as protocol version.
 * 
 * @since 2.0
 */
public class TableDefinition {
    public static final int DEFAULT_VERSION = 1;

    public static final String COLUMN_DATASOURCE = "datasource";

    public static final TableDefinition DEFAULT_RESULT_COLUMNS = new TableDefinition(new ColumnDefinition(
            Utils.DEFAULT_COLUMN_NAME, DataType.Str, true, DEFAULT_LENGTH, DEFAULT_PRECISION, DEFAULT_SCALE));

    public static final TableDefinition DEBUG_COLUMNS = new TableDefinition(
            new ColumnDefinition(COLUMN_DATASOURCE, DataType.Str, true, DEFAULT_LENGTH, DEFAULT_PRECISION,
                    DEFAULT_SCALE),
            new ColumnDefinition("type", DataType.Str, true, DEFAULT_LENGTH, DEFAULT_PRECISION, DEFAULT_SCALE),
            new ColumnDefinition("definition", DataType.Str, true, DEFAULT_LENGTH, DEFAULT_PRECISION, DEFAULT_SCALE),
            new ColumnDefinition("mtypes", DataType.Str, true, DEFAULT_LENGTH, DEFAULT_PRECISION, DEFAULT_SCALE),
            new ColumnDefinition("query", DataType.Str, true, DEFAULT_LENGTH, DEFAULT_PRECISION, DEFAULT_SCALE),
            new ColumnDefinition("parameters", DataType.Str, true, DEFAULT_LENGTH, DEFAULT_PRECISION, DEFAULT_SCALE));

    public static final TableDefinition MUTATION_COLUMNS = new TableDefinition(
            // datasource type: jdbc, config, script etc.
            new ColumnDefinition("type", DataType.Str, true, DEFAULT_LENGTH, DEFAULT_PRECISION, DEFAULT_SCALE),
            // operation: read or write
            // new ColumnDefinition("operation", DataType.Str, true, DEFAULT_LENGTH,
            // DEFAULT_PRECISION, DEFAULT_SCALE),
            new ColumnDefinition("rows", DataType.UInt64, false, DEFAULT_LENGTH, DEFAULT_PRECISION, DEFAULT_SCALE));

    private static final String COLUMN_HEADER = "columns format version: ";
    private static final String COLUMN_COUNT = " columns:";

    private static final String CONF_VERSION = "version";
    private static final String CONF_QUERY = "query";
    private static final String CONF_COLUMNS = "columns";

    private final int version;
    private final ColumnDefinition[] columns;

    public TableDefinition(List<ColumnDefinition> columns) {
        this(1, columns.toArray(new ColumnDefinition[Objects.requireNonNull(columns).size()]));
    }

    public TableDefinition(ColumnDefinition... columns) {
        this(1, columns);
    }

    public TableDefinition(int version, ColumnDefinition... columns) {
        if (columns == null || columns.length == 0) {
            throw new IllegalArgumentException("At least one column is needed.");
        }

        this.version = version;
        this.columns = new ColumnDefinition[columns.length];

        for (int i = 0; i < columns.length; i++) {
            ColumnDefinition column = columns[i];
            this.columns[i] = new ColumnDefinition(column);
        }
    }

    public TableDefinition(TableDefinition template, boolean insert, ColumnDefinition... columns) {
        this.version = template.version;
        this.columns = new ColumnDefinition[template.columns.length + columns.length];

        if (insert) {
            System.arraycopy(columns, 0, this.columns, 0, columns.length);
            System.arraycopy(template.columns, 0, this.columns, columns.length, template.columns.length);
        } else { // append
            System.arraycopy(template.columns, 0, this.columns, 0, template.columns.length);
            System.arraycopy(columns, 0, this.columns, template.columns.length, columns.length);
        }
    }

    public static TableDefinition fromObject(Object types) {
        if (types == null) {
            return DEFAULT_RESULT_COLUMNS;
        }

        Class<?> clazz = types.getClass();
        final TableDefinition columns;

        if (types instanceof TableDefinition) {
            columns = (TableDefinition) types;
        } else if (types instanceof ColumnDefinition[]) {
            columns = new TableDefinition((ColumnDefinition[]) types);
        } else if (boolean[].class.equals(clazz)) {
            boolean[] array = (boolean[]) types;
            ColumnDefinition[] dcs = new ColumnDefinition[array.length];
            int index = 0;
            for (boolean b : array) {
                dcs[index++] = ColumnDefinition.fromObject(b);
            }
            columns = new TableDefinition(dcs);
        } else if (byte[].class.equals(clazz)) {
            byte[] array = (byte[]) types;
            ColumnDefinition[] dcs = new ColumnDefinition[array.length];
            int index = 0;
            for (byte b : array) {
                dcs[index++] = ColumnDefinition.fromObject(b);
            }
            columns = new TableDefinition(dcs);
        } else if (short[].class.equals(clazz)) {
            short[] array = (short[]) types;
            ColumnDefinition[] dcs = new ColumnDefinition[array.length];
            int index = 0;
            for (short s : array) {
                dcs[index++] = ColumnDefinition.fromObject(s);
            }
            columns = new TableDefinition(dcs);
        } else if (int[].class.equals(clazz)) {
            int[] array = (int[]) types;
            ColumnDefinition[] dcs = new ColumnDefinition[array.length];
            int index = 0;
            for (int i : array) {
                dcs[index++] = ColumnDefinition.fromObject(i);
            }
            columns = new TableDefinition(dcs);
        } else if (long[].class.equals(clazz)) {
            long[] array = (long[]) types;
            ColumnDefinition[] dcs = new ColumnDefinition[array.length];
            int index = 0;
            for (long l : array) {
                dcs[index++] = ColumnDefinition.fromObject(l);
            }
            columns = new TableDefinition(dcs);
        } else if (float[].class.equals(clazz)) {
            float[] array = (float[]) types;
            ColumnDefinition[] dcs = new ColumnDefinition[array.length];
            int index = 0;
            for (float f : array) {
                dcs[index++] = ColumnDefinition.fromObject(f);
            }
            columns = new TableDefinition(dcs);
        } else if (double[].class.equals(clazz)) {
            double[] array = (double[]) types;
            ColumnDefinition[] dcs = new ColumnDefinition[array.length];
            int index = 0;
            for (double d : array) {
                dcs[index++] = ColumnDefinition.fromObject(d);
            }
            columns = new TableDefinition(dcs);
        } else if (types instanceof Enumeration) {
            Enumeration<?> e = (Enumeration<?>) types;
            List<ColumnDefinition> dcs = new ArrayList<>();
            while (e.hasMoreElements()) {
                dcs.add(ColumnDefinition.fromObject(e.nextElement()));
            }
            columns = new TableDefinition(dcs.toArray(new ColumnDefinition[dcs.size()]));
        } else if (types instanceof Iterable) {
            List<ColumnDefinition> dcs = new ArrayList<>();
            for (Object o : (Iterable<?>) types) {
                dcs.add(ColumnDefinition.fromObject(o));
            }
            columns = new TableDefinition(dcs.toArray(new ColumnDefinition[dcs.size()]));
        } else if (clazz.isArray()) {
            Object[] array = (Object[]) types;
            ColumnDefinition[] dcs = new ColumnDefinition[array.length];
            int index = 0;
            for (Object o : array) {
                dcs[index++] = ColumnDefinition.fromObject(o);
            }
            columns = new TableDefinition(dcs);
        } else if (types instanceof Bindings) {
            Bindings cols = (Bindings) types;
            if (Utils.isArray(cols)) {
                ColumnDefinition[] dcs = new ColumnDefinition[cols.size()];
                int index = 0;
                for (Object o : cols.values()) {
                    dcs[index++] = ColumnDefinition.fromObject(o);
                }
                columns = new TableDefinition(dcs);
            } else {
                columns = new TableDefinition(ColumnDefinition.fromObject(types));
            }
        } else if (types instanceof Map) {
            columns = new TableDefinition(ColumnDefinition.fromObject(types));
        } else { // treat as JSON string
            columns = TableDefinition.fromJson(String.valueOf(types));
        }

        return columns;
    }

    public static TableDefinition fromJson(JsonArray config) {
        int version = DEFAULT_VERSION;
        ColumnDefinition[] columns = new ColumnDefinition[0];

        if (config == null) {
            columns = new ColumnDefinition[0];
        } else {
            columns = new ColumnDefinition[config.size()];
            for (int i = 0; i < columns.length; i++) {
                columns[i] = ColumnDefinition.fromJson(config.getJsonObject(i));
            }
        }

        return new TableDefinition(version, columns);
    }

    public static TableDefinition fromJson(String json) {
        int length = json == null ? 0 : json.length();

        JsonArray columns = null;
        for (int i = 0; i < length; i++) {
            char c = json.charAt(i);
            if (c == '{') {
                JsonObject obj = new JsonObject(json);
                columns = obj.getJsonArray(CONF_COLUMNS);
                break;
            } else if (c == '[') {
                columns = new JsonArray(json);
                break;
            } else if (Character.isWhitespace(c)) {
                continue;
            } else {
                break;
            }
        }

        return columns == null ? new TableDefinition(ColumnDefinition.fromObject(json))
                : TableDefinition.fromJson(columns);
    }

    public static TableDefinition fromString(String columnsInfo) {
        int version = DEFAULT_VERSION;
        ColumnDefinition[] columns = new ColumnDefinition[0];

        if (columnsInfo == null) {
            // nothing to do
        } else if (columnsInfo.startsWith(COLUMN_HEADER)) {
            List<String> lines = Utils.splitByChar(columnsInfo, '\n');
            columns = new ColumnDefinition[lines.size() - 2];

            int index = 0;

            String currentLine = null;
            try {
                for (String c : lines) {
                    currentLine = c;

                    if (index == 0) {
                        version = Integer.parseInt(c.substring(COLUMN_HEADER.length()));
                    } else if (index == 1) {
                        if (!c.endsWith(COLUMN_COUNT)) {
                            throw new IllegalArgumentException(new StringBuilder().append("line #").append(index + 1)
                                    .append(" must be end with '").append(COLUMN_COUNT).append('\'').toString());
                        }

                        String cCount = c.substring(0, c.length() - COLUMN_COUNT.length());
                        if (columns.length < Integer.parseInt(cCount)) {
                            throw new IllegalArgumentException(
                                    new StringBuilder().append("inconsistent columns count: declared ").append(cCount)
                                            .append(" but looks like ").append(lines.size()).toString());
                        }
                    } else {
                        columns[index - 2] = ColumnDefinition.fromString(c);
                    }

                    index++;
                }
            } catch (Exception e) {
                throw new IllegalArgumentException(new StringBuilder().append("failed to parse line #")
                        .append(index + 1).append(":\n").append(currentLine).toString(), e);
            }
        } else {
            Stack<Character> stack = new Stack<>();
            char lastChar = '\0';
            StringBuilder sb = new StringBuilder();
            List<String> splittedColumns = new ArrayList<String>();
            for (int i = 0, len = columnsInfo.length(); i < len; i++) {
                char ch = columnsInfo.charAt(i);
                switch (ch) {
                    case '\\':
                        if (i + 1 < len) {
                            sb.append(columnsInfo.charAt(++i));
                        }
                        break;
                    case '\'':
                        i = i + 1 < len && columnsInfo.charAt(i + 1) == '\'' ? i + 1 : i;
                        sb.append(ch);
                        if (lastChar != ch) {
                            lastChar = stack.push(ch);
                        } else {
                            try {
                                stack.pop();
                                lastChar = stack.size() > 0 ? stack.lastElement() : '\0';
                            } catch (EmptyStackException e) {
                                throw new IllegalArgumentException(new StringBuilder()
                                        .append("failed to parse given schema at position #").append(i + 1)
                                        .append(" around character [").append(ch).append(']').toString(), e);
                            }
                        }
                        break;
                    case '(':
                        sb.append(lastChar = stack.push(ch));
                        break;
                    case ')':
                        sb.append(ch);
                        if (lastChar == '(') {
                            try {
                                stack.pop();
                            } catch (EmptyStackException e) {
                                throw new IllegalArgumentException(new StringBuilder()
                                        .append("failed to parse given schema at position #").append(i + 1)
                                        .append(" around character [").append(ch).append(']').toString(), e);
                            }
                        }
                        break;
                    case ',':
                        if (stack.isEmpty()) {
                            splittedColumns.add(sb.toString());
                            sb.setLength(0);
                        } else {
                            sb.append(ch);
                        }
                        break;
                    default:
                        sb.append(ch);
                        break;
                }
            }

            if (sb.length() > 0) {
                splittedColumns.add(sb.toString());
            }

            int index = 0;
            columns = new ColumnDefinition[splittedColumns.size()];
            for (String c : splittedColumns) {
                columns[index++] = ColumnDefinition.fromString(c);
            }
        }

        return new TableDefinition(version, columns);
    }

    public int getVersion() {
        return this.version;
    }

    public boolean hasColumn() {
        return this.columns.length > 0;
    }

    public boolean containsColumn(String columnName) {
        boolean found = false;

        for (ColumnDefinition col : this.columns) {
            if (col.getName().equals(columnName)) {
                found = true;
                break;
            }
        }

        return found;
    }

    public int size() {
        return this.columns.length;
    }

    public ColumnDefinition getColumn(int index) {
        return this.columns[index];
    }

    public ColumnDefinition[] getColumns() {
        return Arrays.copyOf(this.columns, this.columns.length);
    }

    public void updateValues(List<ColumnDefinition> refColumns) {
        if (refColumns == null || refColumns.size() == 0) {
            return;
        }

        for (int i = 0; i < this.columns.length; i++) {
            ColumnDefinition info = this.columns[i];
            for (int j = 0; j < refColumns.size(); j++) {
                ColumnDefinition ref = refColumns.get(j);
                if (info.getName().equals(ref.getName())) { // discard type
                    info.value.merge(ref.value.getValue().toString());
                    break;
                }
            }
        }
    }

    public String toJsonString(String query) {
        JsonObject config = new JsonObject();
        config.put(CONF_VERSION, this.version);
        if (query != null) {
            config.put(CONF_QUERY, query);
        }

        if (this.columns != null && this.columns.length > 0) {
            JsonArray array = new JsonArray();
            for (ColumnDefinition info : this.columns) {
                array.add(info.toJson());
            }

            config.put(CONF_COLUMNS, array);
        }

        return config.toString();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append(COLUMN_HEADER).append(version).append('\n').append(columns.length).append(COLUMN_COUNT).append('\n');

        for (ColumnDefinition column : columns) {
            sb.append(column.toString()).append('\n');
        }

        return sb.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(columns);
        result = prime * result + version;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        TableDefinition other = (TableDefinition) obj;

        return version == other.version && Arrays.equals(columns, other.columns);
    }
}