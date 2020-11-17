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

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import io.vertx.core.json.JsonObject;
import ru.yandex.clickhouse.jdbcbridge.core.ByteBuffer;
import ru.yandex.clickhouse.jdbcbridge.core.DataAccessException;
import ru.yandex.clickhouse.jdbcbridge.core.ColumnDefinition;
import ru.yandex.clickhouse.jdbcbridge.core.TableDefinition;
import ru.yandex.clickhouse.jdbcbridge.core.DataSourceManager;
import ru.yandex.clickhouse.jdbcbridge.core.DataTableReader;
import ru.yandex.clickhouse.jdbcbridge.core.DefaultValues;
import ru.yandex.clickhouse.jdbcbridge.core.Extension;
import ru.yandex.clickhouse.jdbcbridge.core.ExtensionManager;
import ru.yandex.clickhouse.jdbcbridge.core.NamedDataSource;
import ru.yandex.clickhouse.jdbcbridge.core.QueryParameters;
import ru.yandex.clickhouse.jdbcbridge.core.ResponseWriter;
import ru.yandex.clickhouse.jdbcbridge.core.Utils;

import static ru.yandex.clickhouse.jdbcbridge.core.DataType.DEFAULT_LENGTH;
import static ru.yandex.clickhouse.jdbcbridge.core.DataType.DEFAULT_PRECISION;
import static ru.yandex.clickhouse.jdbcbridge.core.DataType.DEFAULT_SCALE;

public class ScriptDataSource extends NamedDataSource {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ScriptDataSource.class);

    private static final Map<String, Object> vars = new HashMap<>();

    public static final String EXTENSION_NAME = "script";

    public static final String DEFAULT_SCRIPT_EXTENSION = "js";

    public static final String FUNC_INFER_TYPES = "__types__";
    public static final String FUNC_GET_RESULTS = "__results__";

    static class ScriptResultReader implements DataTableReader {
        private final Object[][] values;

        private int currentRow = 0;

        protected ScriptResultReader(Object result, String... columnNames) {
            values = Utils.toObjectArrays(result, columnNames);
        }

        @Override
        public boolean nextRow() {
            return currentRow++ < values.length;
        }

        @Override
        public boolean isNull(int row, int column, ColumnDefinition metadata) {
            Object[] r = values[row];

            return column >= r.length || r[column] == null;
        }

        @Override
        public void read(int row, int column, ColumnDefinition metadata, ByteBuffer buffer) {
            Object[] r = values[row];
            Object v = column < r.length ? r[column] : null;

            if (v == null) {
                return;
            }

            switch (metadata.getType()) {
                case Bool:
                case Int8:
                    buffer.writeInt8(converter.as(Byte.class, v));
                    break;
                case Int16:
                    buffer.writeInt16(converter.as(Short.class, v));
                    break;
                case Int32:
                    buffer.writeInt32(converter.as(Integer.class, v));
                    break;
                case Int64:
                    buffer.writeInt64(converter.as(Long.class, v));
                    break;
                case UInt8:
                    buffer.writeUInt8(converter.as(Integer.class, v));
                    break;
                case UInt16:
                    buffer.writeUInt16(converter.as(Integer.class, v));
                    break;
                case UInt32:
                    buffer.writeUInt32(converter.as(Long.class, v));
                    break;
                case UInt64:
                    buffer.writeUInt64(converter.as(Long.class, v));
                    break;
                case Float32:
                    buffer.writeFloat32(converter.as(Float.class, v));
                    break;
                case Float64:
                    buffer.writeFloat64(converter.as(Double.class, v));
                    break;
                case Date:
                    buffer.writeDate(converter.as(Date.class, v));
                    break;
                case DateTime:
                    buffer.writeDateTime(converter.as(Date.class, v), metadata.getTimeZone());
                    break;
                case DateTime64:
                    buffer.writeDateTime64(converter.as(Date.class, v), metadata.getTimeZone());
                    break;
                case Decimal:
                    buffer.writeDecimal(converter.as(BigDecimal.class, v), metadata.getPrecision(),
                            metadata.getScale());
                    break;
                case Decimal32:
                    buffer.writeDecimal32(converter.as(BigDecimal.class, v), metadata.getScale());
                    break;
                case Decimal64:
                    buffer.writeDecimal64(converter.as(BigDecimal.class, v), metadata.getScale());
                    break;
                case Decimal128:
                    buffer.writeDecimal128(converter.as(BigDecimal.class, v), metadata.getScale());
                    break;
                case Decimal256:
                    buffer.writeDecimal256(converter.as(BigDecimal.class, v), metadata.getScale());
                    break;
                case Str:
                default:
                    buffer.writeString(Utils.toJsonString(v));
                    break;
            }
        }
    }

    public static void initialize(ExtensionManager manager) {
        ScriptDataSource.vars.putAll(manager.getScriptableObjects());

        Extension<NamedDataSource> thisExtension = manager.getExtension(ScriptDataSource.class);
        manager.getDataSourceManager().registerType(EXTENSION_NAME, thisExtension);
    }

    public static ScriptDataSource newInstance(Object... args) {
        if (Objects.requireNonNull(args).length < 2) {
            throw new IllegalArgumentException(
                    "In order to create JDBC datasource, you need to specify at least ID and datasource manager.");
        }

        String id = (String) args[0];
        DataSourceManager manager = (DataSourceManager) Objects.requireNonNull(args[1]);
        JsonObject config = args.length > 2 ? (JsonObject) args[2] : null;

        return new ScriptDataSource(id, manager, config);
    }

    private final ScriptEngineManager scriptManager;

    protected ScriptDataSource(String id, DataSourceManager manager, JsonObject config) {
        super(id, manager, config);

        ClassLoader loader = getDriverClassLoader();
        if (loader == null) {
            // use the classloader associated with the extension
            loader = Thread.currentThread().getContextClassLoader();
        }

        this.scriptManager = new ScriptEngineManager(loader);
        for (Map.Entry<String, Object> v : vars.entrySet()) {
            this.scriptManager.put(v.getKey(), v.getValue());
        }
    }

    protected ScriptEngine getScriptEngine(String schema, String query) {
        String extName = DEFAULT_SCRIPT_EXTENSION;

        if (schema != null && !schema.isEmpty()) {
            extName = schema;
        } else {
            // in case the "normalizedQuery" is a local file...
            if (query.indexOf('\n') == -1 && isSavedQuery(query) && Utils.fileExists(query)) {
                extName = query.substring(query.lastIndexOf('.') + 1);
            }
        }

        ScriptEngine engine = scriptManager.getEngineByExtension(extName);

        if (engine == null) {
            engine = scriptManager.getEngineByName(extName);
            if (engine == null) {
                throw new IllegalArgumentException("No script engine available for [" + extName + "]");
            }
        }

        return engine;
    }

    protected TableDefinition guessColumns(ScriptEngine engine, Object result, QueryParameters params) {
        TableDefinition columns = TableDefinition.DEFAULT_RESULT_COLUMNS;

        if (result == null) {
            try {
                try {
                    Invocable i = (Invocable) engine;
                    columns = TableDefinition.fromObject(i.invokeFunction(FUNC_INFER_TYPES));
                } catch (NoSuchMethodException e) {
                    // log.warn("Failed to infer types from given script", e);
                    columns = TableDefinition.fromObject(engine.get(FUNC_INFER_TYPES));
                }
            } catch (ScriptException e) {
                throw new IllegalStateException("Failed to execute given script", e);
            }
        } else if (result instanceof ResultSet) {
            try (JdbcDataSource jdbc = new JdbcDataSource(JdbcDataSource.EXTENSION_NAME, null, null)) {
                jdbc.getColumnsFromResultSet((ResultSet) result, params);
            } catch (SQLException e) {
                throw new DataAccessException(getId(), e);
            }
        } else {
            columns = new TableDefinition(new ColumnDefinition(Utils.DEFAULT_COLUMN_NAME, converter.from(result), true,
                    DEFAULT_LENGTH, DEFAULT_PRECISION, DEFAULT_SCALE));
        }

        return columns;
    }

    @Override
    protected boolean isSavedQuery(String file) {
        return file != null && file.indexOf('.') > 0;
    }

    @Override
    protected TableDefinition inferTypes(String schema, String originalQuery, String loadedQuery,
            QueryParameters params) {
        TableDefinition columns = TableDefinition.DEFAULT_RESULT_COLUMNS;

        // had to evaluate the script for type inferring...
        ScriptEngine engine = getScriptEngine(schema, originalQuery);

        try {
            columns = guessColumns(engine, engine.eval(loadedQuery), params);
        } catch (ScriptException e) {
            throw new DataAccessException(getId(), e);
        }

        return columns;
    }

    @Override
    protected void writeQueryResult(String schema, String originalQuery, String loadedQuery, QueryParameters params,
            ColumnDefinition[] requestColumns, ColumnDefinition[] customColumns, DefaultValues defaultValues,
            ResponseWriter writer) {
        ScriptEngine engine = getScriptEngine(schema, originalQuery);

        try {
            Object result = engine.eval(loadedQuery);

            ColumnDefinition[] resultColumns = guessColumns(engine, result, params).getColumns();

            if (result == null) {
                try {
                    Invocable i = (Invocable) engine;
                    result = i.invokeFunction(FUNC_GET_RESULTS);
                } catch (NoSuchMethodException e) {
                    // log.warn("Failed to get query results from given script", e);
                    result = engine.get(FUNC_GET_RESULTS);
                }
            }

            if (result instanceof ResultSet) {
                try (JdbcDataSource jdbc = new JdbcDataSource(JdbcDataSource.EXTENSION_NAME, null, null)) {
                    ResultSet rs = (ResultSet) result;
                    DataTableReader reader = new JdbcDataSource.ResultSetReader(getId(), rs, params);
                    reader.process(getId(), requestColumns, customColumns, jdbc.getColumnsFromResultSet(rs, params),
                            defaultValues, getTimeZone(), params, writer);
                } catch (SQLException e) {
                    throw new DataAccessException(getId(), e);
                }
            } else {
                String[] names = new String[resultColumns.length];
                for (int i = 0; i < names.length; i++) {
                    names[i] = resultColumns[i].getName();
                }

                DataTableReader reader = new ScriptResultReader(result, names);
                reader.process(getId(), requestColumns, customColumns, resultColumns, defaultValues, getTimeZone(),
                        params, writer);
            }
        } catch (ScriptException e) {
            throw new DataAccessException(getId(), e);
        }
    }

    @Override
    public void executeMutation(String schema, String table, TableDefinition columns, QueryParameters parameters,
            ByteBuffer buffer) {
        super.executeMutation(schema, table, columns, parameters, buffer);
    }
}
