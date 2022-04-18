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
package com.clickhouse.jdbcbridge.impl;

import static com.clickhouse.jdbcbridge.core.DataType.DEFAULT_LENGTH;
import static com.clickhouse.jdbcbridge.core.DataType.DEFAULT_PRECISION;
import static com.clickhouse.jdbcbridge.core.DataType.DEFAULT_SCALE;

import java.math.BigDecimal;
import java.math.BigInteger;
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

import com.clickhouse.jdbcbridge.core.ByteBuffer;
import com.clickhouse.jdbcbridge.core.ColumnDefinition;
import com.clickhouse.jdbcbridge.core.DataAccessException;
import com.clickhouse.jdbcbridge.core.DataTableReader;
import com.clickhouse.jdbcbridge.core.DataTypeConverter;
import com.clickhouse.jdbcbridge.core.DefaultValues;
import com.clickhouse.jdbcbridge.core.Extension;
import com.clickhouse.jdbcbridge.core.ExtensionManager;
import com.clickhouse.jdbcbridge.core.NamedDataSource;
import com.clickhouse.jdbcbridge.core.QueryParameters;
import com.clickhouse.jdbcbridge.core.Repository;
import com.clickhouse.jdbcbridge.core.ResponseWriter;
import com.clickhouse.jdbcbridge.core.TableDefinition;
import com.clickhouse.jdbcbridge.core.Utils;

import io.vertx.core.json.JsonObject;

public class ScriptDataSource extends NamedDataSource {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ScriptDataSource.class);

    private static final Map<String, Object> vars = new HashMap<>();

    public static final String EXTENSION_NAME = "script";

    public static final String DEFAULT_SCRIPT_EXTENSION = "js";

    public static final String FUNC_INFER_TYPES = "__types__";
    public static final String FUNC_GET_RESULTS = "__results__";

    static class ScriptResultReader implements DataTableReader {
        private final DataTypeConverter converter;
        private final Object[][] values;

        private int currentRow = 0;

        protected ScriptResultReader(DataTypeConverter converter, Object result, String... columnNames) {
            this.converter = Objects.requireNonNull(converter);
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
                case Enum:
                case Enum8:
                    try {
                        v = converter.as(Integer.class, v);
                    } catch (NumberFormatException e) {
                        // pass
                    }

                    if (v instanceof Integer) {
                        int optionValue = (int) v;
                        buffer.writeEnum8(metadata.requireValidOptionValue(optionValue));
                    } else { // treat as String
                        buffer.writeEnum8(metadata.getOptionValue(String.valueOf(v)));
                    }
                    break;
                case Enum16:
                    try {
                        v = converter.as(Integer.class, v);
                    } catch (NumberFormatException e) {
                        // pass
                    }

                    if (v instanceof Integer) {
                        int optionValue = (int) v;
                        buffer.writeEnum16(metadata.requireValidOptionValue(optionValue));
                    } else { // treat as String
                        buffer.writeEnum16(metadata.getOptionValue(String.valueOf(v)));
                    }
                    break;
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
                case Int128:
                    buffer.writeInt128(converter.as(BigInteger.class, v));
                    break;
                case Int256:
                    buffer.writeInt256(converter.as(BigInteger.class, v));
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
                case UInt128:
                    buffer.writeUInt128(converter.as(BigInteger.class, v));
                    break;
                case UInt256:
                    buffer.writeUInt256(converter.as(BigInteger.class, v));
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
                    buffer.writeDateTime64(converter.as(Date.class, v), metadata.getScale(), metadata.getTimeZone());
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

        Repository<NamedDataSource> dsRepo = manager.getRepositoryManager().getRepository(NamedDataSource.class);

        Extension<NamedDataSource> thisExtension = manager.getExtension(ScriptDataSource.class);
        dsRepo.registerType(EXTENSION_NAME, thisExtension);
    }

    @SuppressWarnings("unchecked")
    public static ScriptDataSource newInstance(Object... args) {
        if (Objects.requireNonNull(args).length < 2) {
            throw new IllegalArgumentException(
                    "In order to create JDBC datasource, you need to specify at least ID and datasource manager.");
        }

        String id = (String) args[0];
        Repository<NamedDataSource> manager = (Repository<NamedDataSource>) Objects.requireNonNull(args[1]);
        JsonObject config = args.length > 2 ? (JsonObject) args[2] : null;

        ScriptDataSource ds = new ScriptDataSource(id, manager, config);
        ds.validate();

        return ds;
    }

    private final ScriptEngineManager scriptManager;

    protected ScriptDataSource(String id, Repository<NamedDataSource> manager, JsonObject config) {
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

        if (schema != null && !schema.isEmpty() && schema.indexOf(' ') == -1) {
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

        if (log.isDebugEnabled()) {
            log.debug("Got result from script engine: [{}]", result == null ? null : result.getClass().getName());
        }

        if (result == null) {
            if (log.isDebugEnabled()) {
                log.debug("Trying to infer types by calling function [{}] or reading variable with same name",
                        FUNC_INFER_TYPES);
            }
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
            if (log.isDebugEnabled()) {
                log.debug("Trying to infer types from JDBC ResultSet");
            }
            try (JdbcDataSource jdbc = new JdbcDataSource(JdbcDataSource.EXTENSION_NAME, null, null)) {
                jdbc.getColumnsFromResultSet((ResultSet) result, params);
            } catch (SQLException e) {
                throw new DataAccessException(getId(), e);
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("No clue on types so let's go with default");
            }

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

            ColumnDefinition[] resultColumns = requestColumns.length > 1
                    && !Utils.DEFAULT_COLUMN_NAME.equals(requestColumns[0].getName()) ? requestColumns
                            : guessColumns(engine, result, params).getColumns();

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

                DataTableReader reader = new ScriptResultReader(converter, result, names);
                reader.process(getId(), requestColumns, customColumns, resultColumns, defaultValues, getTimeZone(),
                        params, writer);
            }
        } catch (ScriptException e) {
            throw new DataAccessException(getId(), e);
        }
    }

    @Override
    public void executeMutation(String schema, String table, TableDefinition columns, QueryParameters parameters,
            ByteBuffer buffer, ResponseWriter writer) {
        super.executeMutation(schema, table, columns, parameters, buffer, writer);
    }
}
