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

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Objects;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.Map.Entry;

import com.clickhouse.jdbcbridge.core.ByteBuffer;
import com.clickhouse.jdbcbridge.core.ColumnDefinition;
import com.clickhouse.jdbcbridge.core.DataAccessException;
import com.clickhouse.jdbcbridge.core.DataTableReader;
import com.clickhouse.jdbcbridge.core.DataType;
import com.clickhouse.jdbcbridge.core.DefaultValues;
import com.clickhouse.jdbcbridge.core.Extension;
import com.clickhouse.jdbcbridge.core.ExtensionManager;
import com.clickhouse.jdbcbridge.core.NamedDataSource;
import com.clickhouse.jdbcbridge.core.QueryParameters;
import com.clickhouse.jdbcbridge.core.Repository;
import com.clickhouse.jdbcbridge.core.ResponseWriter;
import com.clickhouse.jdbcbridge.core.TableDefinition;
import com.clickhouse.jdbcbridge.core.Utils;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Meter.Id;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class JdbcDataSource extends NamedDataSource {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(JdbcDataSource.class);

    private static final Set<String> PRIVATE_PROPS = Collections
            .unmodifiableSet(new HashSet<>(Arrays.asList(CONF_SCHEMA, CONF_TYPE, CONF_TIMEZONE, CONF_CACHE,
                    CONF_ALIASES, CONF_DRIVER_URLS, CONF_QUERY_TIMEOUT, CONF_WRITE_TIMEOUT, CONF_SEALED)));

    private static final Properties DEFAULT_DATASOURCE_PROPERTIES = new Properties();

    private static final String PROP_DRIVER_CLASS = "driverClassName";
    private static final String PROP_INITIALIZATION_FAIL_TIMEOUT = "initializationFailTimeout";
    private static final String PROP_POOL_NAME = "poolName";
    private static final String PROP_PASSWORD = "password";

    private static final String PROP_CLIENT_NAME = "ClientUser";
    private static final String DEFAULT_CLIENT_NAME = "clickhouse-jdbc-bridge";

    private static final String QUERY_STMT_SELECT = "SELECT ";
    private static final String QUERY_STMT_FROM = " FROM ";
    private static final String QUERY_TABLE_BEGIN = QUERY_STMT_SELECT + "*" + QUERY_STMT_FROM;
    private static final String QUERY_TABLE_END = " WHERE 1 = 0";

    private static final String CONF_DATASOURCE = "dataSource";

    private static final String QUERY_FILE_EXT = ".sql";

    private static final String USAGE_PREFIX = "hikaricp.";
    private static final String USAGE_POOL = "pool";

    public static final String EXTENSION_NAME = "jdbc";

    static {
        // set default properties
        DEFAULT_DATASOURCE_PROPERTIES.setProperty("connectionTestQuery", "SELECT 1");
        DEFAULT_DATASOURCE_PROPERTIES.setProperty("minimumIdle", "1");
        DEFAULT_DATASOURCE_PROPERTIES.setProperty("maximumPoolSize", "5");
    }

    static class ResultSetReader implements DataTableReader {
        private final String id;
        private final ResultSet rs;
        private final QueryParameters params;

        protected ResultSetReader(String id, ResultSet rs, QueryParameters params) {
            this.id = id;
            this.rs = rs;
            this.params = params;
        }

        @Override
        public int skipRows(QueryParameters parameters) {
            int rowCount = 0;

            if (rs == null || parameters == null) {
                return rowCount;
            }

            int position = parameters.getPosition();
            int offset = parameters.getOffset();

            // absolute position takes priority
            if (position != 0 || (position = offset) < 0) {
                try {
                    rs.absolute(position);
                    // many JDBC drivers didn't validate position
                    // if you have only two rows in database, you can still use rs.position(100)...
                    // FIXME inaccurate row count here
                    rowCount = position;
                } catch (SQLException e) {
                    throw new IllegalStateException("Not able to move cursor to row #" + position, e);
                }
            } else if (offset != 0) {
                DataTableReader.super.skipRows(parameters);
            }

            return rowCount;
        }

        @Override
        public boolean nextRow() {
            try {
                return rs.next();
            } catch (SQLException e) {
                throw new DataAccessException(id, e);
            }
        }

        @Override
        public boolean isNull(int row, int column, ColumnDefinition metadata) {
            column++;

            try {
                return rs.getObject(column) == null || rs.wasNull();
            } catch (SQLException e) {
                throw new DataAccessException(id, e);
            }
        }

        @Override
        public void read(int row, int column, ColumnDefinition metadata, ByteBuffer buffer) {
            column++;

            try {
                Object value = null;
                switch (metadata.getType()) {
                    case Bool:
                    case Enum:
                    case Enum8:
                        value = rs.getObject(column);
                        if (value instanceof Integer) {
                            int optionValue = (int) value;
                            buffer.writeEnum8(metadata.requireValidOptionValue(optionValue));
                        } else { // treat as String
                            buffer.writeEnum8(metadata.getOptionValue(String.valueOf(value)));
                        }
                        break;
                    case Enum16:
                        value = rs.getObject(column);
                        if (value instanceof Integer) {
                            int optionValue = (int) value;
                            buffer.writeEnum16(metadata.requireValidOptionValue(optionValue));
                        } else { // treat as String
                            buffer.writeEnum16(metadata.getOptionValue(String.valueOf(value)));
                        }
                        break;
                    case Int8:
                        buffer.writeInt8(rs.getInt(column));
                        break;
                    case Int16:
                        buffer.writeInt16(rs.getInt(column));
                        break;
                    case Int32:
                        buffer.writeInt32(rs.getInt(column));
                        break;
                    case Int64:
                        buffer.writeInt64(rs.getLong(column));
                        break;
                    case Int128:
                        buffer.writeInt128(rs.getObject(column, java.math.BigInteger.class));
                        break;
                    case Int256:
                        buffer.writeInt256(rs.getObject(column, java.math.BigInteger.class));
                        break;
                    case UInt8:
                        buffer.writeUInt8(rs.getInt(column));
                        break;
                    case UInt16:
                        buffer.writeUInt16(rs.getInt(column));
                        break;
                    case UInt32:
                        buffer.writeUInt32(rs.getLong(column));
                        break;
                    case UInt64:
                        buffer.writeUInt64(rs.getLong(column));
                        break;
                    case UInt128:
                        buffer.writeUInt128(rs.getObject(column, java.math.BigInteger.class));
                        break;
                    case UInt256:
                        buffer.writeUInt256(rs.getObject(column, java.math.BigInteger.class));
                        break;
                    case Float32:
                        buffer.writeFloat32(rs.getFloat(column));
                        break;
                    case Float64:
                        buffer.writeFloat64(rs.getDouble(column));
                        break;
                    case Date:
                        buffer.writeDate(rs.getDate(column));
                        break;
                    case DateTime:
                        buffer.writeDateTime(rs.getTimestamp(column), metadata.getTimeZone());
                        break;
                    case DateTime64:
                        buffer.writeDateTime64(rs.getTimestamp(column), metadata.getScale(), metadata.getTimeZone());
                        break;
                    case Decimal:
                        buffer.writeDecimal(rs.getBigDecimal(column), metadata.getPrecision(), metadata.getScale());
                        break;
                    case Decimal32:
                        buffer.writeDecimal32(rs.getBigDecimal(column), metadata.getScale());
                        break;
                    case Decimal64:
                        buffer.writeDecimal64(rs.getBigDecimal(column), metadata.getScale());
                        break;
                    case Decimal128:
                        buffer.writeDecimal128(rs.getBigDecimal(column), metadata.getScale());
                        break;
                    case Decimal256:
                        buffer.writeDecimal256(rs.getBigDecimal(column), metadata.getScale());
                        break;
                    case FixedStr:
                        buffer.writeFixedString(rs.getString(column), metadata.getLength());
                        break;
                    case Str:
                    default:
                        buffer.writeString(rs.getString(column), params.nullAsDefault());
                        break;
                }
            } catch (SQLException e) {
                throw new DataAccessException(id, e);
            }
        }
    }

    protected static void deregisterJdbcDriver(String driverClassName) {
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            Driver driver = drivers.nextElement();
            if (driver.getClass().getName().equals(driverClassName)) {
                try {
                    DriverManager.deregisterDriver(driver);
                } catch (SQLException e) {
                    log.error("Failed to deregister driver: " + driver, e);
                }
            }
        }
    }

    public static void initialize(ExtensionManager manager) {
        Extension<NamedDataSource> thisExtension = manager.getExtension(JdbcDataSource.class);
        manager.getRepositoryManager().getRepository(NamedDataSource.class).registerType(EXTENSION_NAME, thisExtension);
    }

    @SuppressWarnings("unchecked")
    public static JdbcDataSource newInstance(Object... args) {
        if (Objects.requireNonNull(args).length < 2) {
            throw new IllegalArgumentException(
                    "In order to create JDBC datasource, you need to specify at least ID and datasource manager.");
        }

        String id = (String) args[0];
        Repository<NamedDataSource> manager = (Repository<NamedDataSource>) Objects.requireNonNull(args[1]);
        JsonObject config = args.length > 2 ? (JsonObject) args[2] : null;

        JdbcDataSource ds = new JdbcDataSource(id, manager, config);
        ds.validate();

        return ds;
    }

    private final String jdbcUrl;
    private final HikariDataSource datasource;

    // cached identifier quote
    private String quoteIdentifier = null;

    private int bulkMutation(PreparedStatement stmt) throws SQLException {
        int mutationCount = 0;

        int[] results = stmt.executeBatch();
        for (int i = 0; i < results.length; i++) {
            mutationCount += results[i];
        }
        stmt.clearBatch();

        return mutationCount;
    }

    private String buildErrorMessage(Throwable t) {
        StringBuilder err = new StringBuilder();

        if (t instanceof SQLException) {
            SQLException exp = (SQLException) t;

            String state = exp.getSQLState();
            int code = exp.getErrorCode();

            if (state != null && state.length() > 0) {
                err.append("SQLState(").append(state).append(')').append(' ');
            }
            err.append("VendorCode(").append(code).append(')').append(' ').append(exp.getMessage());
        } else {
            err.append(t == null ? "Unknown error: " : t.getMessage());
        }

        Throwable rootCause = t;
        while (rootCause.getCause() != null && rootCause.getCause() != rootCause) {
            rootCause = rootCause.getCause();
        }
        if (rootCause != t) {
            err.append('\n').append("Root cause: ").append(rootCause.getMessage());
        }

        return err.toString();
    }

    protected Driver findDriver(String url) {
        ServiceLoader<Driver> loader = ServiceLoader.load(Driver.class, this.getDriverClassLoader());
        for (Driver d : loader) {
            try {
                if (d.acceptsURL(url)) {
                    return d;
                }
            } catch (SQLException e) {
                log.warn("Error occured when testing driver [{}] due to [{}]", d, e.getMessage());
            }
        }

        throw new IllegalStateException("Not able to find suitable driver for datasource: " + this.getId());
    }

    protected JdbcDataSource(String id, Repository<NamedDataSource> resolver, JsonObject config) {
        super(id, resolver, config);

        Properties props = new Properties();
        props.putAll(DEFAULT_DATASOURCE_PROPERTIES);

        if (id != null && id.startsWith(EXTENSION_NAME) && config == null) { // adhoc
            this.jdbcUrl = id;
            this.datasource = null;
        } else { // named
            if (config != null) {
                for (Entry<String, Object> field : config) {
                    String key = field.getKey();

                    if (PRIVATE_PROPS.contains(key)) {
                        continue;
                    }

                    Object value = field.getValue();

                    if (value instanceof JsonObject) {
                        if (CONF_DATASOURCE.equals(key)) {
                            for (Entry<String, Object> entry : (JsonObject) value) {
                                String propName = entry.getKey();
                                String propValue = String.valueOf(entry.getValue());
                                if (!PROP_PASSWORD.equals(propName)) {
                                    propValue = resolver.resolve(propValue);
                                }

                                props.setProperty(
                                        new StringBuilder().append(key).append('.').append(propName).toString(),
                                        propValue);
                            }
                        }
                    } else if (value != null && !(value instanceof JsonArray)) {
                        String propValue = String.valueOf(value);
                        if (!PROP_PASSWORD.equals(key)) {
                            propValue = resolver.resolve(propValue);
                        }
                        props.setProperty(key, propValue);
                    }
                }
            }

            if (!props.containsKey(PROP_INITIALIZATION_FAIL_TIMEOUT)) {
                props.setProperty(PROP_INITIALIZATION_FAIL_TIMEOUT, "0");
            }
            props.setProperty(PROP_POOL_NAME, id);

            this.jdbcUrl = null;

            if (USE_CUSTOM_DRIVER_LOADER) {
                String driverClassName = props.getProperty(PROP_DRIVER_CLASS);

                if (driverClassName == null || driverClassName.isEmpty()) {
                    String url = props.getProperty(CONF_JDBC_URL);
                    if (url == null || url.isEmpty()) {
                        throw new IllegalArgumentException(CONF_JDBC_URL + " was not specified!");
                    }

                    props.setProperty(PROP_DRIVER_CLASS, driverClassName = findDriver(url).getClass().getName());
                }

                // in case there's any driver in classpath was loaded, which might not be the
                // exact version we need
                deregisterJdbcDriver(driverClassName);

                Thread currentThread = Thread.currentThread();
                ClassLoader currentContextClassLoader = currentThread.getContextClassLoader();

                try {
                    ClassLoader loader = this.getDriverClassLoader();
                    currentThread.setContextClassLoader(loader);

                    // FIXME not thread-safe
                    HikariConfig conf = new HikariConfig(props);
                    conf.setMetricRegistry(Utils.getDefaultMetricRegistry());
                    this.datasource = new HikariDataSource(conf);
                } finally {
                    currentThread.setContextClassLoader(currentContextClassLoader);
                }
            } else {
                HikariConfig conf = new HikariConfig(props);
                conf.setMetricRegistry(Utils.getDefaultMetricRegistry());
                this.datasource = new HikariDataSource(conf);
            }
        }
    }

    protected final Connection getConnection() throws SQLException {
        final Connection conn;

        if (this.datasource != null) {
            conn = this.datasource.getConnection();
        } else {
            conn = findDriver(this.jdbcUrl).connect(this.jdbcUrl, new Properties());

            this.initQuoteIdentifier(conn);

            try {
                conn.setAutoCommit(true);
            } catch (Throwable e) {
                log.warn("Failed to enable auto-commit due to {}", e.getMessage());
            }
        }

        try {
            conn.setClientInfo(PROP_CLIENT_NAME, DEFAULT_CLIENT_NAME);
        } catch (Throwable e) {
            log.warn("Failed call setClientInfo due to {}", e.getMessage());
        }

        return conn;
    }

    protected final Statement createStatement(Connection conn) throws SQLException {
        return createStatement(conn, null);
    }

    protected final Statement createStatement(Connection conn, QueryParameters parameters) throws SQLException {
        final Statement stmt;

        if (parameters == null) {
            stmt = conn.createStatement();
        } else {
            boolean scrollable = parameters.getPosition() != 0;
            stmt = conn.createStatement(scrollable ? ResultSet.TYPE_SCROLL_INSENSITIVE : ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);

            stmt.setFetchSize(parameters.getFetchSize());
            stmt.setMaxRows(parameters.getMaxRows());
        }

        return stmt;
    }

    protected final PreparedStatement createPreparedStatement(Connection conn, String sql, QueryParameters parameters)
            throws SQLException {
        log.info("Mutation: {}", sql);

        return conn.prepareStatement(sql);
    }

    protected final void setTimeout(Statement stmt, int expectedTimeout) {
        int currentTimeout = 0;
        try {
            currentTimeout = stmt.getQueryTimeout();
        } catch (Exception e) {
        }

        // change timeout only when needed
        if (currentTimeout != expectedTimeout && expectedTimeout >= 0) {
            try {
                stmt.setQueryTimeout(expectedTimeout);
            } catch (Exception e) {
                log.warn("Not able to set query timeout to {} seconds", expectedTimeout);
            }
        }
    }

    protected long getFirstMutationResult(Statement stmt) throws SQLException {
        long count = 0L;

        try {
            count = stmt.getLargeUpdateCount();
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            count = stmt.getUpdateCount();
        }

        return count == -1 ? 0 : count;
    }

    protected ResultSet getFirstQueryResult(Statement stmt, boolean hasResultSet) throws SQLException {
        ResultSet rs = null;

        if (hasResultSet) {
            rs = stmt.getResultSet();
        } else if (stmt.getUpdateCount() == -1) {
            throw new SQLException("No query result!");
        }

        return rs != null ? rs : getFirstQueryResult(stmt, stmt.getMoreResults());
    }

    protected String getColumnName(ResultSetMetaData meta, int columnIndex) throws SQLException {
        String columnName = null;

        boolean fallback = true;
        try {
            columnName = meta.getColumnLabel(columnIndex);
            if (columnName == null || columnName.isEmpty()) {
                fallback = false;
                columnName = meta.getColumnName(columnIndex);
            }
        } catch (RuntimeException e) {
            // in case get column label was not supported
            if (fallback) {
                columnName = meta.getColumnName(columnIndex);
            }
        }

        if (columnName == null || columnName.isEmpty()) {
            columnName = generateColumnName(columnIndex);
        }

        return columnName;
    }

    protected ColumnDefinition[] getColumnsFromResultSet(ResultSet rs, QueryParameters params) throws SQLException {
        ResultSetMetaData meta = Objects.requireNonNull(rs).getMetaData();

        ColumnDefinition[] columns = new ColumnDefinition[meta.getColumnCount()];

        for (int i = 1; i <= columns.length; i++) {
            boolean isSigned = true;
            int nullability = ResultSetMetaData.columnNullable;
            int length = 0;
            int precision = 0;
            int scale = 0;

            // Why try-catch? Try a not-fully implemented JDBC driver and you'll see...
            try {
                isSigned = meta.isSigned(i);
            } catch (Exception e) {
            }

            try {
                nullability = meta.isNullable(i);
            } catch (Exception e) {
            }

            try {
                length = meta.getColumnDisplaySize(i);
            } catch (Exception e) {
            }

            try {
                precision = meta.getPrecision(i);
            } catch (Exception e) {
            }

            try {
                scale = meta.getScale(i);
            } catch (Exception e) {
            }

            String name = getColumnName(meta, i);
            String typeName = meta.getColumnTypeName(i);
            JDBCType jdbcType = JDBCType.valueOf(meta.getColumnType(i));
            DataType type = converter.from(jdbcType, typeName, precision, scale, isSigned);

            columns[i - 1] = new ColumnDefinition(name, type, ResultSetMetaData.columnNoNulls != nullability, length,
                    precision, scale);
        }

        return columns;
    }

    @Override
    protected TableDefinition inferTypes(String schema, String originalQuery, String loadedQuery,
            QueryParameters params) {
        try (Connection conn = getConnection(); Statement stmt = createStatement(conn)) {
            setTimeout(stmt, this.getQueryTimeout(params.getTimeout()));
            stmt.setMaxRows(1);
            stmt.setFetchSize(1);

            // in case it's a table query
            if (!Utils.containsWhitespace(loadedQuery)) {
                // let's generate a query based on given schema name, table name and column list
                String quote = this.getQuoteIdentifier();
                StringBuilder sb = new StringBuilder().append(QUERY_TABLE_BEGIN);
                // add schema name if any
                if (schema != null && !schema.isEmpty() && !Utils.containsWhitespace(schema)) {
                    sb.append(quote).append(schema).append(quote).append('.');
                }
                loadedQuery = sb.append(quote).append(loadedQuery).append(quote).append(QUERY_TABLE_END).toString();
            }

            if (loadedQuery != null && loadedQuery.indexOf(' ') == -1) {
                StringBuilder sb = new StringBuilder().append(QUERY_TABLE_BEGIN);
                String quote = this.getQuoteIdentifier();
                if (schema != null && schema.length() > 0) {
                    sb.append(quote).append(schema).append(quote).append('.');
                }
                loadedQuery = sb.append(quote).append(loadedQuery).append(quote).append(QUERY_TABLE_END).toString();
            }

            // could be very slow...
            ColumnDefinition[] columns = getColumnsFromResultSet(getFirstQueryResult(stmt, stmt.execute(loadedQuery)),
                    params);

            return new TableDefinition(columns);
        } catch (SQLException e) {
            throw new DataAccessException(getId(), e);
        }
    }

    @Override
    protected boolean isSavedQuery(String file) {
        return super.isSavedQuery(file) || file.endsWith(QUERY_FILE_EXT);
    }

    @Override
    protected void writeMutationResult(String schema, String originalQuery, String loadedQuery, QueryParameters params,
            ColumnDefinition[] requestColumns, ColumnDefinition[] customColumns, DefaultValues defaultValues,
            ResponseWriter writer) {
        try (Connection conn = getConnection(); Statement stmt = createStatement(conn, params)) {
            setTimeout(stmt, this.getQueryTimeout(params.getTimeout()));

            stmt.execute(loadedQuery);
            this.writeMutationResult(getFirstMutationResult(stmt), requestColumns, customColumns, writer);
        } catch (SQLException e) {
            throw new DataAccessException(getId(), buildErrorMessage(e), e);
        } catch (DataAccessException e) {
            Throwable cause = e.getCause();
            throw new IllegalStateException(
                    "Failed to mutate against [" + this.getId() + "] due to: " + buildErrorMessage(cause), cause);
        }
    }

    @Override
    protected void writeQueryResult(String schema, String originalQuery, String loadedQuery, QueryParameters params,
            ColumnDefinition[] requestColumns, ColumnDefinition[] customColumns, DefaultValues defaultValues,
            ResponseWriter writer) {
        // check if it's a table query
        if (!Utils.containsWhitespace(loadedQuery)) {
            // let's generate a query based on given schema name, table name and column list
            String quote = this.getQuoteIdentifier();
            StringBuilder sb = new StringBuilder();
            sb.append(QUERY_STMT_SELECT);
            if (requestColumns == null || requestColumns.length == 0) {
                sb.append('*');
            } else {
                for (ColumnDefinition c : requestColumns) {
                    sb.append(quote).append(c.getName()).append(quote).append(',');
                }
                sb.deleteCharAt(sb.length() - 1);
            }
            sb.append(QUERY_STMT_FROM);
            // add schema name if any
            if (schema != null && !schema.isEmpty() && !Utils.containsWhitespace(schema)) {
                sb.append(quote).append(schema).append(quote).append('.');
            }
            // now table name
            sb.append(quote).append(loadedQuery).append(quote);
            loadedQuery = sb.toString();
        }

        try (Connection conn = getConnection(); Statement stmt = createStatement(conn, params)) {
            setTimeout(stmt, this.getQueryTimeout(params.getTimeout()));

            final ResultSet rs = getFirstQueryResult(stmt, stmt.execute(loadedQuery));

            DataTableReader reader = new ResultSetReader(getId(), rs, params);
            reader.process(getId(), requestColumns, customColumns, getColumnsFromResultSet(rs, params), defaultValues,
                    getTimeZone(), params, writer);

            /*
             * if (stmt.execute(loadedQuery)) { // TODO multiple resultsets
             * 
             * } else if (columns.size() == 1 && columns.getColumn(0).getType() ==
             * ClickHouseDataType.Int32) {
             * writer.write(ClickHouseBuffer.newInstance(4).writeInt32(stmt.getUpdateCount()
             * )); } else { throw new IllegalStateException(
             * "Not able to handle query result due to incompatible columns: " + columns); }
             */
        } catch (SQLException e) {
            throw new DataAccessException(getId(), buildErrorMessage(e), e);
        } catch (DataAccessException e) {
            Throwable cause = e.getCause();
            throw new IllegalStateException(
                    "Failed to query against [" + this.getId() + "] due to: " + buildErrorMessage(cause), cause);
        }
    }

    protected final void write(PreparedStatement stmt, ColumnDefinition[] cols, QueryParameters params,
            ByteBuffer buffer) throws SQLException {
        for (int i = 1; i <= cols.length; i++) {
            ColumnDefinition info = cols[i - 1];
            if (info.isNullable() && buffer.readNull()) {
                // stmt.setNull(i, info.getT);
                stmt.setString(i, null);
                continue;
            }

            switch (info.getType()) {
                case Bool:
                case Int8:
                    stmt.setByte(i, buffer.readInt8());
                    break;
                case Int16:
                    stmt.setShort(i, buffer.readInt16());
                    break;
                case Int32:
                    stmt.setInt(i, buffer.readInt32());
                    break;
                case Int64:
                    stmt.setLong(i, buffer.readInt64());
                    break;
                case Int128:
                    stmt.setBigDecimal(i, new BigDecimal(buffer.readInt128()));
                    break;
                case Int256:
                    stmt.setBigDecimal(i, new BigDecimal(buffer.readInt256()));
                    break;
                case UInt8:
                    stmt.setInt(i, buffer.readUInt8());
                    break;
                case UInt16:
                    stmt.setInt(i, buffer.readUInt16());
                    break;
                case UInt32:
                    stmt.setLong(i, buffer.readUInt32());
                    break;
                case UInt64:
                    stmt.setString(i, buffer.readUInt64().toString(10));
                    break;
                case UInt128:
                    stmt.setBigDecimal(i, new BigDecimal(buffer.readUInt128()));
                    break;
                case UInt256:
                    stmt.setBigDecimal(i, new BigDecimal(buffer.readUInt256()));
                    break;
                case Float32:
                    stmt.setFloat(i, buffer.readFloat32());
                    break;
                case Float64:
                    stmt.setDouble(i, buffer.readFloat64());
                    break;
                case Date:
                    stmt.setDate(i, buffer.readDate());
                    break;
                case DateTime:
                    stmt.setTimestamp(i, buffer.readDateTime(info.getTimeZone()));
                    break;
                case DateTime64:
                    stmt.setTimestamp(i, buffer.readDateTime64(info.getTimeZone()));
                    break;
                case Decimal:
                    stmt.setBigDecimal(i, buffer.readDecimal(info.getPrecision(), info.getScale()));
                    break;
                case Decimal32:
                    stmt.setBigDecimal(i, buffer.readDecimal32(info.getScale()));
                    break;
                case Decimal64:
                    stmt.setBigDecimal(i, buffer.readDecimal64(info.getScale()));
                    break;
                case Decimal128:
                    stmt.setBigDecimal(i, buffer.readDecimal128(info.getScale()));
                    break;
                case Decimal256:
                    stmt.setBigDecimal(i, buffer.readDecimal256(info.getScale()));
                    break;
                case Str:
                    stmt.setString(i, buffer.readString());
                    break;
                default:
                    break;
            }

        }
    }

    protected final void initQuoteIdentifier(Connection conn) {
        if (this.quoteIdentifier == null) {
            synchronized (this) {
                if (this.quoteIdentifier == null) {
                    this.quoteIdentifier = DEFAULT_QUOTE_IDENTIFIER;

                    String errorMsg = "Failed to get identifier quote string due to {}";
                    String str = null;
                    if (conn != null) {
                        try {
                            str = conn.getMetaData().getIdentifierQuoteString();
                        } catch (Exception e) {
                            log.warn(errorMsg, e.getMessage());
                        }
                    } else {
                        try (Connection c = getConnection()) {
                            str = c.getMetaData().getIdentifierQuoteString();
                        } catch (Exception e) {
                            log.warn(errorMsg, e.getMessage());
                        }
                    }

                    if (str != null && !str.trim().isEmpty()) {
                        this.quoteIdentifier = str;
                    }
                }
            }
        }
    }

    @Override
    public final String getType() {
        return EXTENSION_NAME;
    }

    @Override
    public final String getQuoteIdentifier() {
        this.initQuoteIdentifier(null);

        return this.quoteIdentifier;
    }

    @Override
    public String getPoolUsage() {
        JsonObject obj = new JsonObject();

        Object metricRegistry = Utils.getDefaultMetricRegistry();
        if (metricRegistry instanceof MeterRegistry) {
            for (Meter meter : ((MeterRegistry) metricRegistry).getMeters()) {
                Id meterId = meter.getId();
                String name = meterId.getName();

                if (name != null && name.startsWith(USAGE_PREFIX) && this.getId().equals(meterId.getTag(USAGE_POOL))) {
                    name = name.substring(USAGE_PREFIX.length()).replace('.', '_');
                    for (Measurement m : meter.measure()) {
                        obj.put(name + "_" + m.getStatistic().getTagValueRepresentation(), m.getValue());
                    }
                }
            }
        }

        return obj.toString();
    }

    @Override
    public void executeMutation(String schema, String table, TableDefinition columns, QueryParameters params,
            ByteBuffer buffer, ResponseWriter writer) {
        log.info("Executing mutation: schema=[{}], table=[{}]", schema, table);

        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ");
        if (schema != null && schema.length() > 0 && table.indexOf('.') == -1) {
            sql.append(schema).append('.');
        }
        sql.append(table).append(" VALUES(?");

        final ColumnDefinition[] cols = columns.getColumns();
        for (int i = 1; i < cols.length; i++) {
            sql.append(',').append('?');
        }
        sql.append(')');

        int batchSize = params.getBatchSize();
        int rowCount = 0;

        int mutationCount = 0;

        try (Connection conn = getConnection();
                PreparedStatement stmt = createPreparedStatement(conn, sql.toString(), params)) {
            setTimeout(stmt, this.getWriteTimeout(params.getTimeout()));

            int counter = 0;
            boolean stopped = false;
            while (!buffer.isExausted()) {
                write(stmt, cols, params, buffer);
                rowCount++;

                if (batchSize <= 0) {
                    mutationCount += stmt.executeUpdate();
                } else {
                    stmt.addBatch();

                    if (++counter >= batchSize) {
                        mutationCount += this.bulkMutation(stmt);
                        counter = 0;
                    }
                }

                if (!writer.isOpen()) {
                    stopped = true;
                    break;
                }
            }

            if (!stopped && batchSize > 0 && counter > 0) {
                mutationCount += this.bulkMutation(stmt);
            }

            log.info("Mutation {} on [{}]: batchSize={}, inputRows={}, effectedRows={}",
                    stopped ? "stopped" : "completed", this.getId(), batchSize, rowCount, mutationCount);
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to mutate in [" + this.getId() + "] due to: " + buildErrorMessage(e), e);
        }
    }

    @Override
    public void close() {
        super.close();

        if (this.datasource != null) {
            this.datasource.close();
        }
    }
}