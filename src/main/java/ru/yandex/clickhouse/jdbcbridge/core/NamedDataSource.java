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
package ru.yandex.clickhouse.jdbcbridge.core;

import java.io.Closeable;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import static ru.yandex.clickhouse.jdbcbridge.core.DataType.*;

/**
 * This class defines a named datasource. It's the base class of all other types
 * of datasources.
 * 
 * @since 2.0
 */
public class NamedDataSource implements Closeable {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(NamedDataSource.class);

    private static final String DATASOURCE_TYPE = "general";

    protected static final String CONF_ID = "id";

    protected static final String CONF_CACHE = "cache";
    protected static final String CONF_SIZE = "size";
    protected static final String CONF_EXPIRATION = "expiration";

    protected static final String CONF_ALIASES = "aliases";
    protected static final String CONF_COLUMNS = "columns";
    protected static final String CONF_DEFAULTS = "defaults";
    protected static final String CONF_DRIVER_URLS = "driverUrls";
    protected static final String CONF_PARAMETERS = "parameters";

    protected static final String EMPTY_USAGE = "{}";

    protected static final String CACHE_STAT_HIT_COUNT = "hitCount";
    protected static final String CACHE_STAT_MISS_COUNT = "missCount";
    protected static final String CACHE_STAT_LOAD_SUCCESS_COUNT = "loadSuccessCount";
    protected static final String CACHE_STAT_LOAD_FAILURE_COUNT = "loadFailureCount";
    protected static final String CACHE_STAT_TOTAL_LOAD_TIME = "totalLoadTime";
    protected static final String CACHE_STAT_EVICTION_COUNT = "evictionCount";
    protected static final String CACHE_STAT_EVICTION_WEIGHT = "evictionWeight";

    protected static final String COLUMN_PREFIX = "col_";

    protected static final DataTypeConverter converter = Utils.loadService(DataTypeConverter.class);

    // See all supported values defined in:
    // https://github.com/ClickHouse/ClickHouse/blob/master/src/Parsers/IdentifierQuotingStyle.h
    public static final String DEFAULT_QUOTE_IDENTIFIER = "`";

    public static final String CONF_SCHEMA = "$schema";
    public static final String CONF_TYPE = "type";
    public static final String CONF_TIMEZONE = "timezone";
    public static final String CONF_DATETIME = "datetime";
    public static final String CONF_QUERY_TIMEOUT = "queryTimeout";
    public static final String CONF_WRITE_TIMEOUT = "writeTimeout";
    public static final String CONF_SEALED = "sealed";

    protected static final boolean USE_CUSTOM_DRIVER_LOADER = Boolean
            .valueOf(Utils.getConfiguration("true", "CUSTOM_DRIVER_LOADER", "jdbc-bridge.driver.loader"));

    private static final String QUERY_FILE_EXT = ".query";

    private static final ClassLoader DEFAULT_DRIVER_CLASSLOADER = new ExpandedUrlClassLoader(
            NamedDataSource.class.getClassLoader(),
            Paths.get(Utils.getConfiguration("drivers", "DRIVER_DIR", "jdbc-bridge.driver.dir")).toFile()
                    .getAbsolutePath());

    private final Cache<String, TableDefinition> columnsCache;

    private final String id;
    private final SortedSet<String> aliases;

    private final Date createDateTime;

    private final SortedSet<String> driverUrls;
    private final ClassLoader driverClassLoader;

    private final String digest;

    private final boolean dateTime;
    private final TimeZone timezone;
    private final int queryTimeout;
    private final int writeTimeout;
    private final boolean sealed;
    private final List<ColumnDefinition> customColumns;
    private final DefaultValues defaultValues;
    private final QueryParameters queryParameters;

    public static NamedDataSource newInstance(Object... args) {
        if (Objects.requireNonNull(args).length < 2) {
            throw new IllegalArgumentException(
                    "In order to create named datasource, you need to specify at least ID and datasource manager.");
        }

        String id = (String) args[0];
        DataSourceManager manager = (DataSourceManager) Objects.requireNonNull(args[1]);
        JsonObject config = args.length > 2 ? (JsonObject) args[2] : null;

        return new NamedDataSource(id, manager, config);
    }

    protected static String generateColumnName(int columnIndex) {
        return new StringBuilder().append(COLUMN_PREFIX).append(columnIndex).toString();
    }

    protected TableDefinition inferTypes(String schema, String originalQuery, String loadedQuery,
            QueryParameters params) {
        return TableDefinition.DEBUG_COLUMNS;
    }

    protected boolean isSavedQuery(String file) {
        return Objects.requireNonNull(file).endsWith(QUERY_FILE_EXT);
    }

    private void writeDebugResult(String schema, String originalQuery, String loadedQuery, QueryParameters parameters,
            TableDefinition metaData, ResponseWriter writer) {
        if (metaData == null) {
            metaData = new TableDefinition();
        }

        ByteBuffer buffer = ByteBuffer.newInstance(loadedQuery.length() * 4);

        StringBuilder sb = new StringBuilder(metaData.size() * 10);
        for (ColumnDefinition info : metaData.getColumns()) {
            sb.append(',').append('{').append('"').append(info.getName()).append('"').append(',')
                    .append(converter.toMType(info.getType())).append('}');
        }
        if (sb.length() > 0) {
            sb.deleteCharAt(0);
        }
        sb.insert(0, '{').append('}');

        for (String str : new String[] { getId(), getType(), metaData.toJsonString(loadedQuery), sb.toString(),
                loadedQuery, parameters == null ? null : parameters.toQueryString() }) {
            if (str == null) {
                buffer.writeNull();
            } else {
                buffer.writeNonNull().writeString(str);
            }
        }

        Objects.requireNonNull(writer).write(buffer);
    }

    protected void writeMutationResult(String schema, String originalQuery, String loadedQuery, QueryParameters params,
            ColumnDefinition[] requestColumns, ColumnDefinition[] customColumns, DefaultValues defaultValues,
            ResponseWriter writer) {
    }

    protected void writeQueryResult(String schema, String originalQuery, String loadedQuery, QueryParameters params,
            ColumnDefinition[] requestColumns, ColumnDefinition[] customColumns, DefaultValues defaultValues,
            ResponseWriter writer) {
    }

    public NamedDataSource(String id, DataSourceManager manager, JsonObject config) {
        if (Objects.requireNonNull(id).isEmpty()) {
            throw new IllegalArgumentException("Non-empty datasource id required.");
        }

        Objects.requireNonNull(manager);

        this.id = id;
        this.aliases = new TreeSet<>();

        this.createDateTime = new Date();

        this.driverUrls = new TreeSet<>();
        this.digest = Utils.digest(config);

        this.customColumns = new ArrayList<ColumnDefinition>();

        int cacheSize = 100;
        int cacheExpireMinute = 5;

        if (config == null) {
            this.dateTime = false;
            this.timezone = null;
            this.queryTimeout = -1;
            this.writeTimeout = -1;
            this.sealed = false;
            this.defaultValues = new DefaultValues();
            this.queryParameters = new QueryParameters();
        } else {
            this.dateTime = config.getBoolean(CONF_DATETIME, false);
            String tz = config.getString(CONF_TIMEZONE);
            this.timezone = tz == null ? null : TimeZone.getTimeZone(tz);
            this.queryTimeout = config.getInteger(CONF_QUERY_TIMEOUT, -1);
            this.writeTimeout = config.getInteger(CONF_WRITE_TIMEOUT, -1);
            this.sealed = config.getBoolean(CONF_SEALED, false);

            JsonArray array = config.getJsonArray(CONF_ALIASES);
            if (array != null) {
                for (Object item : array) {
                    if ((item instanceof String) && !Utils.EMPTY_STRING.equals(item)) {
                        this.aliases.add((String) item);
                    }
                }

                this.aliases.remove(id);
            }
            array = config.getJsonArray(CONF_DRIVER_URLS);
            if (array != null) {
                for (Object item : array) {
                    if ((item instanceof String) && !Utils.EMPTY_STRING.equals(item)) {
                        this.driverUrls.add((String) item);
                    }
                }
            }

            JsonObject cacheConfig = config.getJsonObject(CONF_CACHE);
            if (cacheConfig != null) {
                for (Entry<String, Object> entry : cacheConfig) {
                    String cacheName = entry.getKey();
                    if (CONF_COLUMNS.equals(cacheName) && entry.getValue() instanceof JsonObject) {
                        JsonObject json = (JsonObject) entry.getValue();
                        cacheSize = json.getInteger(CONF_SIZE, cacheSize);
                        cacheExpireMinute = json.getInteger(CONF_EXPIRATION, cacheExpireMinute);
                        break;
                    }
                }
            }
            array = config.getJsonArray(CONF_COLUMNS);
            if (array != null) {
                for (Object obj : array) {
                    if (obj instanceof JsonObject) {
                        this.customColumns.add(ColumnDefinition.fromJson((JsonObject) obj));
                    }
                }
            }
            this.defaultValues = new DefaultValues(config.getJsonObject(CONF_DEFAULTS));
            this.queryParameters = new QueryParameters(config.getJsonObject(CONF_PARAMETERS));
        }

        this.driverClassLoader = USE_CUSTOM_DRIVER_LOADER
                ? new ExpandedUrlClassLoader(DEFAULT_DRIVER_CLASSLOADER,
                        this.driverUrls.toArray(new String[this.driverUrls.size()]))
                : null;

        this.columnsCache = Caffeine.newBuilder().maximumSize(cacheSize).recordStats()
                .expireAfterAccess(cacheExpireMinute, TimeUnit.MINUTES).build();
    }

    public String getCacheUsage() {
        CacheStats stats = this.columnsCache.stats();

        JsonObject obj = new JsonObject();

        obj.put(CACHE_STAT_HIT_COUNT, stats.hitCount());
        obj.put(CACHE_STAT_MISS_COUNT, stats.missCount());
        obj.put(CACHE_STAT_LOAD_SUCCESS_COUNT, stats.loadSuccessCount());
        obj.put(CACHE_STAT_LOAD_FAILURE_COUNT, stats.loadFailureCount());
        obj.put(CACHE_STAT_TOTAL_LOAD_TIME, stats.totalLoadTime());
        obj.put(CACHE_STAT_EVICTION_COUNT, stats.evictionCount());
        obj.put(CACHE_STAT_EVICTION_WEIGHT, stats.evictionWeight());

        return obj.toString();
    }

    public String getPoolUsage() {
        return EMPTY_USAGE;
    }

    public final String getId() {
        return this.id;
    }

    public final SortedSet<String> getAliases() {
        return Collections.unmodifiableSortedSet(this.aliases);
    }

    public final Date getCreateDateTime() {
        return this.createDateTime;
    }

    public final SortedSet<String> getDriverUrls() {
        return Collections.unmodifiableSortedSet(this.driverUrls);
    }

    public final ClassLoader getDriverClassLoader() {
        return this.driverClassLoader;
    }

    public final boolean useDateTime() {
        return this.dateTime;
    }

    public final TimeZone getTimeZone() {
        return this.timezone;
    }

    public final int getQueryTimeout() {
        return this.queryTimeout;
    }

    public final int getQueryTimeout(int customTimeout) {
        return !this.sealed && customTimeout >= 0 ? customTimeout : this.queryTimeout;
    }

    public final int getWriteTimeout() {
        return this.writeTimeout;
    }

    public final int getWriteTimeout(int customTimeout) {
        return !this.sealed && customTimeout >= 0 ? customTimeout : this.writeTimeout;
    }

    public final boolean isSealed() {
        return this.sealed;
    }

    public final String getParametersAsJsonString() {
        JsonObject obj = new JsonObject();

        obj.put(CONF_ID, this.getId());
        SortedSet<String> aliases = this.getAliases();
        if (aliases.size() > 1) {
            JsonArray array = new JsonArray();
            for (String a : aliases) {
                array.add(a);
            }
            obj.put(CONF_ALIASES, array);
        }

        SortedSet<String> driverUrls = this.getDriverUrls();
        if (driverUrls.size() > 1) {
            JsonArray array = new JsonArray();
            for (String a : driverUrls) {
                array.add(a);
            }
            obj.put(CONF_DRIVER_URLS, array);
        }

        obj.put(CONF_DATETIME, this.useDateTime());
        if (this.getTimeZone() != null) {
            obj.put(CONF_TIMEZONE, this.getTimeZone().getID());
        }

        int timeout = this.getQueryTimeout();
        if (timeout != -1) {
            obj.put(CONF_QUERY_TIMEOUT, timeout);
        }
        timeout = this.getWriteTimeout();
        if (timeout != -1) {
            obj.put(CONF_WRITE_TIMEOUT, timeout);
        }

        obj.put(CONF_SEALED, this.isSealed());
        obj.put(CONF_PARAMETERS, this.queryParameters.toJson());

        return obj.toString();
    }

    public final TableDefinition getResultColumns(String schema, String query, QueryParameters params) {
        log.debug("Inferring columns: schema=[{}], query=[{}]", schema, query);

        final TableDefinition columns;

        if (params.isDebug()) {
            columns = TableDefinition.DEBUG_COLUMNS;
        } else if (params.isMutation()) {
            columns = TableDefinition.MUTATION_COLUMNS;
        } else {
            try {
                columns = params.doNotUseCache()
                        ? inferTypes(schema, query, this.loadSavedQueryAsNeeded(query, params), params)
                        : columnsCache.get(query, k -> {
                            return inferTypes(schema, query, this.loadSavedQueryAsNeeded(k, params), params);
                        });
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Failed to infer schema from [" + this.getId() + "] due to: " + e.getMessage(), e);
            }
        }

        return columns;
    }

    public final boolean isDifferentFrom(JsonObject newConfig) {
        String newDigest = Utils.digest(newConfig == null ? null : newConfig.encode());
        boolean isDifferent = this.digest == null || this.digest.length() == 0 || !this.digest.equals(newDigest);
        if (isDifferent) {
            log.info("Datasource configuration of [{}] is changed from [{}] to [{}]", this.id, digest, newDigest);
        } else {
            log.info("Datasource configuration of [{}] remains the same", this.id);
        }

        return isDifferent;
    }

    public final List<ColumnDefinition> getCustomColumns() {
        return Collections.unmodifiableList(this.customColumns);
    }

    public final String getCustomColumnsAsJsonString() {
        JsonArray array = new JsonArray();

        for (ColumnDefinition col : this.customColumns) {
            array.add(col.toJson());
        }

        return array.toString();
    }

    public final DefaultValues getDefaultValues() {
        return this.defaultValues;
    }

    public final String getDefaultValuesAsJsonString() {
        return this.defaultValues.asJsonString();
    }

    public final QueryParameters newQueryParameters(QueryParameters paramsToMerge) {
        return new QueryParameters().merge(this.queryParameters).merge(paramsToMerge);
    }

    public final void executeQuery(String schema, NamedQuery query, TableDefinition requestColumns,
            QueryParameters params, ResponseWriter writer) {
        Objects.requireNonNull(query);
        Objects.requireNonNull(requestColumns);
        Objects.requireNonNull(params);

        List<ColumnDefinition> additionalColumns = new ArrayList<ColumnDefinition>();
        if (params.showDatasourceColumn()) {
            additionalColumns.add(new ColumnDefinition(TableDefinition.COLUMN_DATASOURCE, DataType.Str, true,
                    DEFAULT_LENGTH, DEFAULT_PRECISION, DEFAULT_SCALE, null, this.getId(), null));
        }
        if (params.showCustomColumns()) {
            additionalColumns.addAll(this.customColumns);
        }
        requestColumns.updateValues(additionalColumns);

        /*
         * DataColumnList allColumns = query.getColumns(params);
         * 
         * for (int i = additionalColumns.size(); i < requestColumns.size(); i++) {
         * DataColumn r = requestColumns.getColumn(i); for (int j = 0; j <
         * allColumns.size(); j++) { if
         * (r.getName().equals(allColumns.getColumn(j).getName())) { r.setIndex(j);
         * break; } } }
         */

        String originalQuery = query.getQuery();
        executeQuery(schema, originalQuery, loadSavedQueryAsNeeded(query.getQuery(), params), requestColumns, params,
                writer);
    }

    public final String loadSavedQueryAsNeeded(String normalizedQuery, QueryParameters params) {
        // in case the "normalizedQuery" is a local file...
        if (normalizedQuery.indexOf('\n') == -1 && isSavedQuery(normalizedQuery) && Utils.fileExists(normalizedQuery)) {
            normalizedQuery = Utils.loadTextFromFile(normalizedQuery);
        }

        return Utils.applyVariables(normalizedQuery, params == null ? null : params.asVariables());
    }

    @Override
    public void close() {
        log.info("Closing datasource[id={}, instance={}]", this.id, this);
    }

    public final void executeQuery(String schema, String originalQuery, String loadedQuery, TableDefinition columns,
            QueryParameters params, ResponseWriter writer) {
        log.info("Executing query(schema=[{}]):\n{}", schema, loadedQuery);

        if (params.isDebug()) {
            writeDebugResult(schema, originalQuery, loadedQuery, params, null, writer);
        } else {
            ColumnDefinition[] customColumns = this.customColumns
                    .toArray(new ColumnDefinition[this.customColumns.size()]);

            if (params.isMutation()) {
                writeMutationResult(schema, originalQuery, loadedQuery, params, columns.getColumns(), customColumns,
                        this.getDefaultValues(), writer);
            } else {
                writeQueryResult(schema, originalQuery, loadedQuery, params, columns.getColumns(), customColumns,
                        this.getDefaultValues(), writer);
            }
        }
    }

    public void executeMutation(String schema, String target, TableDefinition columns, QueryParameters parameters,
            ByteBuffer buffer) {
        log.info("Executing mutation: schema=[{}], target=[{}]", schema, target);
    }

    public boolean undertandsSQL() {
        return false;
    }

    public String getQuoteIdentifier() {
        return DEFAULT_QUOTE_IDENTIFIER;
    }

    public String getType() {
        return DATASOURCE_TYPE;
    }
}