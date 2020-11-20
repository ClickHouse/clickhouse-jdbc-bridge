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

import ru.yandex.clickhouse.jdbcbridge.core.ByteBuffer;
import ru.yandex.clickhouse.jdbcbridge.core.ColumnDefinition;
import ru.yandex.clickhouse.jdbcbridge.core.TableDefinition;
import ru.yandex.clickhouse.jdbcbridge.core.NamedDataSource;
import ru.yandex.clickhouse.jdbcbridge.core.DataType;
import ru.yandex.clickhouse.jdbcbridge.core.DefaultValues;
import ru.yandex.clickhouse.jdbcbridge.core.Extension;
import ru.yandex.clickhouse.jdbcbridge.core.ExtensionManager;
import ru.yandex.clickhouse.jdbcbridge.core.ResponseWriter;
import ru.yandex.clickhouse.jdbcbridge.core.Utils;
import ru.yandex.clickhouse.jdbcbridge.core.DataSourceStats;
import ru.yandex.clickhouse.jdbcbridge.core.DataTableReader;
import ru.yandex.clickhouse.jdbcbridge.core.DataSourceManager;
import ru.yandex.clickhouse.jdbcbridge.core.QueryParameters;

import static ru.yandex.clickhouse.jdbcbridge.core.DataType.*;

import java.util.Iterator;
import java.util.List;

/**
 * This class defines a new type of datasource, which can be used to retrieve
 * runtime metrics of all other datasources.
 * 
 * @since 2.0
 */
public class ConfigDataSource extends NamedDataSource {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ConfigDataSource.class);

    public static final String EXTENSION_NAME = "config";

    private static final String KWD_SHOW = "SHOW";
    private static final String KWD_DATASOURCES = "DATASOURCES";

    private static final String COL_NAME = "name";
    private static final String COL_IS_ALIAS = "is_alias";
    private static final String COL_INSTANCE = "instance";
    private static final String COL_CREATE_DATETIME = "create_datetime";
    private static final String COL_TYPE = CONF_TYPE;
    private static final String COL_PARAMETERS = "parameters";
    private static final String COL_DEFAULTS = "defaults";
    private static final String COL_CUSTOM_COLUMNS = "custom_columns";
    private static final String COL_CACHE_USAGE = "cache_usage";
    private static final String COL_POOL_USAGE = "pool_usage";

    private static final TableDefinition DATASOURCE_CONFIG_COLUMNS = new TableDefinition(
            new ColumnDefinition(COL_NAME, DataType.Str, true, DEFAULT_LENGTH, DEFAULT_PRECISION, DEFAULT_SCALE),
            new ColumnDefinition(COL_IS_ALIAS, DataType.UInt8, true, DEFAULT_LENGTH, DEFAULT_PRECISION, DEFAULT_SCALE),
            new ColumnDefinition(COL_INSTANCE, DataType.Int32, true, DEFAULT_LENGTH, DEFAULT_PRECISION, DEFAULT_SCALE),
            new ColumnDefinition(COL_CREATE_DATETIME, DataType.DateTime, true, DEFAULT_LENGTH, DEFAULT_PRECISION,
                    DEFAULT_SCALE),
            new ColumnDefinition(COL_TYPE, DataType.Str, true, DEFAULT_LENGTH, DEFAULT_PRECISION, DEFAULT_SCALE),
            new ColumnDefinition(COL_PARAMETERS, DataType.Str, true, DEFAULT_LENGTH, DEFAULT_PRECISION, DEFAULT_SCALE),
            new ColumnDefinition(COL_DEFAULTS, DataType.Str, true, DEFAULT_LENGTH, DEFAULT_PRECISION, DEFAULT_SCALE),
            new ColumnDefinition(COL_CUSTOM_COLUMNS, DataType.Str, true, DEFAULT_LENGTH, DEFAULT_PRECISION,
                    DEFAULT_SCALE),
            new ColumnDefinition(COL_CACHE_USAGE, DataType.Str, true, DEFAULT_LENGTH, DEFAULT_PRECISION, DEFAULT_SCALE),
            new ColumnDefinition(COL_POOL_USAGE, DataType.Str, true, DEFAULT_LENGTH, DEFAULT_PRECISION, DEFAULT_SCALE));

    protected static class ConfigQuery {
        String configType;
        String queryType;
    }

    static class DataSourceStatReader implements DataTableReader {
        private final Iterator<DataSourceStats> stats;

        private DataSourceStats current = null;

        protected DataSourceStatReader(Iterator<DataSourceStats> stats) {
            this.stats = stats;
        }

        @Override
        public boolean nextRow() {
            boolean hasNext = false;

            while (stats.hasNext()) {
                current = stats.next();
                if (current.getName().isEmpty()) { // skip this special datasource)
                    continue;
                } else {
                    hasNext = true;
                    break;
                }
            }

            return hasNext;
        }

        @Override
        public boolean isNull(int row, int column, ColumnDefinition metadata) {
            return false;
        }

        @Override
        public void read(int row, int column, ColumnDefinition metadata, ByteBuffer buffer) {
            switch (metadata.getName()) {
                case COL_NAME:
                    buffer.writeString(current.getName());
                    break;
                case COL_IS_ALIAS:
                    buffer.writeBoolean(current.isAlias());
                    break;
                case COL_INSTANCE:
                    buffer.writeInt32(current.getInstance());
                    break;
                case COL_CREATE_DATETIME:
                    buffer.writeDateTime(current.getCreateDateTime());
                    break;
                case COL_TYPE:
                    buffer.writeString(current.getType());
                    break;
                case COL_CUSTOM_COLUMNS:
                    buffer.writeString(current.getCustomColumns());
                    break;
                case COL_DEFAULTS:
                    buffer.writeString(current.getDefaults());
                    break;
                case COL_PARAMETERS:
                    buffer.writeString(current.getParameters());
                    break;
                case COL_CACHE_USAGE:
                    buffer.writeString(current.getCacheUsage());
                    break;
                case COL_POOL_USAGE:
                    buffer.writeString(current.getPoolUsage());
                    break;
                default:
                    break;
            }
        }
    }

    public static void initialize(ExtensionManager manager) {
        DataSourceManager dsManager = manager.getDataSourceManager();

        Extension<NamedDataSource> thisExtension = manager.getExtension(ConfigDataSource.class);
        dsManager.registerType(EXTENSION_NAME, thisExtension);
        dsManager.put(Utils.EMPTY_STRING, new ConfigDataSource(dsManager));
    }

    private final DataSourceManager dataSourceManager;

    protected ConfigQuery parse(String query) {
        ConfigQuery cq = new ConfigQuery();

        if (query != null) {
            // FIXME what about \t, \r and \n
            List<String> parsedQuery = Utils.splitByChar(query, ' ', true);

            int parts = parsedQuery.size();
            if (parts == 2 && KWD_SHOW.equalsIgnoreCase(cq.queryType = parsedQuery.get(0))) {

                if (KWD_DATASOURCES.equalsIgnoreCase(parsedQuery.get(1))) {
                    cq.configType = KWD_DATASOURCES;
                }
            }
        }

        if (cq.configType == null) {
            throw new IllegalArgumentException("Invalid query [" + query + "], try SHOW DATASOURCES");
        }

        return cq;
    }

    protected ConfigDataSource(DataSourceManager dataSourceManager) {
        super(EXTENSION_NAME, dataSourceManager, null);

        this.dataSourceManager = dataSourceManager;
    }

    @Override
    protected void writeQueryResult(String schema, String originalQuery, String loadedQuery, QueryParameters params,
            ColumnDefinition[] requestColumns, ColumnDefinition[] customColumns, DefaultValues defaultValues,
            ResponseWriter writer) {
        ConfigQuery cq = parse(loadedQuery);

        if (cq.configType != KWD_DATASOURCES) {
            return;
        }

        new DataSourceStatReader(dataSourceManager.getDataSourceStats().iterator()).process(getId(), requestColumns,
                customColumns, DATASOURCE_CONFIG_COLUMNS.getColumns(), defaultValues, getTimeZone(), params, writer);
    }

    @Override
    public String getType() {
        return EXTENSION_NAME;
    }

    @Override
    protected TableDefinition inferTypes(String schema, String originalQuery, String loadedQuery,
            QueryParameters params) {
        parse(loadedQuery);

        return DATASOURCE_CONFIG_COLUMNS;
    }
}