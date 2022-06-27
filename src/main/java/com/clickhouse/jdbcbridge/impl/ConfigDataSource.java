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

import static com.clickhouse.jdbcbridge.core.DataType.*;

import java.util.Iterator;
import java.util.List;

import com.clickhouse.jdbcbridge.core.ByteBuffer;
import com.clickhouse.jdbcbridge.core.ColumnDefinition;
import com.clickhouse.jdbcbridge.core.DataSourceStats;
import com.clickhouse.jdbcbridge.core.DataTableReader;
import com.clickhouse.jdbcbridge.core.DataType;
import com.clickhouse.jdbcbridge.core.DefaultValues;
import com.clickhouse.jdbcbridge.core.ExtensionManager;
import com.clickhouse.jdbcbridge.core.NamedDataSource;
import com.clickhouse.jdbcbridge.core.QueryParameters;
import com.clickhouse.jdbcbridge.core.Repository;
import com.clickhouse.jdbcbridge.core.ResponseWriter;
import com.clickhouse.jdbcbridge.core.TableDefinition;
import com.clickhouse.jdbcbridge.core.UsageStats;
import com.clickhouse.jdbcbridge.core.Utils;

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
        private final Iterator<UsageStats> stats;

        private DataSourceStats current = null;

        protected DataSourceStatReader(List<UsageStats> stats) {
            this.stats = stats.iterator();
        }

        @Override
        public boolean nextRow() {
            boolean hasNext = false;

            while (stats.hasNext()) {
                UsageStats usage = stats.next();

                // skip non-supported statistics and ConfigDataSource
                if (usage instanceof DataSourceStats && !(current = (DataSourceStats) usage).getName().isEmpty()) {
                    hasNext = true;
                    break;
                } else {
                    log.warn("Discard unsupported usage statistics: {}", usage);
                    continue;
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
        Repository<NamedDataSource> dsRepo = manager.getRepositoryManager().getRepository(NamedDataSource.class);

        dsRepo.put(Utils.EMPTY_STRING, new ConfigDataSource(dsRepo));
    }

    private final Repository<NamedDataSource> dataSourceRepo;

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

    protected ConfigDataSource(Repository<NamedDataSource> dataSourceRepo) {
        super(EXTENSION_NAME, dataSourceRepo, null);

        this.dataSourceRepo = dataSourceRepo;
    }

    @Override
    protected void writeQueryResult(String schema, String originalQuery, String loadedQuery, QueryParameters params,
            ColumnDefinition[] requestColumns, ColumnDefinition[] customColumns, DefaultValues defaultValues,
            ResponseWriter writer) {
        ConfigQuery cq = parse(loadedQuery);

        if (cq.configType != KWD_DATASOURCES) {
            return;
        }

        new DataSourceStatReader(dataSourceRepo.getUsageStats()).process(getId(), requestColumns, customColumns,
                DATASOURCE_CONFIG_COLUMNS.getColumns(), defaultValues, getTimeZone(), params, writer);
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