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

import java.util.Date;
import java.util.Objects;

/**
 * This class defines run-time statistics of a datasource.
 * 
 * @since 2.0
 */
public class DataSourceStats implements UsageStats {
    private final String idOrAlias;
    private final int instance;
    private final boolean alias;
    private final Date createDateTime;
    private final String type;

    private final String defaults;
    private final String parameters;
    private final String customColumns;
    private final String cacheUsage;
    private final String poolUsage;

    public DataSourceStats(String id, NamedDataSource ds) {
        this.idOrAlias = Objects.requireNonNull(id);
        this.instance = ds.hashCode();
        this.alias = !this.idOrAlias.equals(Objects.requireNonNull(ds).getId());
        this.createDateTime = ds.getCreateDateTime();
        this.type = ds.getType();

        this.defaults = ds.getDefaultValuesAsJsonString();
        this.parameters = ds.getParametersAsJsonString();
        this.customColumns = ds.getCustomColumnsAsJsonString();
        this.cacheUsage = ds.getCacheUsage();
        this.poolUsage = ds.getPoolUsage();
    }

    public String getName() {
        return this.idOrAlias;
    }

    public int getInstance() {
        return this.instance;
    }

    public boolean isAlias() {
        return this.alias;
    }

    public Date getCreateDateTime() {
        return this.createDateTime;
    }

    public String getType() {
        return this.type;
    }

    public String getParameters() {
        return this.parameters;
    }

    public String getCustomColumns() {
        return this.customColumns;
    }

    public String getDefaults() {
        return this.defaults;
    }

    public String getCacheUsage() {
        return this.cacheUsage;
    }

    public String getPoolUsage() {
        return this.poolUsage;
    }
}
