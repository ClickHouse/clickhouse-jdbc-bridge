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

import java.util.Objects;

import io.vertx.core.json.JsonObject;

/**
 * This class defines a named schema.
 * 
 * @since 2.0
 */
public class NamedSchema extends ManagedEntity {
    protected static final String CONF_COLUMNS = "columns";

    private final TableDefinition columns;

    public NamedSchema(String id, Repository<NamedSchema> repo, JsonObject config) {
        super(id, Objects.requireNonNull(config));

        this.columns = TableDefinition.fromJson(config.getJsonArray(CONF_COLUMNS));
    }

    public boolean hasColumn() {
        return this.columns != null && this.columns.hasColumn();
    }

    public TableDefinition getColumns() {
        return this.columns;
    }

    @Override
    public UsageStats getUsage(String idOrAlias) {
        return null;
    }
}