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
 * This class defines a named query, which is composed of query, schema and
 * query parameter.
 * 
 * @since 2.0
 */
public class NamedQuery {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(NamedQuery.class);

    private static final String CONF_QUERY = "query";
    private static final String CONF_COLUMNS = "columns";
    private static final String CONF_PARAMETERS = "parameters";

    private final String id;
    private final String digest;
    private final String query;
    private final TableDefinition columns;

    private final QueryParameters parameters;

    public NamedQuery(String id, JsonObject config) {
        Objects.requireNonNull(config);

        this.id = id;
        this.digest = Utils.digest(config);

        String namedQuery = config.getString(CONF_QUERY);
        Objects.requireNonNull(namedQuery);

        this.query = namedQuery;
        this.columns = TableDefinition.fromJson(config.getJsonArray(CONF_COLUMNS));
        this.parameters = new QueryParameters(config.getJsonObject(CONF_PARAMETERS));
    }

    public String getId() {
        return this.id;
    }

    public String getQuery() {
        return this.query;
    }

    public boolean hasColumn() {
        return this.columns != null && this.columns.hasColumn();
    }

    public TableDefinition getColumns(QueryParameters params) {
        return this.columns;
    }

    public QueryParameters getParameters() {
        return this.parameters;
    }

    public final boolean isDifferentFrom(JsonObject newConfig) {
        String newDigest = Utils.digest(newConfig == null ? null : newConfig.encode());
        boolean isDifferent = this.digest == null || this.digest.length() == 0 || !this.digest.equals(newDigest);
        if (isDifferent) {
            log.info("Query configuration of [{}] is changed from [{}] to [{}]", this.id, digest, newDigest);
        } else {
            log.debug("Query configuration of [{}] remains the same", this.id);
        }

        return isDifferent;
    }
}