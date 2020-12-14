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
 * parameters.
 * 
 * @since 2.0
 */
public class NamedQuery extends NamedSchema {
    private static final String CONF_QUERY = "query";
    private static final String CONF_PARAMETERS = "parameters";

    private final String query;

    private final QueryParameters parameters;

    public NamedQuery(String id, Repository<NamedSchema> repo, JsonObject config) {
        super(id, repo, config);

        String namedQuery = config.getString(CONF_QUERY);
        Objects.requireNonNull(namedQuery);

        this.query = namedQuery;
        this.parameters = new QueryParameters(config.getJsonObject(CONF_PARAMETERS));
    }

    public String getQuery() {
        return this.query;
    }

    public TableDefinition getColumns(QueryParameters params) {
        return this.getColumns();
    }

    public QueryParameters getParameters() {
        return this.parameters;
    }
}