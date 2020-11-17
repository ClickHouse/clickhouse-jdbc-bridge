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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import ru.yandex.clickhouse.jdbcbridge.core.ExtensionManager;
import ru.yandex.clickhouse.jdbcbridge.core.NamedQuery;
import ru.yandex.clickhouse.jdbcbridge.core.QueryManager;
import ru.yandex.clickhouse.jdbcbridge.core.Utils;
import io.vertx.core.json.JsonObject;

/**
 * This class is default implementation of QueryManager. Basically, it loads
 * named queries from JSON files under <work directory>/config/queries, and then
 * later used to retrieve named query by its name.
 * 
 * @since 2.0
 */
public class DefaultQueryManager implements QueryManager {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(DefaultQueryManager.class);

    private final Map<String, NamedQuery> mappings = Collections.synchronizedMap(new HashMap<>());

    public static void initialize(ExtensionManager manager) {
        QueryManager qManager = manager.getQueryManager();
        if (qManager == null || !(qManager instanceof DefaultQueryManager)) {
            manager.setQueryManager(qManager = new DefaultQueryManager());
        }

        manager.registerConfigLoader(
                Utils.getConfiguration("queries", "QUERY_CONFIG_DIR", "jdbc-bridge.query.config.dir"),
                qManager::reload);
    }

    protected void update(String id, JsonObject config) {
        NamedQuery query = mappings.get(id);

        boolean addQuery = false;
        if (query == null) {
            addQuery = true;
        } else if (query.isDifferentFrom(config)) {
            mappings.remove(id);
            addQuery = true;
        }

        if (addQuery && config != null) {
            log.info("Adding query [{}]...", id);
            try {
                mappings.put(id, new NamedQuery(id, config));
            } catch (Exception e) {
                log.error("Failed to add query", e);
            }
        }
    }

    @Override
    public void reload(JsonObject config) {
        if (config == null || config.fieldNames().size() == 0) {
            log.info("No query configuration found");
            mappings.clear();
        } else {
            HashSet<String> keys = new HashSet<>();
            config.forEach(action -> {
                String id = action.getKey();
                if (id != null) {
                    keys.add(id);
                    update(id, action.getValue() instanceof JsonObject ? (JsonObject) action.getValue() : null);
                }
            });

            mappings.entrySet().removeIf(entry -> {
                boolean shouldRemove = !keys.contains(entry.getKey());

                if (shouldRemove) {
                    log.info("Removing query [{}]...", entry.getKey());
                }

                return shouldRemove;
            });
        }
    }

    @Override
    public NamedQuery get(String query) {
        return mappings.get(query);
    }
}