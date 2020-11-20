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
import ru.yandex.clickhouse.jdbcbridge.core.NamedSchema;
import ru.yandex.clickhouse.jdbcbridge.core.SchemaManager;
import ru.yandex.clickhouse.jdbcbridge.core.Utils;
import io.vertx.core.json.JsonObject;

/**
 * This class is default implementation of
 * {@link ru.yandex.clickhouse.jdbcbridge.core.QueryManager}. Basically, it
 * loads named schemas from JSON files under <work directory>/config/schemas,
 * and then later used to retrieve named schema by its name.
 * 
 * @since 2.0
 */
public class DefaultSchemaManager implements SchemaManager {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(DefaultQueryManager.class);

    private final Map<String, NamedSchema> mappings = Collections.synchronizedMap(new HashMap<>());

    public static void initialize(ExtensionManager manager) {
        SchemaManager sManager = manager.getSchemaManager();
        if (sManager == null || !(sManager instanceof DefaultSchemaManager)) {
            manager.setSchemaManager(sManager = new DefaultSchemaManager());
        }

        manager.registerConfigLoader(
                Utils.getConfiguration("schemas", "SCHEMA_CONFIG_DIR", "jdbc-bridge.schema.config.dir"),
                sManager::reload);
    }

    protected void update(String id, JsonObject config) {
        NamedSchema schema = mappings.get(id);

        boolean addSchema = false;
        if (schema == null) {
            addSchema = true;
        } else if (schema.isDifferentFrom(config)) {
            mappings.remove(id);
            addSchema = true;
        }

        if (addSchema && config != null) {
            log.info("Adding schema [{}]...", id);
            try {
                mappings.put(id, new NamedSchema(id, config));
            } catch (Exception e) {
                log.error("Failed to add schema", e);
            }
        }
    }

    @Override
    public void reload(JsonObject config) {
        if (config == null || config.fieldNames().size() == 0) {
            log.info("No schema configuration found");
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
                    log.info("Removing schema [{}]...", entry.getKey());
                }

                return shouldRemove;
            });
        }
    }

    @Override
    public NamedSchema get(String query) {
        return mappings.get(query);
    }
}