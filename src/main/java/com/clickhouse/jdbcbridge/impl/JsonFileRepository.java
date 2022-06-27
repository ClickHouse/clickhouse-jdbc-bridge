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

import java.util.HashSet;
import java.util.Objects;
import java.util.Map.Entry;

import com.clickhouse.jdbcbridge.core.BaseRepository;
import com.clickhouse.jdbcbridge.core.ExtensionManager;
import com.clickhouse.jdbcbridge.core.ManagedEntity;
import com.clickhouse.jdbcbridge.core.NamedDataSource;
import com.clickhouse.jdbcbridge.core.NamedQuery;
import com.clickhouse.jdbcbridge.core.NamedSchema;
import com.clickhouse.jdbcbridge.core.Reloadable;
import com.clickhouse.jdbcbridge.core.Utils;

import io.vertx.core.json.JsonObject;

public class JsonFileRepository<T extends ManagedEntity> extends BaseRepository<T> implements Reloadable {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(JsonFileRepository.class);

    @SuppressWarnings("unchecked")
    public static <T extends ManagedEntity> JsonFileRepository<T> newInstance(Object... args) {
        if (Objects.requireNonNull(args).length < 2) {
            throw new IllegalArgumentException(
                    "In order to create a JSON file repository, you need to specify at least ExtensionManager and entity class.");
        }

        ExtensionManager manager = (ExtensionManager) Objects.requireNonNull(args[0]);
        Class<T> entityClass = (Class<T>) Objects.requireNonNull(args[1]);

        JsonFileRepository<T> repo = new JsonFileRepository<>(entityClass);
        String defaultDir = entityClass.getSimpleName().toLowerCase();
        String defaultEnv = entityClass.getSimpleName().toUpperCase() + "_CONFIG_DIR";
        String defaultProp = "jdbc-bridge." + defaultDir + ".config.dir";
        if (NamedDataSource.class.equals(entityClass)) {
            defaultDir = "datasources";
            defaultEnv = "DATASOURCE_CONFIG_DIR";
            defaultProp = "jdbc-bridge.datasource.config.dir";
        } else if (NamedSchema.class.equals(entityClass)) {
            defaultDir = "schemas";
            defaultEnv = "SCHEMA_CONFIG_DIR";
            defaultProp = "jdbc-bridge.schema.config.dir";
        } else if (NamedQuery.class.equals(entityClass)) {
            defaultDir = "queries";
            defaultEnv = "QUERY_CONFIG_DIR";
            defaultProp = "jdbc-bridge.query.config.dir";
        }

        manager.registerConfigLoader(Utils.getConfiguration(defaultDir, defaultEnv, defaultProp), repo::reload);

        return repo;
    }

    public JsonFileRepository(Class<T> clazz) {
        super(clazz);
    }

    @Override
    public void reload(JsonObject config) {
        if (config == null || config.fieldNames().size() == 0) {
            log.info("No {} configuration found", getEntityName());

            HashSet<String> keys = new HashSet<>();
            for (String key : mappings.keySet()) {
                keys.add(key);
            }

            for (String key : keys) {
                remove(key);
            }
            // mappings.clear();
        } else {
            log.info("Loading {} configuration...", getEntityName());
            HashSet<String> keys = new HashSet<>();
            for (Entry<String, Object> entry : config) {
                String key = entry.getKey();
                Object value = entry.getValue();
                if (key != null && value instanceof JsonObject) {
                    keys.add(key);
                    update(key, (JsonObject) value);
                }
            }

            HashSet<String> entityIds = new HashSet<>();
            mappings.entrySet().forEach(entry -> {
                String id = entry.getKey();
                T entity = entry.getValue();
                if (id != null && !id.isEmpty() && entity != null && id.equals(entity.getId())) {
                    entityIds.add(id);
                }
            });

            for (String id : entityIds) {
                if (!keys.contains(id)) {
                    remove(id);
                }
            }
        }
    }
}
