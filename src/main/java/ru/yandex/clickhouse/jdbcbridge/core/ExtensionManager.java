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

import java.util.Map;
import java.util.function.Consumer;

import io.vertx.core.json.JsonObject;

/**
 * This interface defines extension manager, which manages can be used get/set
 * managers for datasource, query and schema, as well as how to reload
 * configuration files on disk after change.
 * 
 * @since 2.0
 */
public interface ExtensionManager {
    <T> Extension<T> getExtension(Class<? extends T> clazz);

    DataSourceManager getDataSourceManager();

    void setDataSourceManager(DataSourceManager manager);

    QueryManager getQueryManager();

    void setQueryManager(QueryManager manager);

    SchemaManager getSchemaManager();

    void setSchemaManager(SchemaManager manager);

    /**
     * Register a consumer to load configuration files(in JSON format) based on
     * given path, regardless it's a directory or a file. And it will be called
     * again later for reloading, when there's change detected on disk.
     * 
     * @param configPath path to monitor, in general a relative path under
     *                   configuration path
     * @param consumer   consumer to handle loaded configuration, regardless it's
     *                   new or changed
     */
    void registerConfigLoader(String configPath, Consumer<JsonObject> consumer);

    /**
     * Get list of named scriptable objects.
     * 
     * @return named scriptable objects
     */
    Map<String, Object> getScriptableObjects();
}
