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

import io.vertx.core.json.JsonObject;

/**
 * This interface defines the ability to load configuration automatically when
 * there's change detected.
 * 
 * @since 2.0
 */
public interface Reloadable {
    /**
     * Reload configuration. This will be called for first-time loading and reload
     * when there's changes.
     * 
     * @param config configuration in JSON format
     */
    default void reload(JsonObject config) {
    }
}
