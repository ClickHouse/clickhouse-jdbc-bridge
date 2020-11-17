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

/**
 * This interface defines how to manage named schemas.
 * 
 * @since 2.0
 */
public interface SchemaManager extends Reloadable {
    /**
     * Get named schema.
     * 
     * @param name name of the schema
     * @return desired schema
     */
    NamedSchema get(String name);
}
