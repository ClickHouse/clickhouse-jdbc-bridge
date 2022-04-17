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

import java.util.List;

/**
 * This interface defines a repository for managing {@link ManagedEntity} like
 * {@link NamedDataSource}, {@link NamedSchema}, and {@link NamedQuery}.
 * 
 * @since 2.0
 */
public interface Repository<T extends ManagedEntity> {
    /**
     * Get class of managed entities.
     * 
     * @return class of managed entities
     */
    Class<T> getEntityClass();

    /**
     * Check if the given type of entity can be managed by this repository.
     * 
     * @param clazz class of the entity
     * @return true if the type of entity can be managed by this repository; false
     *         otherwise
     */
    boolean accept(Class<?> clazz);

    /**
     * Resolve given name. Usually just about DNS SRV record resolving, for example:
     * {@code jdbc:clickhouse:{{ ch-server.somedomain }}/system} will be resolved to
     * something like {@code jdbc:clickhouse:127.0.0.1:8123/system}.
     * 
     * @param name name to resolve
     * @return resolved name
     */
    String resolve(String name);

    /**
     * Get usage statistics of the repository.
     * 
     * @return usage statistics
     */
    List<UsageStats> getUsageStats();

    /**
     * Register new type of entity to be manged in this repository.
     * 
     * @param type      type of entity, defaults to extension class name
     * @param extension extension for instantiate new type of entity
     */
    void registerType(String type, Extension<T> extension);

    /**
     * Put a named entity into the repository.
     * 
     * @param id     id of the entity, could be null
     * @param entity non-null entity to be added
     */
    void put(String id, T entity);

    /**
     * Get entity from repository by id.
     * 
     * @param id id of the entity
     * @return desired entity
     */
    T get(String id);

    /**
     * Get or create entity from repository by id.
     * 
     * @param id id of the entity
     * @return desired entity
     */
    default T getOrCreate(String id) {
        T entity = get(id);

        if (entity == null) {
            throw new UnsupportedOperationException("Creating entity is not supported");
        }

        return entity;
    }
}
