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

import static com.clickhouse.jdbcbridge.core.Utils.EMPTY_STRING;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Map.Entry;

import io.vertx.core.json.JsonObject;

/**
 * Base class for implementing a repository managing entities by ID and
 * optionally (registered) entity type.
 * 
 * @since 2.0
 */
public abstract class BaseRepository<T extends ManagedEntity> implements Repository<T> {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(BaseRepository.class);

    protected final Map<String, Extension<T>> types = Collections.synchronizedMap(new LinkedHashMap<>());
    protected final Map<String, T> mappings = Collections.synchronizedMap(new HashMap<>());

    private final Class<T> clazz;
    private final String name;
    private final DnsResolver resolver;

    private String defaultType = null;

    protected final Extension<T> defaultExtension;

    protected String getEntityName() {
        return this.name;
    }

    protected Extension<T> getExtensionByType(String type, boolean autoCreate) {
        Extension<T> extension = types.size() > 0 ? types.get(type) : null;

        if (extension == null) {
            if (autoCreate) {
                extension = defaultExtension;
            } else {
                throw new IllegalArgumentException("Unsupported type of " + getEntityName() + ": " + type);
            }
        }

        return extension;
    }

    /**
     * Create entity of given type.
     * 
     * @param id   id of the entity
     * @param type type of the entity
     * @return non-null entity
     */
    protected T createFromType(String id, String type) {
        Extension<T> extension = getExtensionByType(type, false);
        return extension == null ? null : extension.newInstance(id, this, null);
    }

    /**
     * Create entity based on {@code type} defined given configuration.
     * 
     * @param id     id of the entity
     * @param config configuration in JSON format
     * @return non-null entity
     */
    protected T createFromConfig(String id, JsonObject config) {
        String type = config == null ? null : config.getString(ManagedEntity.CONF_TYPE);
        if (type == null) {
            type = defaultType;
        }
        Extension<T> extension = getExtensionByType(type, true);

        return extension.newInstance(id, this, config);
    }

    /**
     * Atomic add operation. For example, save the entity into a database table.
     * 
     * @param entity entity
     */
    protected void atomicAdd(T entity) {
    }

    /**
     * Atomic remove operation. For example, delete the entity from a database
     * table.
     * 
     * @param entity entity
     */
    protected void atomicRemove(T entity) {
    }

    /**
     * Remove an entity and all its aliases(when {@code id} is not an alias) based
     * on given id.
     * 
     * @param id id of the entity, could be an alias
     */
    protected void remove(String id) {
        // empty id is reserved for ConfigDataSource
        T entity = EMPTY_STRING.equals(id) ? null : mappings.remove(id);

        if (entity == null) {
            return;
        }

        final List<T> removedEntities;
        if (id != null && id.equals(entity.getId())) {
            log.info("Removing {}(id={}) and all its aliases...", getEntityName(), id);

            Set<String> aliases = entity.getAliases();
            removedEntities = new ArrayList<>(aliases.size());
            for (String alias : entity.getAliases()) {
                T ref = mappings.get(alias);
                // we don't want to remove an entity when its id is same as an
                // alias of another entity
                if (ref != null && !alias.equals(ref.getId())) {
                    log.info("Removing alias [{}] of {}(id={})...", alias, getEntityName(), ref.getId());
                    T removedEntity = mappings.remove(alias);
                    if (removedEntity != entity) {
                        removedEntities.add(removedEntity);
                    }
                }
            }

            if (entity instanceof Closeable) {
                if (log.isDebugEnabled()) {
                    log.debug("Closing {}(id={})...", getEntityName(), id);
                }
                // TODO async close in case it's too slow?
                try {
                    ((Closeable) entity).close();
                } catch (Exception e) {
                }
            }

            atomicRemove(entity);
        } else { // just an alias
            log.info("Removing alias [{}] of {}(id={})...", id, getEntityName(), entity.getId());
            removedEntities = Collections.singletonList(entity);
        }

        // close remove entries as needed
        for (T e : removedEntities) {
            if (!(e instanceof Closeable)) {
                continue;
            }

            boolean matched = false;
            for (T v : mappings.values()) {
                if (e == v) {
                    matched = true;
                    break;
                }
            }

            if (matched) {
                if (log.isDebugEnabled()) {
                    log.debug("Closing {}(id={})...", getEntityName(), e.getId());
                }
                try {
                    ((Closeable) e).close();
                } catch (Exception exp) {
                }
            }
        }
    }

    protected void update(String id, JsonObject config) {
        T entity = mappings.get(id);

        boolean addEntity = entity == null;

        if (!addEntity) {
            if (entity.isDifferentFrom(config)) {
                remove(id);
                addEntity = true;
            }
        }

        if (addEntity && config != null) {
            log.info("Adding {}(id={})...", getEntityName(), id);

            try {
                entity = createFromConfig(id, config);
                mappings.put(id, entity);

                for (String alias : entity.getAliases()) {
                    if (mappings.containsKey(alias)) {
                        log.warn("Not able to add {} alias [{}] as it exists already", getEntityName(), alias);
                    } else {
                        mappings.put(alias, entity);
                    }
                }

                atomicAdd(entity);
            } catch (RuntimeException e) {
                log.warn("Failed to add " + getEntityName() + "(id=" + id + ")", e);
            }
        }
    }

    public BaseRepository(Class<T> clazz) {
        this.clazz = Objects.requireNonNull(clazz);
        this.name = clazz.getSimpleName();
        this.resolver = new DnsResolver();

        this.defaultExtension = new Extension<>(clazz);
    }

    @Override
    public Class<T> getEntityClass() {
        return this.clazz;
    }

    @Override
    public boolean accept(Class<?> clazz) {
        return clazz != null && clazz.isAssignableFrom(this.clazz);
    }

    @Override
    public String resolve(String name) {
        return Utils.applyVariables(name, resolver::apply);
    }

    @Override
    public List<UsageStats> getUsageStats() {
        List<UsageStats> list = new ArrayList<>(this.mappings.size());
        for (Entry<String, T> entry : mappings.entrySet()) {
            UsageStats usage = entry.getValue().getUsage(entry.getKey());
            if (usage != null) {
                list.add(usage);
            }
        }

        return Collections.unmodifiableList(list);
    }

    @Override
    public void registerType(String type, Extension<T> extension) {
        String className = Objects.requireNonNull(extension).getProviderClass().getName();
        type = type == null || type.isEmpty() ? extension.getName() : type;

        Extension<T> registered = this.types.put(type, extension);

        if (registered != null) {
            log.warn("Discard [{}] as type [{}] is reserved by [{}]", className, type, registered.getClass().getName());
            return;
        }

        log.info("Registering new type of {}: [{}] -> [{}]", getEntityName(), type, className);

        if (types.size() == 1) {
            log.info("Default type of {} is set to [{}]", getEntityName(), defaultType = type);
        }
    }

    @Override
    public void put(String id, T entity) {
        Objects.requireNonNull(entity);

        if (id == null) {
            id = entity.getId();
        }

        this.remove(id);
        this.mappings.put(id, entity);
        atomicAdd(entity);

        // now update aliases...
        for (String alias : entity.getAliases()) {
            if (alias == null || alias.isEmpty()) {
                continue;
            }

            T e = this.mappings.get(alias);
            if (e != null && alias.equals(e.getId())) {
                log.warn("Not going to add alias [{}] as it's an ID reserved by another {}", alias, getEntityName());
                continue;
            } else {
                this.remove(alias);
            }

            this.mappings.put(alias, entity);
        }
    }

    @Override
    public T get(final String id) {
        T entity = null;

        String normalizedId = id == null ? EMPTY_STRING : id;
        // [<type>:]<id>[?<query parameters>], for example: "my-db" and "jdbc:my-db"
        // connection string like "jdbc:clickhouse:..."
        if (this.types.size() > 0) { // multi-type repository
            // check if any type is declared first
            int index = normalizedId.indexOf(':');
            if (index >= 0) {
                return createFromType(normalizedId, normalizedId.substring(0, index));
            } else {
                if ((index = normalizedId.indexOf('?')) >= 0) {
                    normalizedId = normalizedId.substring(0, index);
                }

                if ((entity = this.mappings.get(normalizedId)) == null) {
                    throw new IllegalArgumentException(getEntityName() + " [" + normalizedId + "] does not exist!");
                }
            }
        } else {
            entity = this.mappings.get(id);
        }

        return entity;
    }
}
