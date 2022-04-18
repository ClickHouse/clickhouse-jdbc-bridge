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

import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * This class defines a entity which can be initialized using JSON format
 * configuration.
 * 
 * @since 2.0
 */
public abstract class ManagedEntity {
    protected static final String CONF_ID = "id";
    protected static final String CONF_ALIASES = "aliases";
    protected static final String CONF_TYPE = "type";

    protected final Set<String> aliases;
    protected final Date createDateTime;
    protected final String digest;
    protected final String id;
    protected final String type;

    /**
     * Constructor of configurable entity.
     * 
     * @param id     id of the entity
     * @param config configuration in JSON format, {@code id}, {@code type} and
     *               {@code aliases} properties are reserved for instantiation
     */
    protected ManagedEntity(String id, JsonObject config) {
        this.aliases = new LinkedHashSet<>();
        this.createDateTime = new Date();
        this.digest = Utils.digest(config);
        this.id = id == null && config != null ? config.getString(CONF_ID) : id;

        String defaultType = getClass().getSimpleName();
        if (config != null) {
            this.type = config.getString(CONF_TYPE, defaultType);
            JsonArray array = config.getJsonArray(CONF_ALIASES);
            if (array != null) {
                for (Object item : array) {
                    if ((item instanceof String) && !Utils.EMPTY_STRING.equals(item)) {
                        this.aliases.add((String) item);
                    }
                }

                this.aliases.remove(id);
            }
        } else {
            this.type = defaultType;
        }
    }

    /**
     * Validate this instance and throw runtime exception if something wrong.
     */
    public void validate() {
    }

    /**
     * Get id of the entity.
     * 
     * @return id of the entity
     */
    public final String getId() {
        return id;
    }

    /**
     * Get list of aliases of the entity.
     * 
     * @return aliases of the entity
     */
    public final Set<String> getAliases() {
        return Collections.unmodifiableSet(this.aliases);
    }

    /**
     * Get creation datetime of the entity.
     * 
     * @return creation datetime of the entity
     */
    public final Date getCreationDateTime() {
        return this.createDateTime;
    }

    /**
     * Get type of the entity.
     * 
     * @return type of the entity
     */
    public String getType() {
        return Objects.requireNonNull(type);
    }

    /**
     * Check if given configuration is different from current or not.
     * 
     * @param config configuration in JSON format
     * @return true if the given configuration is different from current; false
     *         otherwise
     */
    public final boolean isDifferentFrom(JsonObject config) {
        return this.digest == null || this.digest.isEmpty() || !this.digest.equals(Utils.digest(config));
    }

    public abstract UsageStats getUsage(String idOrAlias);
}
