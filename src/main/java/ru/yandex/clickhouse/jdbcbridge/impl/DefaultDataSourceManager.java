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

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Map.Entry;

import io.vertx.core.json.JsonObject;

import ru.yandex.clickhouse.jdbcbridge.core.NamedDataSource;
import ru.yandex.clickhouse.jdbcbridge.core.Utils;
import ru.yandex.clickhouse.jdbcbridge.core.DataSourceStats;
import ru.yandex.clickhouse.jdbcbridge.core.Extension;
import ru.yandex.clickhouse.jdbcbridge.core.ExtensionManager;
import ru.yandex.clickhouse.jdbcbridge.core.DataSourceManager;

import static ru.yandex.clickhouse.jdbcbridge.core.Utils.EMPTY_STRING;

/**
 * This class is the default implmentation of DataSourceManager.
 * 
 * @since 2.0
 */
public class DefaultDataSourceManager implements DataSourceManager {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(DefaultDataSourceManager.class);

    private final List<Extension<NamedDataSource>> types = Collections.synchronizedList(new ArrayList<>(5));
    private final Map<String, NamedDataSource> mappings = Collections.synchronizedMap(new HashMap<>());

    public static void initialize(ExtensionManager manager) {
        DataSourceManager dsManager = manager.getDataSourceManager();
        if (dsManager == null || !(dsManager instanceof DefaultDataSourceManager)) {
            manager.setDataSourceManager(dsManager = new DefaultDataSourceManager());
        }

        manager.registerConfigLoader(
                Utils.getConfiguration("datasources", "DATASOURCE_CONFIG_DIR", "jdbc-bridge.datasource.config.dir"),
                dsManager::reload);
    }

    private Extension<NamedDataSource> getExtensionByType(String typeName) {
        Extension<NamedDataSource> extension = null;
        boolean isFirst = true;

        for (Extension<NamedDataSource> ext : types) {
            if (isFirst) {
                extension = ext;
                isFirst = false;
            }

            if (ext.getName().equals(typeName)) {
                extension = ext;
                break;
            }
        }

        return extension;
    }

    private NamedDataSource createFromType(String uri, String type, boolean nonNullRequired) {
        NamedDataSource ds = null;

        Extension<NamedDataSource> extension = getExtensionByType(type);

        if (extension != null) {
            try {
                ds = extension.newInstance(uri, this, null);
            } catch (Exception e) {
                log.error("Failed to create data source [" + uri + "]", e);
            }
        }

        return ds == null && nonNullRequired ? new NamedDataSource(uri, this, null) : ds;
    }

    /**
     * Create datasource object based on given configuration.
     * 
     * @param id     datasource id
     * @param config configuration in JSON format
     * @return desired datasource
     */
    protected NamedDataSource createFromConfig(String id, JsonObject config) {
        NamedDataSource ds = null;

        Extension<NamedDataSource> extension = getExtensionByType(
                config == null ? null : config.getString(NamedDataSource.CONF_TYPE));
        ds = extension == null ? null : extension.newInstance(id, this, config);

        // fall back to default implementation
        if (ds == null) {
            ds = new NamedDataSource(id, this, config);
        }

        return ds;
    }

    protected void remove(String id, NamedDataSource ds) {
        if (ds == null || EMPTY_STRING.equals(id)) {
            return;
        }

        if (id != null && id.equals(ds.getId())) {
            log.info("Removing datasource [{}] and all its aliases...", id);

            for (String alias : ds.getAliases()) {
                log.info("Removing alias [{}] of datasource [{}]...", alias, id);
                NamedDataSource ref = mappings.get(alias);
                // we don't want to remove a datasource when its id is same as an
                // alias of another datasource
                if (ref == ds) {
                    mappings.remove(alias);
                }
            }
        } else { // just an alias
            log.info("Removing alias [{}] of datasource [{}]...", id, ds.getId());
            mappings.remove(id);
        }

        try {
            ds.close();
        } catch (Exception e) {
        }
    }

    protected void update(String id, JsonObject config) {
        NamedDataSource ds = mappings.get(id);

        boolean addDataSource = false;
        if (ds == null) {
            addDataSource = true;
        } else if (ds.isDifferentFrom(config)) {
            remove(id, mappings.remove(id));
            addDataSource = true;
        }

        if (addDataSource && config != null) {
            log.info("Adding datasource [{}]...", id);

            try {
                ds = createFromConfig(id, config);
                mappings.put(id, ds);

                for (String alias : ds.getAliases()) {
                    if (mappings.containsKey(alias)) {
                        log.warn("Not able to add datasource alias [{}] as it exists already", alias);
                    } else {
                        mappings.put(alias, ds);
                    }
                }
            } catch (Exception e) {
                log.warn("Failed to add datasource [" + id + "]", e);
            }
        }
    }

    @Override
    public void registerType(String typeName, Extension<NamedDataSource> extension) {
        String className = Objects.requireNonNull(extension).getProviderClass().getName();
        typeName = typeName == null || typeName.isEmpty() ? extension.getName() : typeName;

        Extension<NamedDataSource> registered = null;
        for (Extension<NamedDataSource> ext : this.types) {
            if (ext.getName().equals(typeName)) {
                registered = ext;
                break;
            }
        }

        if (registered != null) {
            log.warn("Discard [{}] as type [{}] is reserved by [{}]", className, typeName,
                    registered.getClass().getName());
            return;
        }

        log.info("Registering new type of datasource: [{}] -> [{}]", typeName, className);
        types.add(extension);

        if (types.size() == 1) {
            log.info("Default datasource type is set to [{}]", typeName);
        }
    }

    @Override
    public void put(String id, NamedDataSource datasource) {
        if (datasource == null) {
            log.warn("Non-null datasource is required for registration!");
            return;
        }

        if (id == null) {
            id = datasource.getId();
        }

        NamedDataSource existDs = this.mappings.get(id);
        if (existDs != null) {
            String existId = existDs.getId();

            this.mappings.remove(id);
            if (existId == null ? id == null : existId.equals(id)) {
                log.warn("Datasource alias [{}] was overrided", id);
            } else {
                for (String alias : existDs.getAliases()) {
                    this.mappings.remove(alias);
                }

                log.warn("Datasource [{}] and all its aliases[{}] were removed", existId, existDs.getAliases());
            }

            try {
                existDs.close();
            } catch (Exception e) {
            }
        }

        this.mappings.put(id, datasource);

        // now update aliases...
        for (String alias : datasource.getAliases()) {
            if (alias == null || alias.isEmpty()) {
                continue;
            }

            NamedDataSource ds = this.mappings.get(alias);
            if (ds != null && alias.equals(ds.getId())) {
                log.warn("Not going to add datasource alias [{}] as it's been taken by a datasource", alias);
                continue;
            }

            if (ds != null) {
                this.mappings.remove(alias);
                log.warn("Datasource alias [{}] will be replaced", alias);
            }

            this.mappings.put(alias, datasource);
        }
    }

    @Override
    public void reload(JsonObject config) {
        if (config == null || config.fieldNames().size() == 0) {
            log.info("No datasource configuration found");

            HashSet<String> keys = new HashSet<>();
            for (String key : mappings.keySet()) {
                keys.add(key);
            }

            for (String key : keys) {
                remove(key, mappings.remove(key));
            }
            // mappings.clear();
        } else {
            HashSet<String> keys = new HashSet<>();
            for (Entry<String, Object> entry : config) {
                String key = entry.getKey();
                Object value = entry.getValue();
                if (key != null && value instanceof JsonObject) {
                    keys.add(key);
                    update(key, (JsonObject) value);
                }
            }

            HashSet<String> dsIds = new HashSet<>();
            mappings.entrySet().forEach(entry -> {
                String id = entry.getKey();
                NamedDataSource ds = entry.getValue();
                if (id != null && !id.isEmpty() && ds != null && id.equals(ds.getId())) {
                    dsIds.add(id);
                }
            });

            for (String id : dsIds) {
                if (!keys.contains(id)) {
                    remove(id, mappings.remove(id));
                }
            }
        }
    }

    /**
     * Get or create a datasource from given URI.
     * 
     * @param uri      connection string
     * @param orCreate true to create the datasource anyway
     * @return desired datasource
     */
    public NamedDataSource get(String uri, boolean orCreate) {
        // [<type>:]<id or connection string>[?<query parameters>]
        String id = uri;
        String type = null;
        if (id != null) {
            // remove query parameters first
            int index = id.indexOf('?');
            if (index >= 0) {
                id = id.substring(0, index);
            }

            // and then type prefix
            index = id.indexOf(':');
            if (index >= 0) {
                type = id.substring(0, index);
                id = id.substring(index + 1);
            }

            // now try parsing it as URI
            try {
                URI u = new URI(id);
                if (u.getHost() != null) {
                    id = u.getHost();
                }
            } catch (Exception e) {
            }
        }

        NamedDataSource ds = mappings.get(id);

        if (ds == null && (ds = createFromType(uri, type, orCreate)) == null) {
            throw new IllegalArgumentException("Data source [" + uri + "] not found!");
        }

        return ds;
    }

    @Override
    public final List<DataSourceStats> getDataSourceStats() {
        String[] idOrAlias = this.mappings.keySet().toArray(new String[this.mappings.size()]);
        ArrayList<DataSourceStats> list = new ArrayList<>(idOrAlias.length);
        for (int i = 0; i < idOrAlias.length; i++) {
            String dsName = idOrAlias[i];
            NamedDataSource nds = this.mappings.get(dsName);

            if (nds != null) {
                list.add(new DataSourceStats(idOrAlias[i], nds));
            }
        }
        return list;
    }
}