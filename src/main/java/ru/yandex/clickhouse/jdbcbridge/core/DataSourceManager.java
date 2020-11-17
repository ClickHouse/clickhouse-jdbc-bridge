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

import java.util.List;

/**
 * This interface defines a service responsible for registering new datasource
 * type, as well as adding/retrieving named datasources using ID or alias.
 * 
 * @since 2.0
 */
public interface DataSourceManager extends Reloadable {
    static final String DEFAULT_TYPE = "default";

    final DnsResolver resolver = new DnsResolver();

    /**
     * Resolve given connection string.
     * 
     * @param uri connection string
     * @return resolved connection string
     */
    default String resolve(String uri) {
        return Utils.applyVariables(uri, resolver::apply);
    }

    /**
     * Get statistics of all datasources.
     * 
     * @return statistics of all datasources
     */
    List<DataSourceStats> getDataSourceStats();

    /**
     * Register new datasource type.
     * 
     * @param typeName  name of the new datasource type to register
     * @param extension extension of the new datasource type
     */
    void registerType(String typeName, Extension<NamedDataSource> extension);

    /**
     * Add a new datasource.
     * 
     * @param id         id of the new datasource; if it's null, datasource.getId()
     *                   will be used instead
     * @param datasource non-null datasource to be added
     */
    void put(String id, NamedDataSource datasource);

    default NamedDataSource get(String id) {
        return get(id, false);
    }

    /**
     * Get or create a data source from given URI.
     * 
     * @param uri      connection string
     * @param orCreate true to create the data source if no found; false otherwise
     * @return desired data source
     */
    NamedDataSource get(String uri, boolean orCreate);
}
