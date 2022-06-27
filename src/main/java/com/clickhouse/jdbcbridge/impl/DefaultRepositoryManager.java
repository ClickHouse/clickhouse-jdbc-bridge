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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.clickhouse.jdbcbridge.core.ManagedEntity;
import com.clickhouse.jdbcbridge.core.Repository;
import com.clickhouse.jdbcbridge.core.RepositoryManager;

/**
 * Default implementation of
 * {@link com.clickhouse.jdbcbridge.core.RepositoryManager}.
 * 
 * @since 2.0
 */
public class DefaultRepositoryManager implements RepositoryManager {
    private final List<Repository<? extends ManagedEntity>> repos = Collections.synchronizedList(new ArrayList<>());

    @SuppressWarnings("unchecked")
    @Override
    public <T extends ManagedEntity> Repository<T> getRepository(Class<T> clazz) {
        Objects.requireNonNull(clazz);

        for (Repository<?> repo : repos) {
            if (repo.accept(clazz)) {
                return (Repository<T>) repo;
            }
        }

        throw new IllegalArgumentException("No repository available for " + clazz.getName());
    }

    @Override
    public void update(List<Repository<?>> repos) {
        if (repos == null) {
            return;
        }

        for (Repository<?> repo : repos) {
            boolean replaced = false;
            for (int i = 0, len = this.repos.size(); i < len; i++) {
                Repository<?> current = this.repos.get(i);
                if (!current.getClass().equals(repo.getClass())
                        && !current.getEntityClass().equals(repo.getEntityClass())) {
                    this.repos.set(i, repo);
                    replaced = true;
                    break;
                }
            }

            if (!replaced) {
                this.repos.add(repo);
            }
        }
    }
}
