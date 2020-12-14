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
package ru.yandex.clickhouse.jdbcbridge;

import static org.testng.Assert.*;

import java.util.List;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import io.vertx.core.Vertx;
import ru.yandex.clickhouse.jdbcbridge.core.BaseRepository;
import ru.yandex.clickhouse.jdbcbridge.core.ManagedEntity;
import ru.yandex.clickhouse.jdbcbridge.core.Extension;
import ru.yandex.clickhouse.jdbcbridge.core.ExtensionManager;
import ru.yandex.clickhouse.jdbcbridge.core.NamedDataSource;
import ru.yandex.clickhouse.jdbcbridge.core.NamedQuery;
import ru.yandex.clickhouse.jdbcbridge.core.NamedSchema;
import ru.yandex.clickhouse.jdbcbridge.core.Repository;
import ru.yandex.clickhouse.jdbcbridge.core.Utils;
import ru.yandex.clickhouse.jdbcbridge.impl.JsonFileRepository;

public class JdbcBridgeVerticleTest {
    private Vertx vertx;

    public static class TestRepository<T extends ManagedEntity> extends BaseRepository<T> {
        public TestRepository(ExtensionManager manager, Class<T> clazz) {
            super(clazz);
        }
    }

    @BeforeSuite(groups = { "unit" })
    public void beforeSuite() {
        vertx = Vertx.vertx();
    }

    @AfterSuite(groups = { "unit" })
    public void afterSuite() {
        if (vertx != null) {
            vertx.close();
        }
    }

    @Test(groups = { "unit" })
    public void testLoadRepositories() {
        JdbcBridgeVerticle main = new JdbcBridgeVerticle();
        vertx.deployVerticle(main);
        List<Repository<?>> repos = main.loadRepositories(null);
        assertNotNull(repos);
        assertEquals(repos.size(), 3);
        assertEquals(repos.get(0).getClass(), JsonFileRepository.class);
        assertEquals(repos.get(0).getEntityClass(), NamedDataSource.class);
        assertEquals(repos.get(1).getClass(), JsonFileRepository.class);
        assertEquals(repos.get(1).getEntityClass(), NamedSchema.class);
        assertEquals(repos.get(2).getClass(), JsonFileRepository.class);
        assertEquals(repos.get(2).getEntityClass(), NamedQuery.class);

        repos = main.loadRepositories(Utils.loadJsonFromFile("src/test/resources/server.json"));
        assertNotNull(repos);
        assertEquals(repos.size(), 2);
        assertEquals(repos.get(0).getClass(), JsonFileRepository.class);
        assertEquals(repos.get(0).getEntityClass(), NamedDataSource.class);
        assertEquals(repos.get(1).getClass(), TestRepository.class);
        assertEquals(repos.get(1).getEntityClass(), NamedSchema.class);
    }

    @Test(groups = { "unit" })
    public void testLoadExtensions() {
        JdbcBridgeVerticle main = new JdbcBridgeVerticle();
        List<Extension<?>> extensions = main.loadExtensions(null);
        assertNotNull(extensions);
        assertEquals(extensions.size(), 3);

        extensions = main.loadExtensions(Utils.loadJsonFromFile("src/test/resources/server.json"));
        assertNotNull(extensions);
        assertEquals(extensions.size(), 3);
    }
}