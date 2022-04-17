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

import static org.testng.Assert.*;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import com.clickhouse.jdbcbridge.core.Extension;
import com.clickhouse.jdbcbridge.core.ExtensionManager;
import com.clickhouse.jdbcbridge.core.NamedDataSource;
import com.clickhouse.jdbcbridge.core.Repository;
import com.clickhouse.jdbcbridge.core.RepositoryManager;
import com.clickhouse.jdbcbridge.core.Utils;

import org.testng.annotations.Test;

import io.vertx.core.json.JsonObject;

public class JsonFileRepositoryTest {
    static class TestExtensionManager implements ExtensionManager {
        private final RepositoryManager repoManager = new DefaultRepositoryManager();

        @Override
        public <T> Extension<T> getExtension(Class<? extends T> clazz) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public RepositoryManager getRepositoryManager() {
            return repoManager;
        }

        @Override
        public void registerConfigLoader(String configPath, Consumer<JsonObject> consumer) {
        }

        @Override
        public Map<String, Object> getScriptableObjects() {
            return new HashMap<>();
        }
    }

    @Test(groups = { "unit" })
    public void testGet() {
        Repository<NamedDataSource> repo = new JsonFileRepository<>(NamedDataSource.class);
        assertNull(repo.get("non-existent-ds"));
        assertNull(repo.get("invalid-type:non-existent-ds"));

        repo.registerType("jdbc", new Extension<NamedDataSource>(NamedDataSource.class));
        assertThrows(IllegalArgumentException.class, new ThrowingRunnable() {
            @Override
            public void run() throws Throwable {
                repo.get("non-existent-ds");
            }
        });
        assertThrows(IllegalArgumentException.class, new ThrowingRunnable() {
            @Override
            public void run() throws Throwable {
                repo.get("invalid-type:non-existent-ds");
            }
        });
        assertThrows(IllegalArgumentException.class, new ThrowingRunnable() {
            @Override
            public void run() throws Throwable {
                repo.get("jenkins:https://my.ci-server.org/internal/");
            }
        });

        String uri = "jdbc:mysql://localhost:3306/test?useSSL=false";
        NamedDataSource ds = repo.get(uri);
        assertNotNull(ds);
        assertEquals(ds.getId(), uri);

        uri = "jdbc:weird:vendor:hostname:1234?database=test";
        ds = repo.get(uri);
        assertNotNull(ds);
        assertEquals(ds.getId(), uri);
    }

    @Test(groups = { "unit" })
    public void testPutAndGet() {
        Repository<NamedDataSource> repo = new JsonFileRepository<>(NamedDataSource.class);

        assertThrows(NullPointerException.class, new ThrowingRunnable() {
            @Override
            public void run() throws Throwable {
                repo.put(null, null);
            }
        });
        assertThrows(NullPointerException.class, new ThrowingRunnable() {
            @Override
            public void run() throws Throwable {
                repo.put("random", null);
            }
        });

        String id = "nds";
        JsonObject config = Utils.loadJsonFromFile("src/test/resources/datasources/test-nds.json")
                .getJsonObject("test-nds");

        NamedDataSource nds1 = new NamedDataSource("nds1", repo, config);
        repo.put(id, nds1);
        assertNull(repo.get(nds1.getId()));
        assertEquals(repo.get(id), nds1);
        assertEquals(repo.get("nds01"), nds1);
        assertEquals(repo.get("nds001"), nds1);

        NamedDataSource nds2 = new NamedDataSource("nds02", repo, config);
        repo.put("nds02", nds2);
        assertEquals(repo.get(id), nds1);
        assertEquals(repo.get("nds02"), nds2);
        assertEquals(repo.get("nds01"), nds2);
        assertEquals(repo.get("nds001"), nds2);

        NamedDataSource nds3 = new NamedDataSource(id, repo, config);
        repo.put(id, nds3);
        assertEquals(repo.get(id), nds3);
        assertEquals(repo.get("nds02"), nds2);
        assertEquals(repo.get("nds01"), nds3);
        assertEquals(repo.get("nds001"), nds3);

        NamedDataSource nds4 = new NamedDataSource("nds04", repo,
                Utils.loadJsonFromFile("src/test/resources/datasources/test-datasource.json")
                        .getJsonObject("test-datasource"));
        repo.put("nds01", nds4);
        assertEquals(repo.get(id), nds3);
        assertEquals(repo.get("nds02"), nds2);
        assertEquals(repo.get("nds01"), nds4);
        assertEquals(repo.get("nds001"), nds3);

        NamedDataSource nds5 = new NamedDataSource(id, repo, config);
        repo.put(id, nds5);
        repo.put(id, nds4);
        assertEquals(repo.get(id), nds4);
        assertEquals(repo.get("nds02"), nds2);
        assertNull(repo.get("nds01"));
        assertNull(repo.get("nds001"));
    }

    @Test(groups = { "sit" })
    public void testSrvRecordSupport() {
        Repository<NamedDataSource> repo = new JsonFileRepository<>(NamedDataSource.class);

        String host = "_sip._udp.sip.voice.google.com";
        String port = "5060";
        String hostAndPort = host + ":" + port;

        assertEquals(repo.resolve("jdbc://{{ _sip._udp.sip.voice.google.com }}/aaa"), "jdbc://" + hostAndPort + "/aaa");
    }
}