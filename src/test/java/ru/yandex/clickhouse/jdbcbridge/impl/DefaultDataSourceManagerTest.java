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

import static org.testng.Assert.*;

import ru.yandex.clickhouse.jdbcbridge.core.DataSourceManager;
import ru.yandex.clickhouse.jdbcbridge.core.Extension;
import ru.yandex.clickhouse.jdbcbridge.core.NamedDataSource;
import ru.yandex.clickhouse.jdbcbridge.core.Utils;

import org.testng.annotations.Test;

import io.vertx.core.json.JsonObject;

public class DefaultDataSourceManagerTest {
    @Test(groups = { "unit" })
    public void testGet() {
        DefaultDataSourceManager manager = new DefaultDataSourceManager();
        assertNotNull(manager.get("non-existent-ds", true));
        assertThrows(IllegalArgumentException.class, new ThrowingRunnable() {
            @Override
            public void run() throws Throwable {
                manager.get("non-existent-ds", false);
            }
        });
        assertNotNull(manager.get("invalid-type:non-existent-ds", true));
        assertThrows(IllegalArgumentException.class, new ThrowingRunnable() {
            @Override
            public void run() throws Throwable {
                manager.get("invalid-type:non-existent-ds", false);
            }
        });

        manager.registerType(JdbcDataSource.EXTENSION_NAME, new Extension<NamedDataSource>(JdbcDataSource.class));

        String uri = "some invalid uri";
        NamedDataSource ds = manager.get(uri, true);
        assertNotNull(ds);
        assertEquals(ds.getId(), uri);

        uri = "jdbc:mysql://localhost:3306/test?useSSL=false";
        ds = manager.get(uri, true);
        assertNotNull(ds);
        assertEquals(ds.getId(), uri);

        uri = "jdbc:weird:vendor:hostname:1234?database=test";
        ds = manager.get(uri, true);
        assertNotNull(ds);
        assertEquals(ds.getId(), uri);

        uri = "jenkins:https://my.ci-server.org/internal/";
        ds = manager.get(uri, true);
        assertNotNull(ds);
        assertEquals(ds.getId(), uri);
    }

    @Test(groups = { "unit" })
    public void testPutAndGet() {
        DataSourceManager manager = new DefaultDataSourceManager();

        manager.put(null, null);
        manager.put("random", null);

        String id = "nds";
        JsonObject config = Utils.loadJsonFromFile("src/test/resources/datasources/test-nds.json")
                .getJsonObject("test-nds");

        NamedDataSource nds1 = new NamedDataSource("nds1", manager, config);
        manager.put(id, nds1);
        assertThrows(IllegalArgumentException.class, new ThrowingRunnable() {
            @Override
            public void run() throws Throwable {
                manager.get(nds1.getId(), false);
            }
        });
        assertEquals(manager.get(id, false), nds1);
        assertEquals(manager.get("nds01", false), nds1);
        assertEquals(manager.get("nds001", false), nds1);

        NamedDataSource nds2 = new NamedDataSource("nds02", manager, config);
        manager.put("nds02", nds2);
        assertEquals(manager.get(id, false), nds1);
        assertEquals(manager.get("nds02", false), nds2);
        assertEquals(manager.get("nds01", false), nds2);
        assertEquals(manager.get("nds001", false), nds2);

        NamedDataSource nds3 = new NamedDataSource(id, manager, config);
        manager.put(id, nds3);
        assertEquals(manager.get(id, false), nds3);
        assertEquals(manager.get("nds02", false), nds2);
        assertEquals(manager.get("nds01", false), nds3);
        assertEquals(manager.get("nds001", false), nds3);

        NamedDataSource nds4 = new NamedDataSource("nds04", manager,
                Utils.loadJsonFromFile("src/test/resources/datasources/test-datasource.json")
                        .getJsonObject("test-datasource"));
        manager.put("nds01", nds4);
        assertEquals(manager.get(id, false), nds3);
        assertEquals(manager.get("nds02", false), nds2);
        assertEquals(manager.get("nds01", false), nds4);
        assertThrows(IllegalArgumentException.class, new ThrowingRunnable() {
            @Override
            public void run() throws Throwable {
                manager.get("nds001", false);
            }
        });

        manager.put(id, nds1);
        manager.put(id, nds4);
        assertEquals(manager.get(id, false), nds4);
        assertEquals(manager.get("nds02", false), nds2);
        assertThrows(IllegalArgumentException.class, new ThrowingRunnable() {
            @Override
            public void run() throws Throwable {
                manager.get("nds01", false);
            }
        });
        assertThrows(IllegalArgumentException.class, new ThrowingRunnable() {
            @Override
            public void run() throws Throwable {
                manager.get("nds001", false);
            }
        });
    }

    @Test(groups = { "sit" })
    public void testSrvRecordSupport() {
        DefaultDataSourceManager manager = new DefaultDataSourceManager();

        String host = "_sip._udp.sip.voice.google.com";
        String port = "5060";
        String hostAndPort = host + ":" + port;

        assertEquals(manager.resolve("jdbc://{{ _sip._udp.sip.voice.google.com }}/aaa"),
                "jdbc://" + hostAndPort + "/aaa");
    }
}