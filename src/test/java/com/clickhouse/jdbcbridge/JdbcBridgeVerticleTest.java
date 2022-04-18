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
package com.clickhouse.jdbcbridge;

import static org.testng.Assert.*;

import java.time.Duration;
import java.util.List;

import com.clickhouse.jdbcbridge.core.BaseRepository;
import com.clickhouse.jdbcbridge.core.Extension;
import com.clickhouse.jdbcbridge.core.ExtensionManager;
import com.clickhouse.jdbcbridge.core.ManagedEntity;
import com.clickhouse.jdbcbridge.core.Utils;

import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import static java.time.temporal.ChronoUnit.SECONDS;

public class JdbcBridgeVerticleTest {
    // containers for SIT(sytem integration test)
    private static final Network sharedNetwork = Network.newNetwork();
    // https://github.com/testcontainers/testcontainers-java/blob/master/modules/postgresql/src/main/java/org/testcontainers/containers/PostgreSQLContainer.java
    private static final GenericContainer<?> pgServer = new GenericContainer<>("postgres:11.10-alpine")
            .withNetwork(sharedNetwork).withNetworkAliases("postgresql_server").withEnv("POSTGRES_DB", "test")
            .withEnv("POSTGRES_USER", "sa").withEnv("POSTGRES_PASSWORD", "sa")
            .waitingFor(new LogMessageWaitStrategy().withRegEx(".*database system is ready to accept connections.*\\s")
                    .withTimes(2).withStartupTimeout(Duration.of(60, SECONDS)));
    // https://github.com/testcontainers/testcontainers-java/blob/master/modules/mariadb/src/main/java/org/testcontainers/containers/MariaDBContainer.java
    private static final GenericContainer<?> mdServer = new GenericContainer<>("mariadb:10.5")
            .withNetwork(sharedNetwork).withNetworkAliases("mariadb_server").withEnv("MYSQL_DATABASE", "test")
            .withEnv("MYSQL_ROOT_PASSWORD", "root").withStartupAttempts(3)
            .waitingFor(new LogMessageWaitStrategy().withRegEx(".*mysqld: ready for connections.*").withTimes(2)
                    .withStartupTimeout(Duration.of(60, SECONDS)));

    private static final GenericContainer<?> jbServer = new GenericContainer<>("clickhouse/jdbc-bridge")
            .withNetwork(sharedNetwork).withNetworkAliases("jdbc_bridge_server")
            .withFileSystemBind("target", "/build", BindMode.READ_WRITE)
            .withWorkingDirectory("/build/test-classes/sit/jdbc-bridge")
            .withCommand("bash", "-c", "java -jar /build/clickhouse-jdbc-bridge*.jar")
            .waitingFor(Wait.forHttp("/ping").forStatusCode(200).withStartupTimeout(Duration.of(60, SECONDS)));
    private static final GenericContainer<?> chServer = new GenericContainer<>("clickhouse/clickhouse-server:22.3")
            .withNetwork(sharedNetwork).withNetworkAliases("clickhouse_server")
            .withClasspathResourceMapping("sit/ch-server/jdbc-bridge.xml",
                    "/etc/clickhouse-server/config.d/jdbc-bridge.xml", BindMode.READ_ONLY)
            .waitingFor(Wait.forHttp("/ping").forStatusCode(200).withStartupTimeout(Duration.of(60, SECONDS)));

    public static class TestRepository<T extends ManagedEntity> extends BaseRepository<T> {
        public TestRepository(ExtensionManager manager, Class<T> clazz) {
            super(clazz);
        }
    }

    @BeforeSuite(groups = { "sit" })
    public static void beforeSuite() {
        pgServer.start();
        mdServer.start();
        chServer.start();
        jbServer.start();
    }

    @AfterSuite(groups = { "sit" })
    public static void afterSuite() {
        pgServer.stop();
        mdServer.stop();
        chServer.stop();
        jbServer.stop();
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
