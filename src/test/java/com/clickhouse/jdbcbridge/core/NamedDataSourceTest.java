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

import static org.testng.Assert.*;

import org.testng.annotations.Test;

import io.vertx.core.json.JsonObject;

public class NamedDataSourceTest {
    static class TestRepository<T extends ManagedEntity> extends BaseRepository<T> {
        private int counter = 0;

        public TestRepository(Class<T> clazz) {
            super(clazz);
        }

        @Override
        protected void atomicAdd(T entity) {
            counter++;
        }

        @Override
        protected void atomicRemove(T entity) {
            counter--;
        }

        public int getCounter() {
            return this.counter;
        }
    }

    @Test(groups = { "unit" })
    public void testConstructor() {
        String dataSourceId = "test-datasource";
        JsonObject config = Utils.loadJsonFromFile("src/test/resources/datasources/test-datasource.json");

        NamedDataSource ds = new NamedDataSource(dataSourceId, new TestRepository<>(NamedDataSource.class),
                config.getJsonObject(dataSourceId));
        assertEquals(ds.getId(), dataSourceId);
        for (ColumnDefinition col : ds.getCustomColumns()) {
            assertEquals(col.getName(), "c_" + col.getType().name().toLowerCase());
            switch (col.getType()) {
                case Bool:
                    assertEquals(String.valueOf(col.getValue()), "1");
                    break;
                case Decimal:
                case Decimal32:
                case Decimal64:
                case Decimal128:
                case Decimal256:
                case Float32:
                case Float64:
                    assertEquals(String.valueOf(col.getValue()), "2.0", col.getType().name());
                    break;
                default:
                    assertEquals(String.valueOf(col.getValue()), "2", col.getType().name());
                    break;
            }
        }

        for (DataType type : DataType.values()) {
            Object value = ds.getDefaultValues().getTypedValue(type).getValue();
            switch (type) {
                case Bool:
                    assertEquals(String.valueOf(value), "0");
                    break;
                case Decimal:
                case Decimal32:
                case Decimal64:
                case Decimal128:
                case Decimal256:
                case Float32:
                case Float64:
                    assertEquals(String.valueOf(value), "3.0", type.name());
                    break;
                default:
                    assertEquals(String.valueOf(value), "3", type.name());
                    break;
            }
        }
    }

    @Test(groups = { "unit" })
    public void testGetColumns() {
        String dataSourceId = "test-datasource";
        JsonObject config = Utils.loadJsonFromFile("src/test/resources/datasources/test-datasource.json");

        NamedDataSource ds = new NamedDataSource(dataSourceId, new TestRepository<>(NamedDataSource.class),
                config.getJsonObject(dataSourceId));
        ds.getResultColumns("", "src/test/resources/simple.query", new QueryParameters());
        assertEquals(ds.getId(), dataSourceId);
    }
}