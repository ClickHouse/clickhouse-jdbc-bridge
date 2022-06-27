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

import java.sql.JDBCType;
import java.sql.Types;

import org.testng.annotations.Test;

public class DataTypeMappingTest {
    @Test(groups = { "unit" })
    public void testConstructor() {
        DataTypeMapping m = new DataTypeMapping(Types.BOOLEAN, null, DataType.Bool);
        assertEquals(m.getSourceJdbcType(), JDBCType.BOOLEAN);
        assertEquals(m.getSourceNativeType(), null);
        assertTrue(m.accept(JDBCType.BOOLEAN, null));
        assertEquals(m.getMappedType(), DataType.Bool);

        m = new DataTypeMapping("boolean", "bool", "String");
        assertEquals(m.getSourceJdbcType(), JDBCType.BOOLEAN);
        assertEquals(m.getSourceNativeType(), "bool");
        assertFalse(m.accept(JDBCType.BOOLEAN, null));
        assertFalse(m.accept(JDBCType.BOOLEAN, "Bool"));
        assertTrue(m.accept(JDBCType.VARCHAR, "bool"));
        assertEquals(m.getMappedType(), DataType.Str);

        m = new DataTypeMapping("bit", "*", "Int8");
        assertEquals(m.getSourceJdbcType(), JDBCType.BIT);
        assertEquals(m.getSourceNativeType(), "*");
        assertTrue(m.getSourceNativeType() == DataTypeMapping.ANY_NATIVE_TYPE);
        assertTrue(m.accept(JDBCType.BOOLEAN, null));
        assertTrue(m.accept(JDBCType.BIT, "Bool"));
        assertTrue(m.accept(JDBCType.VARCHAR, "bit"));
        assertEquals(m.getMappedType(), DataType.Int8);
    }
}