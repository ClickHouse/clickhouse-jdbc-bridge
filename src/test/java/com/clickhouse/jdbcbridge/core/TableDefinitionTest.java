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

import static com.clickhouse.jdbcbridge.core.DataType.*;
import static org.testng.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.Test;

public class TableDefinitionTest {
    @Test(groups = { "unit" })
    public void testUpdateValues() {
        TableDefinition list = new TableDefinition(
                new ColumnDefinition("column1", DataType.Int32, true, DEFAULT_LENGTH, DEFAULT_PRECISION, DEFAULT_SCALE),
                new ColumnDefinition("column2", DataType.Str, true, DEFAULT_LENGTH, DEFAULT_PRECISION, DEFAULT_SCALE),
                new ColumnDefinition("column3", DataType.Int32, true, DEFAULT_LENGTH, DEFAULT_PRECISION,
                        DEFAULT_SCALE));

        Object[] expectedValues = new Object[] { 0, "", 0 };
        for (int i = 0; i < list.size(); i++) {
            assertEquals(list.getColumn(i).value.getValue(), expectedValues[i]);
        }

        list.updateValues(null);
        for (int i = 0; i < list.size(); i++) {
            assertEquals(list.getColumn(i).value.getValue(), expectedValues[i]);
        }

        List<ColumnDefinition> refs = new ArrayList<ColumnDefinition>();
        list.updateValues(refs);
        for (int i = 0; i < list.size(); i++) {
            assertEquals(list.getColumn(i).value.getValue(), expectedValues[i]);
        }

        refs.add(new ColumnDefinition("xcolumn", DataType.Str, true, DEFAULT_LENGTH, DEFAULT_PRECISION, DEFAULT_SCALE,
                null, "x", null));
        list.updateValues(refs);
        for (int i = 0; i < list.size(); i++) {
            assertEquals(list.getColumn(i).value.getValue(), expectedValues[i]);
        }

        refs.add(new ColumnDefinition("column2", DataType.Int16, true, DEFAULT_LENGTH, DEFAULT_PRECISION, DEFAULT_SCALE,
                null, "22", null));
        list.updateValues(refs);
        expectedValues = new Object[] { 0, "22", 0 };
        for (int i = 0; i < list.size(); i++) {
            assertEquals(list.getColumn(i).value.getValue(), expectedValues[i]);
        }
    }

    @Test(groups = { "unit" })
    public void testFromString() {
        String inlineSchema = "a Nullable(UInt8) default 3, b Enum8('N/A'=1, 'SB'=2)";
        TableDefinition def = TableDefinition.fromString(inlineSchema);

        assertNotNull(def.getColumn(1).toString());
    }
}