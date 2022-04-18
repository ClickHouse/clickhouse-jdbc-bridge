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

public class QueryParametersTest {
    @Test(groups = { "unit" })
    public void testMergeUri() {
        QueryParameters params = new QueryParameters();

        assertEquals(params.isDebug(), false);
        assertEquals(params.nullAsDefault(), false);
        assertEquals(params.showCustomColumns(), false);
        assertEquals(params.showDatasourceColumn(), false);

        params.merge("ds?" + QueryParameters.PARAM_NULL_AS_DEFAULT);
        assertEquals(params.nullAsDefault(), true);
        params.merge("ds?" + QueryParameters.PARAM_CUSTOM_COLUMNS);
        assertEquals(params.showCustomColumns(), true);
        params.merge("ds?" + QueryParameters.PARAM_DATASOURCE_COLUMN + "&" + QueryParameters.PARAM_DEBUG);
        assertEquals(params.showDatasourceColumn(), true);
        assertEquals(params.isDebug(), true);
    }
}