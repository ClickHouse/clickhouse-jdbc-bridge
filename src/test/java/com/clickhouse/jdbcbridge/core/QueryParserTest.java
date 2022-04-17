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

public class QueryParserTest {
    @Test(groups = { "unit" })
    public void testNormalizeQuery() {
        String query = "some_table";
        assertEquals(QueryParser.normalizeQuery(query), query);

        query = "some named query";
        assertEquals(QueryParser.normalizeQuery(query), query);

        query = "SELECT col1, col2 FROM some_table";
        assertEquals(QueryParser.normalizeQuery(query), query);

        query = "SELECT col1, col2 FROM some_schema.some_table";
        assertEquals(QueryParser.normalizeQuery(query), query);

        query = "SELECT `col1`, `col2` FROM `some_table`";
        assertEquals(QueryParser.normalizeQuery(query), "some_table");

        query = "SELECT `col1`, `col2` FROM `some_schema`.`some_table`";
        assertEquals(QueryParser.normalizeQuery(query), "some_table");

        query = "SELECT \"col1\", \"col2\" FROM \"some_table\"";
        assertEquals(QueryParser.normalizeQuery(query), "some_table");

        query = "SELECT \"col1\", \"col2\" FROM \"some_schema\".\"some_table\"";
        assertEquals(QueryParser.normalizeQuery(query), "some_table");

        String embeddedQuery = "select 1";
        query = "SELECT `col1`, `col2` FROM `" + embeddedQuery + "`";
        assertEquals(QueryParser.normalizeQuery(query), embeddedQuery);

        query = "SELECT `col1`, `col2` FROM `some_schema`.`" + embeddedQuery + "`";
        assertEquals(QueryParser.normalizeQuery(query), embeddedQuery);

        query = "SELECT \"col1\", \"col2\" FROM \"" + embeddedQuery + "\"";
        assertEquals(QueryParser.normalizeQuery(query), embeddedQuery);

        query = "SELECT \"col1\", \"col2\" FROM \"some_schema\".\"" + embeddedQuery + "\"";
        assertEquals(QueryParser.normalizeQuery(query), embeddedQuery);

        embeddedQuery = "select 's' as s";

        query = "SELECT `s` FROM `" + embeddedQuery + "`";
        assertEquals(QueryParser.normalizeQuery(query), embeddedQuery);

        query = "SELECT `s` FROM `" + embeddedQuery + "` WHERE `s` = 's'";
        assertEquals(QueryParser.normalizeQuery(query), embeddedQuery);

        embeddedQuery = "select\t'1`2\"3'\r\n, -- `\"\n/* \"s` */'`''`'";
        query = "SELECT `col1`, `col2` FROM `" + embeddedQuery + "`";
        assertEquals(QueryParser.normalizeQuery(query), embeddedQuery);

        query = "SELECT `col1`, `col2` FROM `some_schema`.`" + embeddedQuery + "`";
        assertEquals(QueryParser.normalizeQuery(query), embeddedQuery);

        query = "SELECT \"col1\", \"col2\" FROM \"" + embeddedQuery + "\"";
        assertEquals(QueryParser.normalizeQuery(query), embeddedQuery);

        query = "SELECT \"col1\", \"col2\" FROM \"some_schema\".\"" + embeddedQuery + "\"";
        assertEquals(QueryParser.normalizeQuery(query), embeddedQuery);

        query = "SELECT * FROM test.test_table";
        assertEquals(QueryParser.normalizeQuery(query), query);

        embeddedQuery = "SELECT 1 as \\`a\\`, ''\\`2'' as \\`b\\`";
        query = "SELECT `col1`, `col2` FROM `" + embeddedQuery + "`";
        assertEquals(QueryParser.normalizeQuery(query), embeddedQuery.replace("\\", ""));
    }

    @Test(groups = { "unit" })
    public void testExtractSchemaName() {
        assertEquals(QueryParser.extractSchemaName(null), "");
        assertEquals(QueryParser.extractSchemaName(""), "");
        assertEquals(QueryParser.extractSchemaName("a"), "");
        assertEquals(QueryParser.extractSchemaName("a.a"), "");

        String schema = "schema";
        assertEquals(QueryParser.extractSchemaName("SELECT * FROM " + "`" + schema + "`.`table`"), schema);
        assertEquals(QueryParser.extractSchemaName("SELECT * FROM " + "\"" + schema + "\".\"table\""), schema);
    }

    @Test(groups = { "unit" })
    public void testExtractTableName() {
        assertEquals(QueryParser.extractTableName(null), null);
        assertEquals(QueryParser.extractTableName(""), "");
        assertEquals(QueryParser.extractTableName("a"), "a");
        assertEquals(QueryParser.extractTableName("a.a"), "a.a");

        String table = "`schema`.`table`";
        assertEquals(QueryParser.extractTableName("SELECT * FROM " + table), table);
        assertEquals(QueryParser.extractTableName("SELECT * from " + table), table);
        assertEquals(QueryParser.extractTableName("SELECT * FROM  " + table + " where col1=11"), table);
        assertEquals(QueryParser.extractTableName("SELECT * FROM\r" + table + " where col1=11"), table);
        assertEquals(QueryParser.extractTableName("SELECT * FROM (select col1 from " + table + " where col1=11) a"),
                table);
        assertEquals(QueryParser.extractTableName(
                "SELECT col1, ' from b' as a FROM (select col1 from " + table + " where col1=11) a"), table);
    }

    @Test(groups = { "unit" })
    public void testQueryWithParameters() {
        QueryParser parser = new QueryParser("connection_uri?p1=table&p2=column&p3=value1&p4=value2", "",
                "select a from {{ p1 }} where {{p2}} = 2 and '{{ p3}}' != '{{ p4}}'",
                "columns format version: 1\n1 columns:\n`a` Int32", "RowBinary", "false", null);

        assertEquals(parser.getNormalizedQuery(), "select a from table where column = 2 and 'value1' != 'value2'");
    }

    // Reference:
    // https://github.com/ClickHouse/ClickHouse/blob/06446b4f08a142d6f1bc30664c47ded88ab51782/src/Common/tests/gtest_unescapeForFileName.cpp#L9
    @Test(groups = { "unit" })
    public void testUnescape() {
        String str = "";
        assertEquals(str, QueryParser.unescape(str));
        assertEquals(str = "172.19.0.6", QueryParser.unescape(str));
        assertEquals(str = "abcd.", QueryParser.unescape(str));
        assertEquals(str = "abcd", QueryParser.unescape(str));
        assertEquals(str = "..::", QueryParser.unescape(str));

        assertEquals("`nbusfreq` Nullable(String)", QueryParser.unescape("%60nbusfreq%60%20Nullable%28String%29"));
        assertEquals("里程", QueryParser.unescape("%E9%87%8C%E7%A8%8B"));

        assertEquals("{", QueryParser.unescape("%7b"));
        assertEquals("{", QueryParser.unescape("%7B"));
        assertEquals("09AFaf", QueryParser.unescape("%30%39%41%46%61%66"));
    }
}