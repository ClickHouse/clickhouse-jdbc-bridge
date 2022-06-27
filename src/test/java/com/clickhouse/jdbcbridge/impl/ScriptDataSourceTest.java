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

import java.util.List;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;

import org.testng.annotations.Test;

public class ScriptDataSourceTest {
    @Test(groups = { "unit" })
    public void testScriptEval() {
        ScriptEngineManager manager = new ScriptEngineManager();
        List<ScriptEngineFactory> factories = manager.getEngineFactories();
        assertNotNull(factories);

        ScriptEngine engine = manager.getEngineByExtension("js");
        assertNotNull(engine);

        try {
            Object obj = engine.eval("var i = 1");
            obj = engine.eval("1");
            obj = engine.eval("function run() { return [['c1', 'uint8'], ['c2', 'String']] }");
            obj = engine.get("run");

            Invocable inv = (Invocable) engine;
            obj = inv.invokeFunction("run");
            obj = null;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
