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

import java.util.Objects;

import org.testng.annotations.Test;

public class ExtensionTest {
    static abstract class MyService {
        public abstract String echo(String input);
    }

    static class EchoService extends MyService {
        public EchoService(Object someContext) {
        }

        @Override
        public String echo(String input) {
            return input;
        }
    }

    static class MyExtension extends MyService {
        public static final String EXTENSION_NAME = "mine";

        public static final String DEFAULT_PREFIX = "unknown";
        public static boolean flag = false;

        public static void initialize(ExtensionManager manager) {
            flag = true;
        }

        public static MyExtension newInstance(Object... args) {
            return new MyExtension(args != null && args.length > 0 ? Objects.toString(args[0]) : DEFAULT_PREFIX);
        }

        private final String prefix;

        public MyExtension(String prefix) {
            this.prefix = Objects.requireNonNull(prefix);
        }

        @Override
        public String echo(String input) {
            return this.prefix + input;
        }
    }

    static class AnotherExtension extends MyService {
        public static final String DEFAULT_PREFIX = "another";

        private final String prefix;

        public AnotherExtension(byte prefix) {
            this.prefix = String.valueOf(prefix);
        }

        public AnotherExtension(int prefix) {
            this.prefix = String.valueOf(prefix);
        }

        public AnotherExtension(long prefix) {
            this.prefix = String.valueOf(prefix);
        }

        public AnotherExtension(String prefix) {
            this.prefix = Objects.requireNonNull(prefix);
        }

        @Override
        public String echo(String input) {
            return this.prefix + input;
        }
    }

    @Test(groups = { "unit" })
    public void testExtension() {
        assertFalse(MyExtension.flag);

        Extension<MyService> extension = new Extension<>(MyExtension.class);
        assertEquals(extension.getName(), MyExtension.EXTENSION_NAME);
        assertEquals(extension.getProviderClass(), MyExtension.class);

        extension.initialize(null);

        assertTrue(MyExtension.flag);

        String input = "input";
        MyService service = extension.newInstance();
        assertTrue(service instanceof MyExtension);
        assertEquals(((MyExtension) service).prefix, MyExtension.DEFAULT_PREFIX);
        assertEquals(((MyExtension) service).echo(input), MyExtension.DEFAULT_PREFIX + input);

        String prefix = "prefix";
        service = extension.newInstance(prefix);
        assertTrue(service instanceof MyExtension);
        assertEquals(((MyExtension) service).prefix, prefix);
        assertEquals(((MyExtension) service).echo(input), prefix + input);
    }

    @Test(groups = { "unit" })
    public void testNotFullyImplementedExtension() {
        Extension<MyService> extension = new Extension<>(EchoService.class);
        assertEquals(extension.getName(), EchoService.class.getSimpleName());
        assertEquals(extension.getProviderClass(), EchoService.class);

        extension.initialize(null);

        assertThrows(UnsupportedOperationException.class, extension::newInstance);

        String prefix = "secret";
        final Extension<MyService> ext = new Extension<>(AnotherExtension.class);
        ext.initialize(null);
        assertThrows(UnsupportedOperationException.class, ext::newInstance);
        assertThrows(UnsupportedOperationException.class, new ThrowingRunnable() {
            @Override
            public void run() throws Throwable {
                ext.newInstance(null);
            }
        });
        assertThrows(UnsupportedOperationException.class, new ThrowingRunnable() {
            @Override
            public void run() throws Throwable {
                ext.newInstance(1.1);
            }
        });
        assertThrows(UnsupportedOperationException.class, new ThrowingRunnable() {
            @Override
            public void run() throws Throwable {
                ext.newInstance(ext);
            }
        });
        assertThrows(IllegalArgumentException.class, new ThrowingRunnable() {
            @Override
            public void run() throws Throwable {
                ext.newInstance(new Object[] { null });
            }
        });

        MyService service = ext.newInstance(new Object[] { 1 });
        assertTrue(service instanceof AnotherExtension);
        assertEquals(((AnotherExtension) service).prefix, "1");

        service = ext.newInstance(prefix);
        assertTrue(service instanceof AnotherExtension);
        assertEquals(((AnotherExtension) service).prefix, prefix);
    }
}