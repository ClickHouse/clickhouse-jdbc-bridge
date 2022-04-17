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

import static com.clickhouse.jdbcbridge.core.ExpandedUrlClassLoader.FILE_URL_PREFIX;
import static org.testng.Assert.*;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.testng.annotations.Test;

public class ExpandedUrlClassLoaderTest {
    private static final String TMP_DIR_PREFIX = "jdbc-bridge-test_";

    private void downloadJar(String fromUrl, String toFile) throws IOException {
        File file = new File(toFile);
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();
        file.deleteOnExit();

        try (InputStream in = new URL(fromUrl).openConnection().getInputStream();
                OutputStream out = new BufferedOutputStream(new FileOutputStream(file))) {
            byte[] buffer = new byte[1024];

            int numRead = -1;

            while ((numRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, numRead);
            }
        }
    }

    private ClassLoader testLoadClassAndMethod(ClassLoader parent, String[] urls, String className, String methodName,
            boolean hasClass, boolean hasMethod) {
        ExpandedUrlClassLoader classLoader = new ExpandedUrlClassLoader(parent, urls);
        Class<?> clazz = null;
        try {
            clazz = classLoader.loadClass(className);
        } catch (Exception e) {
            if (hasClass) {
                fail("Not able to load " + className);
            }
        }

        if (hasClass) {
            assertNotNull(clazz);

            Method method = null;
            try {
                method = clazz.getDeclaredMethod(methodName);
            } catch (Exception e) {
                if (hasMethod) {
                    fail("Not able to find method [" + methodName + "] from class " + className);
                }
            }

            if (hasMethod) {
                assertNotNull(method);
            } else {
                assertNull(method);
            }

        } else {
            assertNull(clazz);
        }

        return classLoader;
    }

    @Test(groups = { "unit" })
    public void testExpandURLs() throws IOException {
        // invalid URLs
        URL[] urls = ExpandedUrlClassLoader.expandURLs("a", "b", ".", "..", "", null, File.separator);
        assertNotNull(urls);
        assertEquals(urls.length, 5);

        // remote URLs
        String url1 = "https://some.host1.com/path1/a.jar";
        String url2 = "https://some.host2.com/b.jar";
        String url3 = "https://some.host3.com";

        urls = ExpandedUrlClassLoader.expandURLs(url1, url2, url3, url2, url1);
        assertNotNull(urls);
        assertEquals(urls.length, 3);

        // now, local paths
        url1 = FILE_URL_PREFIX + ".";
        urls = ExpandedUrlClassLoader.expandURLs(url1, null, url1);
        assertNotNull(urls);
        assertEquals(urls.length, 1);

        File tmpDir = Files.createTempDirectory(TMP_DIR_PREFIX).toFile();
        tmpDir.deleteOnExit();

        for (String file : new String[] { "a.jar", "b.jar" }) {
            File tmpFile = new File(tmpDir.getPath() + File.separator + file);
            tmpFile.deleteOnExit();
            tmpFile.createNewFile();
        }
        url1 = FILE_URL_PREFIX + tmpDir.getPath();
        url2 = FILE_URL_PREFIX + tmpDir.getPath() + File.separator + "a.jar";
        url3 = FILE_URL_PREFIX + tmpDir.getPath() + File.separator + "non-exist.jar";
        urls = ExpandedUrlClassLoader.expandURLs(url1, url2, url1, url3, url2);
        assertNotNull(urls);
        assertEquals(urls.length, 4);

        url1 = "test" + File.separator + "a";
        url2 = "." + File.separator + "test" + File.separator + "a";
        url3 = FILE_URL_PREFIX + "." + File.separator + "test" + File.separator + "a";
        urls = ExpandedUrlClassLoader.expandURLs(url1, url2, url3);
        assertNotNull(urls);
        assertEquals(urls.length, 2);
    }

    @Test(groups = { "sit" })
    public void testClassLoaderWithOldAndNewClass() throws IOException {
        // https://github.com/ClickHouse/clickhouse-jdbc/commit/ee0b57cbed7a09108e5e3eab461f7adfe97ca546
        String className = "ru.yandex.clickhouse.settings.ClickHouseProperties";
        String methodName = "isUsePathAsDb";

        String notRelated = "https://repo1.maven.org/maven2/ru/yandex/qatools/htmlelements/htmlelements-java/1.20.0/htmlelements-java-1.20.0.jar";
        String oldVersion = "https://repo1.maven.org/maven2/ru/yandex/clickhouse/clickhouse-jdbc/0.1.54/clickhouse-jdbc-0.1.54.jar";
        String newVersion = "https://repo1.maven.org/maven2/ru/yandex/clickhouse/clickhouse-jdbc/0.2.4/clickhouse-jdbc-0.2.4.jar";

        // single url
        ClassLoader cl1 = testLoadClassAndMethod(null, new String[] { notRelated }, className, methodName, false,
                false);
        ClassLoader cl2 = testLoadClassAndMethod(null, new String[] { oldVersion }, className, methodName, true, false);
        ClassLoader cl3 = testLoadClassAndMethod(null, new String[] { newVersion }, className, methodName, true, true);

        testLoadClassAndMethod(cl2, new String[] { notRelated }, className, methodName, true, false);
        testLoadClassAndMethod(cl3, new String[] { oldVersion }, className, methodName, true, true);
        testLoadClassAndMethod(cl1, new String[] { newVersion }, className, methodName, true, true);

        // multiple urls
        cl1 = testLoadClassAndMethod(null, new String[] { notRelated, oldVersion }, className, methodName, true, false);
        cl2 = testLoadClassAndMethod(null, new String[] { newVersion, oldVersion, notRelated }, className, methodName,
                true, true);

        testLoadClassAndMethod(cl2, new String[] { notRelated, oldVersion }, className, methodName, true, true);
        testLoadClassAndMethod(cl1, new String[] { newVersion, oldVersion, notRelated }, className, methodName, true,
                false);

        // local files
        File tmpDir = Files.createTempDirectory(TMP_DIR_PREFIX).toFile();
        tmpDir.deleteOnExit();

        for (String[] pair : new String[][] { new String[] { notRelated, "a.jar" },
                new String[] { oldVersion, "b.jar" }, new String[] { newVersion, "c.jar" } }) {
            downloadJar(pair[0], tmpDir.getPath() + File.separator + pair[1]);
        }

        testLoadClassAndMethod(null, new String[] { FILE_URL_PREFIX + tmpDir.getPath() }, className, methodName, true,
                false);
        testLoadClassAndMethod(null, new String[] { tmpDir.getPath() }, className, methodName, true, false);
        testLoadClassAndMethod(null, new String[] { FILE_URL_PREFIX + tmpDir.getPath() + File.separator + "c.jar",
                FILE_URL_PREFIX + tmpDir.getPath() }, className, methodName, true, true);

        // try again using relative path
        String parentPath = "test-dir";
        Paths.get(parentPath).toFile().deleteOnExit();

        String relativePath = parentPath + File.separator + "drivers";

        tmpDir = Paths.get(relativePath).toFile();
        tmpDir.deleteOnExit();
        tmpDir.mkdirs();

        for (String[] pair : new String[][] { new String[] { notRelated, "a.jar" },
                new String[] { oldVersion, "b.jar" }, new String[] { newVersion, ".." + File.separator + "c.jar" } }) {
            downloadJar(pair[0], tmpDir.getPath() + File.separator + pair[1]);
        }

        testLoadClassAndMethod(null, new String[] { relativePath }, className, methodName, true, false);
        testLoadClassAndMethod(null, new String[] { "." + File.separator + relativePath }, className, methodName, true,
                false);
        testLoadClassAndMethod(null, new String[] { relativePath + File.separator + ".." + File.separator + "drivers" },
                className, methodName, true, false);
        testLoadClassAndMethod(null,
                new String[] {
                        "." + File.separator + relativePath + File.separator + ".." + File.separator + "drivers" },
                className, methodName, true, false);
        testLoadClassAndMethod(null, new String[] { relativePath + File.separator + ".." + File.separator + "c.jar" },
                className, methodName, true, true);

        testLoadClassAndMethod(null, new String[] { FILE_URL_PREFIX + "." + File.separator + relativePath }, className,
                methodName, false, false);
    }
}
