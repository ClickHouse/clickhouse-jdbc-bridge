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
package ru.yandex.clickhouse.jdbcbridge.core;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Enhanced URL class loader which supports directory.
 * 
 * @since 2.0
 */
public class ExpandedUrlClassLoader extends URLClassLoader {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ExpandedUrlClassLoader.class);

    private static final String PROTOCOL_FILE = "file";
    private static final String FILE_URL_PREFIX = PROTOCOL_FILE + ":///";
    private static final String DRIVER_EXTENSION = ".jar";

    // not going to use OSGi and maven which are over-complex
    protected static URL[] expandURLs(String... urls) {
        Set<String> cache = new HashSet<>();
        List<URL> list = new ArrayList<>(Objects.requireNonNull(urls).length * 2);
        Set<URL> negativeSet = new HashSet<>();

        for (String s : urls) {
            if (s == null || s.isEmpty() || cache.contains(s)) {
                continue;
            }

            boolean isNegative = s.length() > 1 && s.charAt(0) == '!';
            if (isNegative) {
                s = s.substring(1);
            }

            URL url = null;
            try {
                url = cache.add(s) ? new URL(s) : null;
            } catch (MalformedURLException e) {
                // might be a local path?
                if (cache.add(s = FILE_URL_PREFIX + Paths.get(s).normalize().toFile().getAbsolutePath())) {
                    try {
                        url = new URL(s);
                    } catch (MalformedURLException exp) {
                        log.warn("Skip malformed URL [{}]", s);
                    }
                }
            }

            if (url == null) {
                continue;
            }

            boolean isValid = true;
            if (PROTOCOL_FILE.equals(url.getProtocol())) {
                Path path = null;
                try {
                    path = Paths.get(url.toURI());
                } catch (URISyntaxException e) {
                    isValid = false;
                    log.warn("Skip invalid URL [{}]", url);
                } catch (InvalidPathException e) {
                    isValid = false;
                    log.warn("Skip invalid path [{}]", url);
                }

                if (path != null && Files.isDirectory(path)) {
                    File dir = path.toFile();
                    for (String file : dir.list()) {
                        if (file.endsWith(DRIVER_EXTENSION)) {
                            file = new StringBuilder().append(FILE_URL_PREFIX).append(dir.getPath()).append('/')
                                    .append(file).toString();

                            if (isNegative) {
                                try {
                                    negativeSet.add(new URL(file));
                                } catch (Exception e) {
                                    // ignore
                                }
                            } else if (cache.add(file)) {
                                try {
                                    list.add(new URL(file));
                                } catch (MalformedURLException e) {
                                    log.warn("Skip invalid file [{}]", file);
                                }
                            } else {
                                log.warn("Discard duplicated file [{}]", file);
                            }
                        }
                    }
                }
            }

            if (isValid) {
                (isNegative ? negativeSet : list).add(url);
            }
        }

        if (list.removeAll(negativeSet)) {
            if (log.isDebugEnabled()) {
                log.debug("Excluded URLs: {}", negativeSet);
            }
        }

        return list.toArray(new URL[list.size()]);
    }

    public ExpandedUrlClassLoader(ClassLoader parent, String... urls) {
        super(expandURLs(urls), parent == null ? ExpandedUrlClassLoader.class.getClassLoader() : parent);
    }
}
