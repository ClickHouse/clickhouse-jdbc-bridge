package ru.yandex.clickhouse.jdbcbridge.util;

import java.net.URL;
import java.net.URLClassLoader;

public class MutableURLClassLoader extends URLClassLoader {
    public MutableURLClassLoader(ClassLoader classLoader) {
        super(new URL[0], classLoader);
    }

    @Override
    public void addURL(URL url) {
        super.addURL(url);
    }
}
