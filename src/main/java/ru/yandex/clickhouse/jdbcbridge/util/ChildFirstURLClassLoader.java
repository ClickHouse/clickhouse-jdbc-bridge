package ru.yandex.clickhouse.jdbcbridge.util;

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import sun.misc.CompoundEnumeration;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;

public class ChildFirstURLClassLoader extends MutableURLClassLoader {

    private final ParentClassLoader loader;

    public ChildFirstURLClassLoader(ClassLoader classLoader) {
        super(null);
        loader = new ParentClassLoader(classLoader);
    }

    @Override
    protected Class<?> loadClass(String s, boolean b) throws ClassNotFoundException {
        try {
            return super.loadClass(s, b);
        } catch (ClassNotFoundException err) {
            return loader.loadClass(s, b);
        }
    }

    @Override
    public URL getResource(String s) {
        URL url = super.findResource(s);
        if (null == url) {
            url = loader.getResource(s);
        }
        return url;
    }

    @Override
    public Enumeration<URL> getResources(String s) throws IOException {
        UnmodifiableIterator<URL> own = Iterators.forEnumeration(super.getResources(s));
        UnmodifiableIterator<URL> another = Iterators.forEnumeration(loader.getResources(s));
        return Iterators.asEnumeration(Iterators.concat(own, another));

    }

    private static class ParentClassLoader extends ClassLoader {
        public ParentClassLoader(ClassLoader loader) {
            super(loader);
        }

        @Override
        public Class<?> findClass(String s) throws ClassNotFoundException {
            return super.findClass(s);
        }

        @Override
        public Class<?> loadClass(String s) throws ClassNotFoundException {
            return super.loadClass(s);
        }

        @Override
        public Class<?> loadClass(String s, boolean b) throws ClassNotFoundException {
            return super.loadClass(s, b);
        }

    }
}

/**
 * private[spark] class ChildFirstURLClassLoader(urls: Array[URL], parent: ClassLoader)
 * extends MutableURLClassLoader(urls, null) {
 * <p>
 * private val parentClassLoader = new ParentClassLoader(parent)
 * <p>
 * override def loadClass(name: String, resolve: Boolean): Class[_] = {
 * try {
 * super.loadClass(name, resolve)
 * } catch {
 * case e: ClassNotFoundException =>
 * parentClassLoader.loadClass(name, resolve)
 * }
 * }
 * <p>
 * override def getResource(name: String): URL = {
 * val url = super.findResource(name)
 * val res = if (url != null) url else parentClassLoader.getResource(name)
 * res
 * }
 * <p>
 * override def getResources(name: String): Enumeration[URL] = {
 * val childUrls = super.findResources(name).asScala
 * val parentUrls = parentClassLoader.getResources(name).asScala
 * (childUrls ++ parentUrls).asJavaEnumeration
 * }
 * <p>
 * override def addURL(url: URL) {
 * super.addURL(url)
 * }
 * <p>
 * }
 */
