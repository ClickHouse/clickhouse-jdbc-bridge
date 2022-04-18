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

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Objects;

/**
 * This class defines an extension of JDBC bridge. Basically it's composed of 3
 * pieces: 1) name; 2) method for initialization; and 3) method for
 * instantiation.
 * 
 * @since 2.0
 */
public final class Extension<T> {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(Extension.class);

    private static final String EXTENSION_NAME = "EXTENSION_NAME";

    private static final String METHOD_INITIALIZE = "initialize";
    private static final String METHOD_NEW_INSTANCE = "newInstance";

    private final String name;
    private final ClassLoader loader;
    private final Class<? extends T> extClass;
    private final Method initMethod;
    private final Method newMethod;

    public Extension(Class<? extends T> clazz) {
        this(null, clazz);
    }

    public Extension(String name, Class<? extends T> clazz) {
        this.loader = Thread.currentThread().getContextClassLoader();
        this.extClass = Objects.requireNonNull(clazz);

        String extName = this.extClass.getSimpleName();
        try {
            Field f = this.extClass.getDeclaredField(EXTENSION_NAME);
            if (f != null && String.class.equals(f.getType())) {
                String declaredName = (String) f.get(null);
                if (declaredName != null) {
                    extName = declaredName;
                }
            }
        } catch (Exception e) {
            if (log.isTraceEnabled()) {
                log.trace("Extension [{}] does not have [{}] declared, use [{}] as its name instead", clazz,
                        EXTENSION_NAME, extName);
            }
        }
        this.name = name == null ? extName : name;

        Method m = null;
        try {
            m = this.extClass.getDeclaredMethod(METHOD_INITIALIZE, ExtensionManager.class);
        } catch (Exception e) {
            if (log.isTraceEnabled()) {
                log.trace("Extension [{}] does not have static method for initialization.", clazz);
            }
        }
        this.initMethod = m;

        try {
            m = this.extClass.getDeclaredMethod(METHOD_NEW_INSTANCE, Object[].class);
        } catch (Exception e) {
            if (log.isTraceEnabled()) {
                log.trace("Extension [{}] does not have static method for instantiation.", clazz);
            }
        }
        this.newMethod = m;
    }

    /**
     * Get name of the extension. It's either from {@code EXTENSION_NAME}(static
     * member of the extension class) or simple name of the class.
     * 
     * @return name of the extension
     */
    public String getName() {
        return this.name;
    }

    /**
     * Get class of the extension. Note that this is different from
     * {@link #getClass()}.
     * 
     * @return class of the extension
     */
    public Class<? extends T> getProviderClass() {
        return this.extClass;
    }

    /**
     * Load a specific class.
     * 
     * @param className class name
     * @return desired class
     */
    public Class<?> loadClass(String className) {
        Class<?> clazz = null;

        ClassLoader loader = this.loader == null ? getClass().getClassLoader() : this.loader;
        try {
            clazz = loader.loadClass(className);
        } catch (ClassNotFoundException e) {
            log.warn("Not able to load class: " + className);
        } catch (Exception e) {
            log.warn("Failed to load class: " + className, e);
        }

        return clazz;
    }

    /**
     * Initialize the extension. This will be only called once at startup of the
     * application.
     * 
     * @param manager extension manager
     */
    public void initialize(ExtensionManager manager) {
        if (this.initMethod == null) {
            return;
        }

        try {
            this.initMethod.invoke(null, manager);
        } catch (Exception e) {
            if (log.isTraceEnabled() || log.isDebugEnabled()) {
                log.debug("Failed to initialize extension: " + extClass, e);
            } else {
                log.warn("Failed to initialize extension [{}]", extClass);
            }
        }
    }

    /**
     * Create a new instance of the extension.
     * 
     * @param args list of arguments for instantiation
     * @return new instance of the extension
     * @throws UnsupportedOperationException if no static {@code newInstance} method
     *                                       and suitable constructor for
     *                                       instantiation
     * @throws IllegalArgumentException      if failed to create new instance using
     *                                       given arguments
     */
    @SuppressWarnings("unchecked")
    public T newInstance(Object... args) {
        final Thread current = Thread.currentThread();
        final ClassLoader currentLoader = current.getContextClassLoader();

        current.setContextClassLoader(loader);

        try {
            if (this.newMethod == null) {
                int argsLength = args == null ? 0 : args.length;

                // fallback to any matched constructor
                Constructor<?> matchedConstructor = null;
                for (Constructor<?> c : this.extClass.getDeclaredConstructors()) {
                    Class<?>[] paramTypes = c.getParameterTypes();
                    if (paramTypes.length == argsLength) {
                        boolean matched = true;

                        for (int i = 0; i < argsLength; i++) {
                            Class<?> clazz = paramTypes[i];
                            Object arg = args[i];

                            if (clazz.isPrimitive()) {
                                if (arg == null || (Byte.TYPE.equals(clazz) && !Byte.class.isInstance(arg))
                                        || (Short.TYPE.equals(clazz) && !Short.class.isInstance(arg))
                                        || (Integer.TYPE.equals(clazz) && !Integer.class.isInstance(arg))
                                        || (Long.TYPE.equals(clazz) && !Long.class.isInstance(arg))
                                        || (Float.TYPE.equals(clazz) && !Float.class.isInstance(arg))
                                        || (Double.TYPE.equals(clazz) && !Double.class.isInstance(arg))
                                        || (Boolean.TYPE.equals(clazz) && !Boolean.class.isInstance(arg))
                                        || (Character.TYPE.equals(clazz) && !Character.class.isInstance(arg))) {
                                    matched = false;
                                    break;
                                }
                            } else if (arg != null && !clazz.isInstance(arg)) {
                                matched = false;
                                break;
                            }
                        }

                        if (matched) {
                            matchedConstructor = c;
                            break;
                        }
                    }
                }

                if (matchedConstructor == null) {
                    throw new UnsupportedOperationException("Instantiation not supported for extension: " + extClass);
                } else {
                    try {
                        return (T) matchedConstructor.newInstance(args);
                    } catch (Exception e) {
                        Throwable rootCause = e.getCause();
                        throw new IllegalArgumentException("Failed to create instance from extension: " + extClass,
                                rootCause == null ? e : rootCause);
                    }
                }
            }

            try {
                return (T) this.newMethod.invoke(null, new Object[] { args });
            } catch (Exception e) {
                Throwable rootCause = e.getCause();
                throw new IllegalStateException("Failed to create instance from extension: " + extClass,
                        rootCause == null ? e : rootCause);
            }
        } finally {
            current.setContextClassLoader(currentLoader);
        }
    }
}
