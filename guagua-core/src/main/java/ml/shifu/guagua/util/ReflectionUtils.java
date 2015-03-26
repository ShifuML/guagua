/*
 * Copyright [2013-2014] eBay Software Foundation
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ml.shifu.guagua.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import ml.shifu.guagua.GuaguaRuntimeException;

/**
 * {@link ReflectionUtils} is used to get instance from java reflection mechanism.
 * 
 * <p>
 * The class should have default constructor for getting instance.
 * 
 * <p>
 * {@link Constructor}s are cached but instances are not cached. Each time you will get a new instance.
 */
public final class ReflectionUtils {

    /** Do not instantiate. */
    private ReflectionUtils() {
    }

    /**
     * Only support constructors with no parameters.
     */
    private static final Class<?>[] EMPTY_ARRAY = new Class[] {};

    /**
     * This map is used for cache
     */
    private static final Map<Class<?>, Constructor<?>> CONSTRUCTOR_CACHE = new ConcurrentHashMap<Class<?>, Constructor<?>>();

    /**
     * Create an object for the given class. The class should have constructor without any parameters.
     * 
     * @param clazz
     *            class of which an object is created
     * @return a new object
     * @throws GuaguaRuntimeException
     *             In case any exception for reflection.
     */
    public static <T> T newInstance(Class<T> clazz) {
        T result;
        try {
            @SuppressWarnings("unchecked")
            Constructor<T> meth = (Constructor<T>) CONSTRUCTOR_CACHE.get(clazz);
            if(meth == null) {
                meth = clazz.getDeclaredConstructor(EMPTY_ARRAY);
                meth.setAccessible(true);
                CONSTRUCTOR_CACHE.put(clazz, meth);
            }
            result = meth.newInstance();
        } catch (Exception e) {
            throw new GuaguaRuntimeException(e);
        }
        return result;
    }

    /**
     * Create an object for the given class. The class should have constructor without any parameters.
     * 
     * @param name
     *            qualified class name.
     * @return a new object
     * @throws GuaguaRuntimeException
     *             In case any exception for reflection.
     */
    public static <T> T newInstance(String name) {
        try {
            @SuppressWarnings("unchecked")
            Class<T> clazz = (Class<T>) Class.forName(name);
            return newInstance(clazz);
        } catch (Exception e) {
            throw new GuaguaRuntimeException(e);
        }
    }

    /**
     * Check if there is empty-parameter constructor in one {@code clazz}.
     */
    public static boolean hasEmptyParameterConstructor(Class<?> clazz) {
        try {
            Constructor<?> met = clazz.getDeclaredConstructor(EMPTY_ARRAY);
            return met != null;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Just to check if has method with methodName. This function only supports functions without parameters.
     */
    public static Method getMethod(Class<?> clazz, String methodName) {
        for(Method method: clazz.getDeclaredMethods()) {
            if(method.getName().equals(methodName)) {
                return method;
            }
        }
        return null;
    }
}
