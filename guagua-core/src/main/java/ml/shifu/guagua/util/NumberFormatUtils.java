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

/**
 * Util class to parse int, long or any other type.
 */
public final class NumberFormatUtils {

    /** Do not instantiate. */
    private NumberFormatUtils() {
    }

    /**
     * Parse string object to int type.
     * 
     * @param str
     *            string needed to be parsed
     * @param defaultValue
     *            default value
     */
    public static int getInt(String str, int defaultValue) {
        return getInt(str, false, defaultValue);
    }

    /**
     * Parse string object to int type. If invalid format, use 0 to replace.
     * 
     * @param str
     *            string needed to be parsed
     */
    public static int getInt(String str) {
        return getInt(str, false, 0);
    }

    /**
     * Parse string object to int type.
     * 
     * @param str
     *            string needed to be parsed
     * @param required
     *            if required, should be parsed successfully without default value to replace; else use 0 as default
     *            value.
     * @throws Exception
     *             if invalid format with required set 'true'
     */
    public static int getInt(String str, boolean required) {
        return getInt(str, required, 0);
    }

    /**
     * Parse string object to int type.
     * 
     * @param str
     *            string needed to be parsed
     * @param required
     *            if required, should be parsed successfully without default value to replace; else use default value.
     * @param defaultValue
     *            default value
     * @throws IllegalArgumentException
     *             if invalid format with required set 'true'
     */
    public static int getInt(String str, boolean required, int defaultValue) {
        try {
            return Integer.valueOf(str);
        } catch (Exception e) {
            if(required) {
                throw new IllegalArgumentException("Not a valid input", e);
            } else {
                return defaultValue;
            }
        }
    }

    /**
     * Parse string object to long type.
     * 
     * @param str
     *            string needed to be parsed
     * @param defaultValue
     *            default value
     */
    public static long getLong(String str, long defaultValue) {
        return getLong(str, false, defaultValue);
    }

    /**
     * Parse string object to long type. If invalid format, use 0 to replace.
     * 
     * @param str
     *            string needed to be parsed
     */
    public static long getLong(String str) {
        return getLong(str, false, 0);
    }

    /**
     * Parse string object to long type. If invalid format, use 0 to replace.
     * 
     * @param str
     *            string needed to be parsed
     * @param required
     *            if required, should be parsed successfully without default value to replace
     * @throws IllegalArgumentException
     *             if invalid format with required set 'true'
     */
    public static long getLong(String str, boolean required) {
        return getLong(str, required, 0);
    }

    /**
     * Parse string object to long type.
     * 
     * @param str
     *            string needed to be parsed
     * @param required
     *            if required, should be parsed successfully without default value to replace; else use default value.
     * @param defaultValue
     *            default value
     * @throws Exception
     *             if invalid format with required set 'true'
     */
    public static long getLong(String str, boolean required, long defaultValue) {
        try {
            return Long.valueOf(str);
        } catch (Exception e) {
            if(required) {
                throw new IllegalArgumentException("Not a valid input", e);
            } else {
                return defaultValue;
            }
        }
    }

    /**
     * Parse string object to double type.
     * 
     * @param str
     *            string needed to be parsed
     * @param defaultValue
     *            default value
     */
    public static double getDouble(String str, double defaultValue) {
        return getDouble(str, false, defaultValue);
    }

    /**
     * Parse string object to double type. If invalid format, use 0 to replace.
     * 
     * @param str
     *            string needed to be parsed
     */
    public static double getDouble(String str) {
        return getDouble(str, false, 0.0d);
    }

    /**
     * Parse string object to double type. If invalid format, use 0 to replace.
     * 
     * @param str
     *            string needed to be parsed
     * @param required
     *            if required, should be parsed successfully without default value to replace
     * @throws Exception
     *             if invalid format with required set 'true'
     */
    public static double getDouble(String str, boolean required) {
        return getDouble(str, required, 0.0d);
    }

    /**
     * Parse string object to double type.
     * 
     * @param str
     *            string needed to be parsed
     * @param required
     *            if required, should be parsed successfully without default value to replace; else use default value.
     * @param defaultValue
     *            default value
     * @throws IllegalArgumentException
     *             if invalid format with required set 'true'
     */
    public static double getDouble(String str, boolean required, double defaultValue) {
        try {
            return Double.valueOf(str);
        } catch (Exception e) {
            if(required) {
                throw new IllegalArgumentException("Not a valid input", e);
            } else {
                return defaultValue;
            }
        }
    }

}
