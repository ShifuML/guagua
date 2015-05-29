/*
 * Copyright [2013-2014] PayPal Software Foundation
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
 * StringUtils class for string operation.
 */
public final class StringUtils {

    /** Do not instantiate. */
    private StringUtils() {
    }

    /**
     * Return string object. If empty return null..
     */
    public static String get(String str) {
        return get(str, false, null);
    }

    /**
     * Return string object. If {@code requireNotEmpty} is true with empty or null {@code str}, an
     * IllegalArgumentException will be thrown. Else return default string as null or original string.
     */
    public static String get(String str, boolean requireNotEmpty) {
        return get(str, requireNotEmpty, null);
    }

    /**
     * Return string object. Return default string if empty or null.
     */
    public static String get(String str, String defaultStr) {
        return get(str, false, defaultStr);
    }

    /**
     * Return string object. If {@code requireNotEmpty} is true with null {@code str}, an IllegalArgumentException will
     * be thrown. Else return default string or original string.
     */
    public static String get(String str, boolean requireNotEmpty, String defaultStr) {
        if(str == null || str.length() == 0) {
            if(requireNotEmpty) {
                throw new IllegalArgumentException("String should not be empty.");
            }
            return defaultStr;
        }
        return str;
    }

}
