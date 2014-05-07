/**
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
package ml.shifu.guagua.yarn.util;

import com.google.gson.Gson;

/**
 * {@link GsonUtils} is used for Object-JSON JSON-Object conversion.
 * 
 * <p>
 * To reduce new operations, only one {@link Gson} instance in this class.
 */
public final class GsonUtils {

    // To avoid somebody new GsonUtils
    private GsonUtils() {
    }

    private static Gson gson = new Gson();

    /**
     * Serialize an object to JSON-format String.
     */
    public static String toJson(Object src) {
        return gson.toJson(src);
    }

    /**
     * De-serialize JSON-format String to an object.
     */
    public static <T> T fromJson(String json, Class<T> c) {
        return gson.fromJson(json, c);
    }

}
