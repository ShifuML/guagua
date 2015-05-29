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
 * Helper static methods for tracking memory usage. Copy from Apache Giraph.
 */
public final class MemoryUtils {
    
    /** Do not instantiate. */
    private MemoryUtils() {
    }

    /**
     * Get megabytes
     */
    private static double megaBytes(long bytes) {
        return bytes / 1024.0 / 1024.0;
    }

    /**
     * Get total memory in megabytes
     * 
     * @return total memory in megabytes
     */
    public static double totalMemoryMB() {
        return megaBytes(Runtime.getRuntime().totalMemory());
    }

    /**
     * Get maximum memory in megabytes
     * 
     * @return maximum memory in megabytes
     */
    public static double maxMemoryMB() {
        return megaBytes(Runtime.getRuntime().maxMemory());
    }

    /**
     * Get free memory in megabytes
     * 
     * @return free memory in megabytes
     */
    public static double freeMemoryMB() {
        return megaBytes(Runtime.getRuntime().freeMemory());
    }

    /**
     * Get used memory in megabytes
     * 
     * @return free memory in megabytes
     */
    public static double usedMemoryMB() {
        return megaBytes(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
    }

    /**
     * Get runtime memory stats
     * 
     * @return String of all runtime stats.
     */
    public static String getRuntimeMemoryStats() {
        return String.format("Memory (free/used/total/max) = %.2fM / %.2fM / %.2fM / %.2fM", freeMemoryMB(),
                usedMemoryMB(), totalMemoryMB(), maxMemoryMB());
    }
}
