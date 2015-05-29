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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper to run jmap and print the output. Copy from Apache Giraph.
 */
public class JMap {

    private static final Logger LOG = LoggerFactory.getLogger(JMap.class);

    /** The command to run */
    public static final String CMD = "jmap ";
    /** Arguments to pass in to command */
    public static final String ARGS = " -histo ";

    /** Do not construct */
    protected JMap() {
    }

    /**
     * Get the process ID of the current running process
     * 
     * @return Integer process ID
     */
    public static int getProcessId() {
        String processId = ManagementFactory.getRuntimeMXBean().getName();
        if(processId.contains("@")) {
            processId = processId.substring(0, processId.indexOf("@"));
        }
        return Integer.parseInt(processId);
    }

    /**
     * Run jmap, print numLines of output from it to stderr.
     * 
     * @param numLines
     *            Number of lines to print
     */
    public static void heapHistogramDump(int numLines) {
        heapHistogramDump(numLines, System.err);
    }

    /**
     * Run jmap, print numLines of output from it to stream passed in.
     * 
     * @param numLines
     *            Number of lines to print
     * @param printStream
     *            Stream to print to
     */
    public static void heapHistogramDump(int numLines, PrintStream printStream) {
        BufferedReader in = null;
        try {
            Process p = Runtime.getRuntime().exec(CMD + ARGS + getProcessId());
            in = new BufferedReader(new InputStreamReader(p.getInputStream(), Charset.defaultCharset()));
            printStream.println("JMap histo dump at " + new Date());
            String line = in.readLine();
            for(int i = 0; i < numLines && line != null; ++i) {
                printStream.println("--\t" + line);
                line = in.readLine();
            }
        } catch (IOException e) {
            LOG.error("IOException in dump heap", e);
        } finally {
            if(in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    LOG.error("Error in closing input stream", e);
                }
            }
        }
    }
    
    public static void main(String[] args) {
        Map<String, String> map = new HashMap<String, String>();
        map.put(null, "123");
        System.out.println(map.get(null));
        map.put("123", null);
        System.out.println(map.get("123"));
        System.out.println(map.containsKey("123"));
        System.out.println("-----------------------------------------");
        map = new LinkedHashMap<String, String>();
        map.put(null, "123");
        System.out.println(map.get(null));
        map.put("123", null);
        System.out.println(map.get("123"));
        System.out.println(map.containsKey("123"));
        
        
    }
}
