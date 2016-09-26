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
package ml.shifu.guagua.coordinator.zk;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.URL;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import ml.shifu.guagua.GuaguaRuntimeException;
import ml.shifu.guagua.util.FileUtils;

import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.Log4jLoggerAdapter;

/**
 * {@link ZooKeeperUtils} is a helper used to start embed zookeeper server in CLI host.
 * 
 * <p>
 * For big data guagua application, independent zookeeper instance is recommended, embed server is for user easy to use
 * guagua if there is no zookeeper server in hand.
 */
public final class ZooKeeperUtils {

    private final static Logger LOG = LoggerFactory.getLogger(ZooKeeperUtils.class);

    private static final int DEFAULT_ZK_PORT = 2181;

    /**
     * Client connection retry count
     */
    public static final int RETRY_COUNT = 4;

    /**
     * How many ports will be used to launch embed zookeeper server.
     */
    public static final int TRY_PORT_COUNT = 20;

    /**
     * Initial zookeeper port used for check valid zookeeper port.
     */
    public static final int INITAL_ZK_PORT = -1;

    private static final Random RANDOM = new Random();

    /** Do not instantiate. */
    private ZooKeeperUtils() {
    }

    /**
     * Check from DEFAULT_ZK_PORT to (DEFAULT_ZK_PORT + TRY_PORT_COUNT) to see if there is valid port to launch embed
     * zookeeper server.
     * 
     * @return valid zookeeper port
     */
    public static int getValidZooKeeperPort() {
        int zkValidPort = INITAL_ZK_PORT;
        int initialPort = DEFAULT_ZK_PORT;
        // add random port to avoid port in the same small range.
        if(System.currentTimeMillis() % 2 == 0) {
            zkValidPort += RANDOM.nextInt(100);
        } else {
            zkValidPort -= RANDOM.nextInt(100);
        }
        for(int i = initialPort; i < (initialPort + TRY_PORT_COUNT); i++) {
            try {
                if(!isServerAlive(InetAddress.getLocalHost(), i)) {
                    zkValidPort = i;
                    break;
                }
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        }
        if(zkValidPort == INITAL_ZK_PORT) {
            throw new RuntimeException(
                    "Too many ports are used, please submit guagua app later or specify one zookeeper instance.");
        }
        return zkValidPort;
    }

    /**
     * Create zoo.cfg file to launch a embed zookeeper server.
     * 
     * @param fileName
     *            zookeeper conf file name
     * @param props
     *            key-value pairs for zookeeper configurations
     */
    public static void populateZooKeeperConfFile(String fileName, Map<String, String> props) {
        OutputStreamWriter writer = null;
        try {
            File file = new File(fileName);
            if(file.exists()) {
                FileUtils.deleteDirectory(file);
            }

            if(!file.createNewFile()) {
                throw new IllegalStateException("Error to create new file " + fileName);
            }

            OutputStream outputStream = new FileOutputStream(file);
            writer = new OutputStreamWriter(outputStream, "utf-8");

            for(Map.Entry<String, String> entry: props.entrySet()) {
                writer.write(entry.getKey() + "=" + entry.getValue());
                writer.write(System.getProperty("line.separator"));
            }
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if(writer != null) {
                try {
                    writer.close();
                } catch (IOException ignore) {
                }
            }
        }
    }

    /**
     * Check whether a server is alive.
     * 
     * @param host
     *            the server host
     * @param port
     *            the server port
     * @return true if a server is alive, false if a server is not alive.
     */
    public static boolean isServerAlive(InetAddress host, int port) {
        Socket socket = null;
        int i = 0;
        while(i < RETRY_COUNT) {
            try {
                socket = new Socket(host, port);
                break;
            } catch (IOException e) {
                i++;
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                }
            } finally {
                try {
                    if(socket != null) {
                        socket.close();
                    }
                } catch (IOException ignore) {
                }
            }
        }

        return i != RETRY_COUNT;
    }

    /**
     * Check whether a server is alive.
     * 
     * @param host
     *            the server host
     * @param port
     *            the server port
     * @return true if a server is alive, false if a server is not alive.
     */
    public static boolean isServerAlive(String host, int port) {
        Socket socket = null;
        int i = 0;
        while(i++ < RETRY_COUNT) {
            try {
                socket = new Socket(host, port);
                break;
            } catch (IOException e) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                }
            } finally {
                try {
                    if(socket != null) {
                        socket.close();
                    }
                } catch (IOException ignore) {
                }
            }
        }

        return i < RETRY_COUNT;
    }

    /**
     * Create a folder with folder name, if exist, delete it firstly.
     */
    private static void createFolder(String folder) throws IOException {
        File file = new File(folder);
        if(file.exists()) {
            FileUtils.deleteDirectory(file);
        }
        if(!file.mkdir()) {
            throw new IllegalStateException("Error to mkdir for folder " + folder);
        }
    }

    /**
     * Create zookeeper file with specified name and client port setting.
     */
    public static void prepZooKeeperConf(String fileName, String clientPort) throws IOException {
        Map<String, String> props = new HashMap<String, String>();
        String dataDir = getZooKeeperWorkingDir() + File.separator + "zkdata";
        createFolder(dataDir);
        String dataLogDir = getZooKeeperWorkingDir() + File.separator + "zklog";
        createFolder(dataLogDir);
        props.put("tickTime", "2000");
        props.put("initLimit", "10");
        props.put("syncLimit", "5");
        props.put("dataDir", dataDir);
        props.put("dataLogDir", dataLogDir);
        props.put("clientPort", clientPort);
        props.put("minSessionTimeout", "10000");
        props.put("maxSessionTimeout", "30000000");
        // The number of snapshots to retain in dataDir
        props.put("autopurge.snapRetainCount", "3");
        // Purge task interval in hours set to "0" to disable auto purge feature
        props.put("autopurge.purgeInterval", "1");
        populateZooKeeperConfFile(fileName, props);
    }

    /**
     * Retrieve user working folder.
     */
    private static String getUserDir() {
        return System.getProperty("user.dir");
    }

    /**
     * Retrieve zookeeper working folder.
     */
    private static String getZooKeeperWorkingDir() {
        return getUserDir() + File.separator + "zookeeper";
    }

    /**
     * Wait for a time period until zookeeper server is started.
     * 
     * @param embedZkClientPort
     *            the valid zookeeper client port
     */
    public static void checkIfEmbedZooKeeperStarted(int embedZkClientPort) {
        try {
            int i = 0;
            while(i++ < RETRY_COUNT && !isServerAlive(InetAddress.getLocalHost(), embedZkClientPort)) {
                try {
                    Thread.sleep(10 * 1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                System.err.println("INFO: Waiting embed server to start.");
            }

            if(i == RETRY_COUNT) {
                throw new RuntimeException("Exception to start embed, please specified zookeeper server by -z");
            }

        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Start embed zookeeper server in a daemon thread.
     */
    public static int startEmbedZooKeeper() throws IOException {
        final String zooKeeperWorkingDir = getZooKeeperWorkingDir();
        createFolder(zooKeeperWorkingDir);

        final String confName = zooKeeperWorkingDir + File.separator + "zoo.cfg";
        int validZkPort = getValidZooKeeperPort();

        prepZooKeeperConf(confName, validZkPort + "");

        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                QuorumPeerMain.main(new String[] { confName });
            }
        }, "Embed ZooKeeper");

        thread.setDaemon(true);
        thread.start();

        // used local data should be cleaned.
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    FileUtils.deleteDirectory(new File(zooKeeperWorkingDir));
                } catch (IOException ignore) {
                }
            }
        }));

        return validZkPort;
    }

    /**
     * Start embed zookeeper server in a child process.
     * 
     * @return null if start child process failed, non empty string if valid zookeeper server in child.
     */
    public static String startChildZooKeeperProcess(String zkJavaOpts) throws IOException {
        // 1. prepare working dir
        final String zooKeeperWorkingDir = getZooKeeperWorkingDir();
        createFolder(zooKeeperWorkingDir);
        // 2. prepare conf file
        final String confName = zooKeeperWorkingDir + File.separator + "zoo.cfg";
        int validZkPort = getValidZooKeeperPort();
        prepZooKeeperConf(confName, validZkPort + "");
        // 3. prepare process buider command
        ProcessBuilder processBuilder = new ProcessBuilder();
        List<String> commandList = new ArrayList<String>();
        String javaHome = System.getProperty("java.home");
        if(javaHome == null) {
            throw new IllegalArgumentException("java.home is not set!");
        }

        commandList.add(javaHome + "/bin/java");
        String[] zkJavaOptsArray = zkJavaOpts.split(" ");
        if(zkJavaOptsArray != null) {
            commandList.addAll(Arrays.asList(zkJavaOptsArray));
        }
        commandList.add("-cp");
        commandList.add(findContainingJar(Log4jLoggerAdapter.class) + ":" + findContainingJar(Logger.class) + ":"
                + findContainingJar(org.apache.log4j.Logger.class) + ":" + findContainingJar(ZooKeeperUtils.class)
                + ":" + findContainingJar(QuorumPeerMain.class));
        commandList.add(ZooKeeperMain.class.getName());
        commandList.add(confName);
        processBuilder.command(commandList);
        File execDirectory = new File(zooKeeperWorkingDir);
        processBuilder.directory(execDirectory);
        processBuilder.redirectErrorStream(true);

        LOG.info("onlineZooKeeperServers: Attempting to start ZooKeeper server with command {} in directory {}.",
                commandList, execDirectory.toString());
        // 4. start process
        Process zkProcess = null;
        StreamCollector zkProcessCollector;
        synchronized(ZooKeeperUtils.class) {
            zkProcess = processBuilder.start();
            zkProcessCollector = new StreamCollector(zkProcess.getInputStream());
            zkProcessCollector.start();
        }
        Runtime.getRuntime().addShutdownHook(
                new Thread(new ZooKeeperShutdownHook(zkProcess, zkProcessCollector, zooKeeperWorkingDir)));
        LOG.info("onlineZooKeeperServers: Shutdown hook added.");

        // 5. check and wait for server just started.
        String hostname = getLocalHostName();
        if(isServerAlive(hostname, validZkPort)) {
            return hostname + ":" + validZkPort;
        } else {
            return null;
        }
    }

    private static String getLocalHostName() {
        // TODO use cache??
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            throw new GuaguaRuntimeException(e);
        }
    }

    private static class ZooKeeperShutdownHook implements Runnable {

        private Process process;

        private StreamCollector collector;

        private String zooKeeperWorkingDir;

        public ZooKeeperShutdownHook(Process process, StreamCollector collector, String zooKeeperWorkingDir) {
            this.process = process;
            this.collector = collector;
            this.zooKeeperWorkingDir = zooKeeperWorkingDir;
        }

        @Override
        public void run() {
            LOG.info("run: Shutdown hook started.");
            synchronized(this) {
                if(process != null) {
                    LOG.warn("onlineZooKeeperServers: Forced a shutdown hook kill of the ZooKeeper process.");
                    process.destroy();
                    int exitCode = -1;
                    try {
                        exitCode = process.waitFor();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    LOG.info(
                            "onlineZooKeeperServers: ZooKeeper process exited with {} (note that 143 typically means killed).",
                            exitCode);
                }
            }
            this.collector.close();
            FileUtils.deleteQuietly(new File(zooKeeperWorkingDir));
        }

    }

    /**
     * Collects the output of a stream and dumps it to the log.
     */
    private static class StreamCollector extends Thread {
        /** Number of last lines to keep */
        private static final int LAST_LINES_COUNT = 100;
        /** Class logger */
        private static final Logger LOG = LoggerFactory.getLogger(StreamCollector.class);
        /** Buffered reader of input stream */
        private final BufferedReader bufferedReader;
        /** Last lines (help to debug failures) */
        private final LinkedList<String> lastLines = new LinkedList<String>();

        /**
         * Constructor.
         * 
         * @param is
         *            InputStream to dump to LOG.info
         */
        public StreamCollector(final InputStream is) {
            super(StreamCollector.class.getName());
            setDaemon(true);
            InputStreamReader streamReader = new InputStreamReader(is, Charset.defaultCharset());
            bufferedReader = new BufferedReader(streamReader);
        }

        @Override
        public void run() {
            readLines();
        }

        /**
         * Read all the lines from the bufferedReader.
         */
        private synchronized void readLines() {
            String line;
            try {
                while((line = bufferedReader.readLine()) != null) {
                    if(lastLines.size() > LAST_LINES_COUNT) {
                        lastLines.removeFirst();
                    }
                    lastLines.add(line);
                    LOG.info("readLines: {}.", line);
                }
            } catch (IOException e) {
                LOG.error("readLines: Ignoring IOException", e);
            }
        }

        /**
         * Dump the last n lines of the collector. Likely used in the case of failure.
         * 
         * @param level
         *            Log level to dump with
         */
        @SuppressWarnings("unused")
        public synchronized void dumpLastLines() {
            // Get any remaining lines
            readLines();
            // Dump the lines to the screen
            for(String line: lastLines) {
                LOG.info(line);
            }
        }

        public void close() {
            try {
                this.bufferedReader.close();
            } catch (IOException ignore) {
            }
        }

    }

    /**
     * Find a jar that contains a class of the same name, if any. It will return a jar file, even if that is not the
     * first thing on the class path that has a class with the same name.
     * 
     * @param my_class
     *            the class to find
     * @return a jar file that contains the class, or null
     */
    @SuppressWarnings("rawtypes")
    public static String findContainingJar(Class my_class) {
        ClassLoader loader = my_class.getClassLoader();
        String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";
        try {
            for(Enumeration itr = loader.getResources(class_file); itr.hasMoreElements();) {
                URL url = (URL) itr.nextElement();
                if("jar".equals(url.getProtocol())) {
                    String toReturn = url.getPath();
                    if(toReturn.startsWith("file:")) {
                        toReturn = toReturn.substring("file:".length());
                    }
                    // URLDecoder is a misnamed class, since it actually decodes
                    // x-www-form-urlencoded MIME type rather than actual
                    // URL encoding (which the file path has). Therefore it would
                    // decode +s to ' 's which is incorrect (spaces are actually
                    // either unencoded or encoded as "%20"). Replace +s first, so
                    // that they are kept sacred during the decoding process.
                    toReturn = toReturn.replaceAll("\\+", "%2B");
                    toReturn = URLDecoder.decode(toReturn, "UTF-8");
                    return toReturn.replaceAll("!.*$", "");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    /**
     * Check zookeeper servers, if one of them is alive, return true;
     */
    public static boolean checkServers(String servers) {
        String[] serverArray = servers.split(",");
        for(String server: serverArray) {
            if(server != null) {
                server = server.trim();
                String port = null;
                String host = null;
                if(server.indexOf(':') > 0) {
                    String[] hostAndPort = server.split(":");
                    host = hostAndPort[0].trim();
                    port = hostAndPort[1].trim();
                } else {
                    host = server;
                    port = DEFAULT_ZK_PORT + "";
                }
                if(isServerAlive(host, Integer.parseInt(port))) {
                    return true;
                }
            }
        }
        return false;
    }

}
