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
package ml.shifu.guagua.coordinator.zk;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import ml.shifu.guagua.util.FileUtils;

import org.apache.zookeeper.server.quorum.QuorumPeerMain;

/**
 * {@link ZooKeeperUtils} is a helper used to start embed zookeeper server in CLI host.
 * 
 * <p>
 * For big data guagua application, independent zookeeper instance is recommended, embed server is for user easy to use
 * guagua if there is no zookeeper server in hand.
 */
public final class ZooKeeperUtils {

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
        for(int i = DEFAULT_ZK_PORT; i < (DEFAULT_ZK_PORT + TRY_PORT_COUNT); i++) {
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
                file.delete();
            }
            file.createNewFile();

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

        return i == RETRY_COUNT;
    }

    /**
     * Create a folder with folder name, if exist, delete it firstly.
     */
    private static void createFolder(String folder) {
        File file = new File(folder);
        if(file.exists()) {
            file.delete();
        }
        file.mkdir();
    }

    /**
     * Create zookeeper file with specified name and client port setting.
     */
    public static void prepZooKeeperConf(String fileName, String clientPort) {
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
    public static int startEmbedZooKeeper() {
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
                }
                if(isServerAlive(host, Integer.parseInt(port))) {
                    return true;
                }
            }
        }
        return false;
    }
}
