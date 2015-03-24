/*
 * Copyright [2013-2015] eBay Software Foundation
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
package ml.shifu.guagua.hadoop;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;

import ml.shifu.guagua.GuaguaConstants;
import ml.shifu.guagua.GuaguaRuntimeException;
import ml.shifu.guagua.coordinator.zk.ZooKeeperUtils;
import ml.shifu.guagua.io.Bytable;
import ml.shifu.guagua.master.BasicMasterInterceptor;
import ml.shifu.guagua.master.MasterContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * To start zookeeper instance in cluster environment.
 * 
 * <p>
 * Compare with start zookeeper instance in client process, {@link ZooKeeperMasterInterceptor} can be used in cluster
 * and even to start zookeeper ensemble in cluster to void single point failure issue.
 */
public class ZooKeeperMasterInterceptor<MASTER_RESULT extends Bytable, WORKER_RESULT extends Bytable> extends
        BasicMasterInterceptor<MASTER_RESULT, WORKER_RESULT> {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperMasterInterceptor.class);

    /**
     * FileSystem instance to store zookeeper server info into hdfs.
     */
    private FileSystem fileSystem;

    /**
     * Zookeeper server file path to store server address and port info.
     */
    private Path zookeeperServerPath;

    /**
     * Do we need to start zookeeper instance in cluster env.
     */
    private boolean isNeedStartZookeeper = false;

    @Override
    public void preApplication(MasterContext<MASTER_RESULT, WORKER_RESULT> context) {
        String zkServers = context.getProps().getProperty(GuaguaConstants.GUAGUA_ZK_SERVERS);
        if(zkServers == null || zkServers.length() == 0 || !ZooKeeperUtils.checkServers(zkServers)) {
            this.isNeedStartZookeeper = true;
            if(fileSystem == null) {
                try {
                    fileSystem = FileSystem.get(new Configuration());
                } catch (IOException e) {
                    throw new GuaguaRuntimeException(e);
                }
            }
            String localHostName = getLocalHostName();
            LOG.warn("No valid zookeeper servers, start one in ZooKeeperMaster {}", localHostName);
            // 1. start embed zookeeper server in one thread.
            String zkJavaOpts = context.getProps().getProperty(GuaguaConstants.GUAGUA_CHILD_ZKSERVER_OPTS,
                    GuaguaConstants.GUAGUA_CHILD_DEFAULT_ZKSERVER_OPTS);
            String zookeeperServer;
            try {
                zookeeperServer = ZooKeeperUtils.startChildZooKeeperProcess(zkJavaOpts);
            } catch (IOException e) {
                LOG.error("Error in start child zookeeper process.", e);
                // set to null to try start zookeeper with tread.
                zookeeperServer = null;
            }

            if(zookeeperServer == null) {
                // if started failed
                zookeeperServer = startZookeeperServer(localHostName);
                LOG.info("Zookeeper server is stated with thread: {}", zookeeperServer);
            } else {
                LOG.info("Zookeeper server is stated with child process: {}", zookeeperServer);
            }

            // 2. write such server info to HDFS file, no any place for us to communicate server address with worker.
            writeServerInfoToHDFS(context, zookeeperServer);

            // 3. set server info to context for next intercepters.
            context.getProps().setProperty(GuaguaConstants.GUAGUA_ZK_SERVERS, zookeeperServer);
        }
    }

    /**
     * Write zookeeper server info to HDFS. Then worker can get such info and connect to such server..
     */
    private void writeServerInfoToHDFS(MasterContext<MASTER_RESULT, WORKER_RESULT> context,
            String embededZooKeeperServer) {
        String hdfsZookeeperServerFolder = getZookeeperServerFolder(context);
        PrintWriter pw = null;
        try {
            if(!this.fileSystem.exists(new Path(hdfsZookeeperServerFolder))) {
                this.fileSystem.mkdirs(new Path(hdfsZookeeperServerFolder));
            }
            this.zookeeperServerPath = fileSystem.makeQualified(new Path(hdfsZookeeperServerFolder,
                    GuaguaConstants.GUAGUA_CLUSTER_ZOOKEEPER_SERVER_FILE));
            LOG.info("Writing hdfs zookeeper server info to {}", this.zookeeperServerPath);
            FSDataOutputStream fos = fileSystem.create(this.zookeeperServerPath);
            pw = new PrintWriter(fos);
            pw.println(embededZooKeeperServer);
            pw.flush();
        } catch (IOException e) {
            LOG.error("Error in writing output.", e);
        } catch (Exception e) {
            LOG.error("Error in writing output.", e);
        } finally {
            IOUtils.closeStream(pw);
        }
    }

    private String getZookeeperServerFolder(MasterContext<MASTER_RESULT, WORKER_RESULT> context) {
        String defaultZooKeeperServePath = new StringBuilder(200).append("tmp").append(Path.SEPARATOR)
                .append("_guagua").append(Path.SEPARATOR).append(context.getAppId()).append(Path.SEPARATOR).toString();
        String hdfsZookeeperServerPath = context.getProps().getProperty(
                GuaguaConstants.GUAGUA_ZK_CLUSTER_SERVER_FOLDER, defaultZooKeeperServePath);
        return hdfsZookeeperServerPath;
    }

    @Override
    public void postApplication(MasterContext<MASTER_RESULT, WORKER_RESULT> context) {
        if(this.isNeedStartZookeeper) {
            try {
                this.fileSystem.delete(new Path(this.getZookeeperServerFolder(context)), true);
            } catch (IOException e) {
                throw new GuaguaRuntimeException(e);
            }
        }
    }

    /**
     * Start zookeeper server in thread of master node.
     */
    private String startZookeeperServer(String localHostName) {
        int embedZkClientPort = 0;
        try {
            embedZkClientPort = ZooKeeperUtils.startEmbedZooKeeper();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // 2. check if it is started.
        ZooKeeperUtils.checkIfEmbedZooKeeperStarted(embedZkClientPort);
        return localHostName + ":" + embedZkClientPort;
    }

    private String getLocalHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            throw new GuaguaRuntimeException(e);
        }
    }

}
