/*
 * Copyright [2013-2015] PayPal Software Foundation
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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

import ml.shifu.guagua.BasicCoordinator.RetryCoordinatorCommand;
import ml.shifu.guagua.GuaguaConstants;
import ml.shifu.guagua.GuaguaRuntimeException;
import ml.shifu.guagua.coordinator.zk.ZooKeeperUtils;
import ml.shifu.guagua.io.Bytable;
import ml.shifu.guagua.util.NumberFormatUtils;
import ml.shifu.guagua.worker.BasicWorkerInterceptor;
import ml.shifu.guagua.worker.WorkerContext;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Follow with {@link ZooKeeperMasterInterceptor}, {@link ZooKeeperWorkerInterceptor} is used to read zookeeper server
 * info from hdfs file.
 */
public class ZooKeeperWorkerInterceptor<MASTER_RESULT extends Bytable, WORKER_RESULT extends Bytable> extends
        BasicWorkerInterceptor<MASTER_RESULT, WORKER_RESULT> {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperWorkerInterceptor.class);

    /**
     * Default waiting time to check master or worker progress from zookeeper servers.
     */
    private static final int WAIT_SLOT_MILLS = 300;

    /**
     * Waiting time to check master or worker progress from zookeeper servers.
     */
    private long sleepTime = WAIT_SLOT_MILLS;

    /**
     * Fixed-time waiting or each time increasing the waiting time.
     */
    private boolean isFixedTime = true;

    @Override
    public void preApplication(WorkerContext<MASTER_RESULT, WORKER_RESULT> context) {
        String zkServers = context.getProps().getProperty(GuaguaConstants.GUAGUA_ZK_SERVERS);
        if(zkServers == null || zkServers.length() == 0 || !ZooKeeperUtils.checkServers(zkServers)) {
            this.sleepTime = NumberFormatUtils.getLong(
                    context.getProps().getProperty(GuaguaConstants.GUAGUA_COORDINATOR_SLEEP_UNIT), WAIT_SLOT_MILLS);
            this.isFixedTime = Boolean.TRUE.toString().equalsIgnoreCase(
                    context.getProps().getProperty(GuaguaConstants.GUAGUA_COORDINATOR_FIXED_SLEEP_ENABLE,
                            GuaguaConstants.GUAGUA_COORDINATOR_FIXED_SLEEP));

            String hdfsZookeeperServerFolder = getZookeeperServerFolder(context);
            long start = System.nanoTime();
            while(true) {
                if(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) > 10 * 60 * 1000L) {
                    throw new GuaguaRuntimeException("Cannot get zookeeper server address in 10 minutes.");
                }
                BufferedReader br = null;
                try {
                    final FileSystem fileSystem = FileSystem.get(new Configuration());
                    final Path zookeeperServerPath = fileSystem.makeQualified(new Path(hdfsZookeeperServerFolder,
                            GuaguaConstants.GUAGUA_CLUSTER_ZOOKEEPER_SERVER_FILE));
                    LOG.info("Embeded zookeeper server address is {}", zookeeperServerPath);

                    new RetryCoordinatorCommand(this.isFixedTime, this.sleepTime) {
                        @Override
                        public boolean retryExecution() throws Exception, InterruptedException {
                            return fileSystem.exists(zookeeperServerPath);
                        }
                    }.execute();

                    FSDataInputStream fis = fileSystem.open(zookeeperServerPath);
                    br = new BufferedReader(new InputStreamReader(fis));
                    String zookeeperServer = br.readLine();
                    if(zookeeperServer == null || zookeeperServer.length() == 0) {
                        LOG.warn("Cannot get zookeeper server in {} ", zookeeperServerPath.toString());
                        // retry
                        continue;
                    }
                    // set server info to context for next intercepters.
                    LOG.info("Embeded zookeeper instance is {}", zookeeperServer);
                    context.getProps().setProperty(GuaguaConstants.GUAGUA_ZK_SERVERS, zookeeperServer);
                    break;
                } catch (Throwable t) {
                    LOG.warn(String.format("Error in get zookeeper address message: %s", t.getMessage()));
                    continue;
                } finally {
                    IOUtils.closeQuietly(br);
                }
            }
        }
    }

    // TODO merge this function together with the one in worker
    private String getZookeeperServerFolder(WorkerContext<MASTER_RESULT, WORKER_RESULT> context) {
        String defaultZooKeeperServePath = new StringBuilder(200).append("tmp").append(Path.SEPARATOR)
                .append("_guagua").append(Path.SEPARATOR).append(context.getAppId()).append(Path.SEPARATOR).toString();
        String hdfsZookeeperServerPath = context.getProps().getProperty(
                GuaguaConstants.GUAGUA_ZK_CLUSTER_SERVER_FOLDER, defaultZooKeeperServePath);
        return hdfsZookeeperServerPath;
    }

}
