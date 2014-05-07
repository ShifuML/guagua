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
package ml.shifu.guagua.worker;

import java.util.concurrent.TimeUnit;

import ml.shifu.guagua.GuaguaConstants;
import ml.shifu.guagua.io.Bytable;
import ml.shifu.guagua.master.MasterComputable;
import ml.shifu.guagua.master.SyncMasterCoordinator;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link SyncWorkerCoordinator} is used to as a worker barrier for each iteration.
 * 
 * <p>
 * For each iteration, {@link SyncWorkerCoordinator} will wait until master's signal.
 * 
 * <p>
 * To start a new iteration, {@link SyncMasterCoordinator} will write a znode for each iteration like
 * '/_guagua/job_201312041304_189025/master/{currentIteration}' with with {@link MasterComputable} result as its data.
 * {@link SyncWorkerCoordinator} is trying to detect whether it exists, if yes, to start a new iteration.
 * 
 * <p>
 * Worker result will be written into each worker iteration znode for master to get.
 * 
 * @param <MASTER_RESULT>
 *            master result for computation in each iteration.
 * @param <WORKER_RESULT>
 *            worker result for computation in each iteration.
 */
public class SyncWorkerCoordinator<MASTER_RESULT extends Bytable, WORKER_RESULT extends Bytable> extends
        AbstractWorkerCoordinator<MASTER_RESULT, WORKER_RESULT> {

    private static final Logger LOG = LoggerFactory.getLogger(SyncWorkerCoordinator.class);

    @Override
    public void preApplication(final WorkerContext<MASTER_RESULT, WORKER_RESULT> context) {
        // initialize zookeeper and other props
        initialize(context.getProps());

        new FailOverCoordinatorCommand(context).execute();

        new BasicCoordinatorCommand() {
            @Override
            public void doExecute() throws KeeperException, InterruptedException {
                String appId = context.getAppId();
                int currentIteration = context.getCurrentIteration();
                String containerId = context.getContainerId();

                final String appMasterNode = getCurrentMasterNode(appId, currentIteration).toString();
                // create worker init znode.
                Stat stat = null;
                String znode = null;
                try {
                    znode = getRootNode().toString();
                    stat = getZooKeeper().exists(znode, false);
                    if(stat == null) {
                        getZooKeeper().createExt(znode, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, false);
                    }
                } catch (KeeperException.NodeExistsException e) {
                    LOG.warn("Has such node:{}", znode);
                }
                try {
                    znode = getAppNode(appId).toString();
                    stat = getZooKeeper().exists(znode, false);
                    if(stat == null) {
                        getZooKeeper().createExt(znode, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, false);
                    }
                } catch (KeeperException.NodeExistsException e) {
                    LOG.warn("Has such node:{}", znode);
                }
                try {
                    znode = getWorkerBaseNode(appId).toString();
                    stat = getZooKeeper().exists(znode, false);
                    if(stat == null) {
                        getZooKeeper().createExt(znode, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, false);
                    }
                } catch (KeeperException.NodeExistsException e) {
                    LOG.warn("Has such node:{}", znode);
                }
                try {
                    znode = getWorkerBaseNode(appId, currentIteration).toString();
                    stat = getZooKeeper().exists(znode, false);
                    if(stat == null) {
                        getZooKeeper().createExt(znode, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, false);
                    }
                } catch (KeeperException.NodeExistsException e) {
                    LOG.warn("Has such node:{}", znode);
                }

                try {
                    znode = getCurrentWorkerNode(appId, containerId, currentIteration).toString();
                    getZooKeeper().createExt(znode, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, false);
                } catch (KeeperException.NodeExistsException e) {
                    LOG.warn("Has such node:{}", znode);
                }
                // check whether master is ok to start iterations.
                new RetryCoordinatorCommand(isFixedTime(), getSleepTime()) {
                    @Override
                    public boolean retryExecution() throws KeeperException, InterruptedException {
                        try {
                            return getZooKeeper().exists(appMasterNode, false) != null;
                        } catch (KeeperException.NoNodeException e) {
                            // to avoid log flood
                            if(System.nanoTime() % 10 == 0) {
                                LOG.warn("No such node:{}", appMasterNode);
                            }
                            return false;
                        }
                    }
                }.execute();

                if(context.getCurrentIteration() != GuaguaConstants.GUAGUA_INIT_STEP) {
                    final String appMasterSplitNode = getCurrentMasterSplitNode(appId, currentIteration).toString();
                    setMasterResult(context, appMasterNode, appMasterSplitNode);
                }
                LOG.info("Master initilization is done.");
            }
        }.execute();
    }

    @Override
    public void postIteration(final WorkerContext<MASTER_RESULT, WORKER_RESULT> context) {
        new BasicCoordinatorCommand() {
            @Override
            public void doExecute() throws KeeperException, InterruptedException {
                String appId = context.getAppId();
                String containerId = context.getContainerId();
                int currentIteration = context.getCurrentIteration();
                final String appMasterNode = getCurrentMasterNode(appId, currentIteration).toString();
                String appWorkerNode = getCurrentWorkerNode(appId, containerId, currentIteration).toString();
                final String appWorkerSplitNode = getCurrentWorkerSplitNode(appId, containerId, currentIteration)
                        .toString();
                // create worker iteration znode, set app worker znode to EPHEMERAL to save znode resources.
                boolean isSplit = false;
                try {
                    byte[] bytes = getWorkerSerializer().objectToBytes(context.getWorkerResult());
                    isSplit = setBytesToZNode(appWorkerNode, appWorkerSplitNode, bytes, CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException e) {
                    LOG.warn("Has such node:{}", appWorkerNode);
                }

                // remove -1 znode, no needed
                if(context.getCurrentIteration() >= 1) {
                    String znode = getWorkerNode(appId, containerId, currentIteration - 1).toString();
                    try {
                        getZooKeeper().deleteExt(znode, -1, false);
                        if(isSplit) {
                            znode = getCurrentWorkerSplitNode(appId, containerId, currentIteration - 1).toString();
                            getZooKeeper().deleteExt(znode, -1, true);
                        }
                    } catch (KeeperException.NoNodeException e) {
                        if(System.nanoTime() % 20 == 0) {
                            LOG.warn("No such node:{}", znode);
                        }
                    }
                }

                long start = System.nanoTime();
                new RetryCoordinatorCommand(isFixedTime(), getSleepTime()) {
                    @Override
                    public boolean retryExecution() throws KeeperException, InterruptedException {
                        try {
                            return getZooKeeper().exists(appMasterNode, false) != null;
                        } catch (KeeperException.NoNodeException e) {
                            // to avoid log flood
                            if(System.nanoTime() % 10 == 0) {
                                LOG.warn("No such node:{}", appMasterNode);
                            }
                            return false;
                        }
                    }
                }.execute();
                LOG.info("Application {} container {} iteration {} waiting ends with {}ms execution time.",
                        context.getAppId(), context.getContainerId(), context.getCurrentIteration(),
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));

                String appMasterSplitNode = getCurrentMasterSplitNode(appId, currentIteration).toString();
                setMasterResult(context, appMasterNode, appMasterSplitNode);

                LOG.info("Master computation is done.");
            }
        }.execute();
    }

}
