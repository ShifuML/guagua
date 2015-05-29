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
package ml.shifu.guagua.worker;

import java.util.concurrent.TimeUnit;

import ml.shifu.guagua.GuaguaConstants;
import ml.shifu.guagua.io.Bytable;
import ml.shifu.guagua.master.MasterComputable;
import ml.shifu.guagua.master.SyncMasterCoordinator;
import ml.shifu.guagua.util.ProgressLock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link AsyncWorkerCoordinator} is used to as a worker barrier for each iteration.
 * 
 * <p>
 * For each iteration, {@link AsyncWorkerCoordinator} will wait until master's signal.
 * 
 * <p>
 * To start a new iteration, {@link SyncMasterCoordinator} will write a znode for each iteration like
 * '/_guagua/job_201312041304_189025/master/{currentIteration}' with with {@link MasterComputable} result as its data.
 * {@link AsyncWorkerCoordinator} is trying to detect whether it exists, if yes, to start a new iteration.
 * 
 * <p>
 * Worker result will be written into each worker iteration znode for master to get.
 * 
 * @param <MASTER_RESULT>
 *            master result for computation in each iteration.
 * @param <WORKER_RESULT>
 *            worker result for computation in each iteration.
 */
public class AsyncWorkerCoordinator<MASTER_RESULT extends Bytable, WORKER_RESULT extends Bytable> extends
        AbstractWorkerCoordinator<MASTER_RESULT, WORKER_RESULT> {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncWorkerCoordinator.class);

    /**
     * Current iteration, start from 1, not 0.
     */
    private int currentIteration;

    /**
     * Application id, mapreduce job id or yarn application id.
     */
    private String appId;

    /**
     * Lock is used to check register info from master.
     */
    protected ProgressLock masterInitLock = new ProgressLock();

    /**
     * Lock is used to check iteration info from master.
     */
    protected ProgressLock masterIterationLock = new ProgressLock();

    @Override
    public void process(WatchedEvent event) {
        LOG.info("DEBUG: process: Got a new event, path = {}, type = {}, state = {}", event.getPath(), event.getType(),
                event.getState());

        if((event.getPath() == null) && (event.getType() == EventType.None)) {
            if(event.getState() == KeeperState.SyncConnected) {
                LOG.info("process: Asynchronous connection complete.");
                super.getZkConnLatch().countDown();
            } else {
                LOG.warn("process: Got unknown null path event " + event);
            }
            return;
        }

        String appMasterNode = getCurrentMasterNode(getAppId(), getCurrentIteration()).toString();

        if(event.getPath().equals(appMasterNode)
                && (event.getType() == EventType.NodeCreated || event.getType() == EventType.NodeDataChanged)) {
            if(getCurrentIteration() == 0) {
                this.masterInitLock.signal();
            } else {
                this.masterIterationLock.signal();
            }
        }
    }

    public int getCurrentIteration() {
        return currentIteration;
    }

    public void setCurrentIteration(int currentIteration) {
        this.currentIteration = currentIteration;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    @Override
    public void preApplication(final WorkerContext<MASTER_RESULT, WORKER_RESULT> context) {
        // initialize zookeeper and other props
        initialize(context.getProps());
        this.setAppId(context.getAppId());

        // Check last successful iteration
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

                // check whether master is ok to start new iteration.
                stat = getZooKeeper().exists(appMasterNode, true);
                if(stat == null) {
                    LOG.info("DEBUG: wait for {}.", appMasterNode);
                    AsyncWorkerCoordinator.this.masterInitLock.waitForever();
                    AsyncWorkerCoordinator.this.masterInitLock.reset();
                }
                // check master result in current iteration, it will be used as master result of last iteration.
                if(context.getCurrentIteration() != GuaguaConstants.GUAGUA_INIT_STEP) {
                    String appMasterSplitNode = getCurrentMasterSplitNode(appId, currentIteration).toString();
                    setMasterResult(context, appMasterNode, appMasterSplitNode);
                }
                LOG.info("Master initilization is done.");
            }
        }.execute();
    }

    @Override
    public void preIteration(WorkerContext<MASTER_RESULT, WORKER_RESULT> context) {
        this.setCurrentIteration(context.getCurrentIteration());
        LOG.info("Start itertion {} with container id {} and app id {}.", context.getCurrentIteration(),
                context.getContainerId(), context.getAppId());
    }

    @Override
    public void postIteration(final WorkerContext<MASTER_RESULT, WORKER_RESULT> context) {
        new BasicCoordinatorCommand() {
            @Override
            public void doExecute() throws KeeperException, InterruptedException {
                String appId = context.getAppId();
                int currentIteration = context.getCurrentIteration();
                String containerId = context.getContainerId();
                final String appMasterNode = getCurrentMasterNode(appId, currentIteration).toString();
                String appWorkerNode = getCurrentWorkerNode(appId, containerId, currentIteration).toString();
                final String appWorkerSplitNode = getCurrentWorkerSplitNode(appId, containerId, currentIteration)
                        .toString();
                // create worker znode
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

                // check whether master is ok to start new iteration.
                LOG.info("wait to check master:{}", appMasterNode);
                long start = System.nanoTime();
                Stat stat = getZooKeeper().exists(appMasterNode, true);
                if(stat == null) {
                    AsyncWorkerCoordinator.this.masterIterationLock.waitForever();
                    AsyncWorkerCoordinator.this.masterIterationLock.reset();
                }
                LOG.info("Application {} container {} iteration {} waiting ends with {}ms execution time.",
                        context.getAppId(), context.getContainerId(), context.getCurrentIteration(),
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));

                // check master result in current iteration, it will be used as master result of last iteration.
                String appMasterSplitNode = getCurrentMasterSplitNode(appId, currentIteration).toString();
                setMasterResult(context, appMasterNode, appMasterSplitNode);
                LOG.info("Master computation is done.");
            }
        }.execute();
    }

}
