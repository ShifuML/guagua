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
package ml.shifu.guagua.master;

import java.util.List;
import java.util.concurrent.TimeUnit;

import ml.shifu.guagua.GuaguaConstants;
import ml.shifu.guagua.io.Bytable;
import ml.shifu.guagua.util.NumberFormatUtils;
import ml.shifu.guagua.util.ProgressLock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link AsyncMasterCoordinator} is used to as a barrier for each iteration.
 * 
 * <p>
 * For each iteration, {@link AsyncMasterCoordinator} will wait until all workers are done.
 * 
 * <p>
 * To start a new iteration, {@link AsyncMasterCoordinator} will write a znode for each iteration like
 * '/_guagua/job_201312041304_189025/master/{currentIteration}' with with {@link MasterComputable} result as its data.
 * This is like a signal to notify workers.
 * 
 * <p>
 * Workers are waiting on current master znode, if got current master znode, it will start another iteration.
 * 
 * @param <MASTER_RESULT>
 *            master result for computation in each iteration.
 * @param <WORKER_RESULT>
 *            worker result for computation in each iteration.
 */
public class AsyncMasterCoordinator<MASTER_RESULT extends Bytable, WORKER_RESULT extends Bytable> extends
        AbstractMasterCoordinator<MASTER_RESULT, WORKER_RESULT> {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncMasterCoordinator.class);

    /**
     * Current iteration
     */
    private int currentIteration;

    /**
     * Current app id.
     */
    private String appId;

    /**
     * Lock is used to check register info from all workers.
     */
    protected ProgressLock workerInitLock = new ProgressLock();

    /**
     * Lock is used to check iteration info from all workers.
     */
    protected ProgressLock workerIterationLock = new ProgressLock();

    @Override
    public void process(WatchedEvent event) {
        LOG.debug("DEBUG: process: Got a new event, path = {}, type = {}, state = {}", event.getPath(),
                event.getType(), event.getState());

        if((event.getPath() == null) && (event.getType() == EventType.None)) {
            if(event.getState() == KeeperState.SyncConnected) {
                LOG.info("process: Asynchronous connection complete.");
                super.getZkConnLatch().countDown();
            } else {
                LOG.warn("process: Got unknown null path event " + event);
            }
            return;
        }

        /**
         * Check lock signal condition.
         */
        String appWorkerBaseNode = getWorkerBaseNode(getAppId(), getCurrentIteration()).toString();
        if(event.getPath().equals(appWorkerBaseNode) && (event.getType() == EventType.NodeChildrenChanged)) {
            if(getCurrentIteration() == 0) {
                this.workerInitLock.signal();
            } else {
                this.workerIterationLock.signal();
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
    public void preApplication(final MasterContext<MASTER_RESULT, WORKER_RESULT> context) {
        initialize(context.getProps());
        this.setAppId(context.getAppId());

        // Master election which is used here to use the same zookeeper instance.
        if(NumberFormatUtils.getInt(context.getProps().getProperty(GuaguaConstants.GUAGUA_MASTER_NUMBER),
                GuaguaConstants.DEFAULT_MASTER_NUMBER) > 1) {
            new MasterElectionCommand(context.getAppId()).execute();
        }

        // Check last successful iteration
        new FailOverCommand(context).execute();

        if(context.getCurrentIteration() != GuaguaConstants.GUAGUA_INIT_STEP) {
            // if not init step, return, because of no need initialize twice for fail-over task
            return;
        }
        new BasicCoordinatorCommand() {
            @Override
            public void doExecute() throws KeeperException, InterruptedException {
                final String appWorkersNode = getWorkerBaseNode(context.getAppId(), context.getCurrentIteration())
                        .toString();

                new RetryCoordinatorCommand(isFixedTime(), getSleepTime()) {
                    @Override
                    public boolean retryExecution() throws KeeperException, InterruptedException {
                        try {
                            // to avoid re-watching
                            List<String> children = getZooKeeper().getChildrenExt(appWorkersNode, false, false, false);
                            int size = children == null ? 0 : children.size();
                            if(isTerminated(size, context.getWorkers(), context.getMinWorkersRatio(),
                                    context.getMinWorkersTimeOut())) {
                                return true;
                            }
                            children = getZooKeeper().getChildrenExt(appWorkersNode, true, false, false);
                            size = children == null ? 0 : children.size();
                            if(isTerminated(size, context.getWorkers(), context.getMinWorkersRatio(),
                                    context.getMinWorkersTimeOut())) {
                                return true;
                            }
                            // to avoid log flood
                            if(System.nanoTime() % 20 == 0) {
                                LOG.info("workers already initialized: {}, still {} workers are not synced.", size,
                                        (context.getWorkers() - size));
                            }
                            AsyncMasterCoordinator.this.workerInitLock.waitForever();
                            AsyncMasterCoordinator.this.workerInitLock.reset();
                        } catch (KeeperException.NoNodeException e) {
                            // to avoid log flood
                            if(System.nanoTime() % 10 == 0) {
                                LOG.warn("No such node:{}", appWorkersNode);
                            }
                        }
                        return false;
                    }
                }.execute();

                LOG.info("All workers are initiliazed successfully.");

                String znode = null;
                try {
                    // create worker znode 1: '/_guagua/<jobId>/workers/1' to avoid re-create znode from workers
                    znode = getWorkerBaseNode(context.getAppId(), context.getCurrentIteration() + 1).toString();
                    getZooKeeper().createExt(znode, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, false);
                    // create master init znode
                    znode = getMasterBaseNode(context.getAppId()).toString();
                    getZooKeeper().createExt(znode, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, false);
                    znode = getCurrentMasterNode(context.getAppId(), context.getCurrentIteration()).toString();
                    getZooKeeper().createExt(znode, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, false);
                } catch (KeeperException.NodeExistsException e) {
                    LOG.warn("Node exists: {}", znode);
                }
            }
        }.execute();
    }

    @Override
    public void preIteration(final MasterContext<MASTER_RESULT, WORKER_RESULT> context) {
        this.setCurrentIteration(context.getCurrentIteration());

        new BasicCoordinatorCommand() {
            @Override
            public void doExecute() throws KeeperException, InterruptedException {
                // wait All Workers Done
                final int currentIteration = context.getCurrentIteration();
                final int workers = context.getWorkers();
                final String appCurrentWorkersNode = getWorkerBaseNode(context.getAppId(), currentIteration).toString();

                long start = System.nanoTime();
                // wait to get all workers results.
                new RetryCoordinatorCommand(isFixedTime(), getSleepTime()) {
                    @Override
                    public boolean retryExecution() throws KeeperException, InterruptedException {
                        try {
                            List<String> workerChildern = getZooKeeper().getChildrenExt(appCurrentWorkersNode, false,
                                    false, false);

                            int size = workerChildern == null ? 0 : workerChildern.size();
                            if(isTerminated(size, context.getWorkers(), context.getMinWorkersRatio(),
                                    context.getMinWorkersTimeOut())) {
                                return true;
                            }

                            workerChildern = getZooKeeper().getChildrenExt(appCurrentWorkersNode, true, false, false);

                            size = workerChildern == null ? 0 : workerChildern.size();
                            if(isTerminated(size, context.getWorkers(), context.getMinWorkersRatio(),
                                    context.getMinWorkersTimeOut())) {
                                return true;
                            }
                            // to avoid log flood
                            if(System.nanoTime() % 20 == 0) {
                                LOG.info("iteration {}, workers compelted: {}, still {} workers are not synced.",
                                        currentIteration, size, (workers - size));
                            }
                            AsyncMasterCoordinator.this.workerIterationLock.waitForever();
                            AsyncMasterCoordinator.this.workerIterationLock.reset();
                        } catch (KeeperException.NoNodeException e) {
                            // to avoid log flood
                            if(System.nanoTime() % 10 == 0) {
                                LOG.warn("No such node:{}", appCurrentWorkersNode);
                            }
                        }
                        return false;
                    }
                }.execute();
                LOG.info("Application {} container {} iteration {} waiting ends with {}ms execution time.",
                        context.getAppId(), context.getContainerId(), context.getCurrentIteration(),
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
                // wait until worker results are set from zookeeper znodes.
                setWorkerResults(context, appCurrentWorkersNode, context.getAppId(), currentIteration);

            }
        }.execute();
    }

}
