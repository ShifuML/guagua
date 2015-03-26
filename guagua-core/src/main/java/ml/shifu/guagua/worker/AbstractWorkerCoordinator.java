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
package ml.shifu.guagua.worker;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import ml.shifu.guagua.BasicCoordinator;
import ml.shifu.guagua.GuaguaConstants;
import ml.shifu.guagua.coordinator.zk.GuaguaZooKeeper.Filter;
import ml.shifu.guagua.io.Bytable;
import ml.shifu.guagua.util.StringUtils;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link AbstractWorkerCoordinator} has some common implementation for both async and sync worker coordinator.
 * 
 * <p>
 * Common functions include: znodes cleaning up, fail-over support and others.
 * 
 * @param <MASTER_RESULT>
 *            master result for computation in each iteration.
 * @param <WORKER_RESULT>
 *            worker result for computation in each iteration.
 */
public abstract class AbstractWorkerCoordinator<MASTER_RESULT extends Bytable, WORKER_RESULT extends Bytable> extends
        BasicCoordinator<MASTER_RESULT, WORKER_RESULT> implements WorkerInterceptor<MASTER_RESULT, WORKER_RESULT> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractWorkerCoordinator.class);

    @Override
    public void preIteration(WorkerContext<MASTER_RESULT, WORKER_RESULT> context) {
        LOG.info("Start itertion {} with container id {} and app id {}.", context.getCurrentIteration(),
                context.getContainerId(), context.getAppId());
    }

    @Override
    public void postApplication(final WorkerContext<MASTER_RESULT, WORKER_RESULT> context) {
        new BasicCoordinatorCommand() {
            @Override
            public void doExecute() throws KeeperException, InterruptedException {
                try {
                    // if clean up zk znodes cost two much running time, one can set zk cleanup flag. But to make sure
                    // clean the znodes manually after application.
                    String zkCleanUpEnabled = StringUtils.get(
                            context.getProps().getProperty(GuaguaConstants.GUAGUA_ZK_CLEANUP_ENABLE),
                            GuaguaConstants.GUAGUA_ZK_DEFAULT_CLEANUP_VALUE);
                    if(Boolean.TRUE.toString().equalsIgnoreCase(zkCleanUpEnabled)) {
                        // delete worker znode
                        String appId = context.getAppId();
                        String containerId = context.getContainerId();
                        int currentIteration = context.getCurrentIteration();
                        String currentWorkerNode = getCurrentWorkerNode(appId, containerId, currentIteration - 1)
                                .toString();
                        try {
                            getZooKeeper().deleteExt(currentWorkerNode, -1, false);
                        } catch (KeeperException.NoNodeException e) {
                            if(System.nanoTime() % 20 == 0) {
                                LOG.warn("No such node:{}", currentWorkerNode);
                            }
                        }
                        // create last worker znode to notice master done state of this worker.
                        String appWorkerNode = getCurrentWorkerNode(appId, containerId, currentIteration).toString();
                        getZooKeeper()
                                .createExt(appWorkerNode, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, false);
                    }
                } finally {
                    closeZooKeeper();
                }
            }
        }.execute();
    }

    protected void setMasterResult(final WorkerContext<MASTER_RESULT, WORKER_RESULT> context,
            final String appMasterNode, final String appMasterSplitNode) throws KeeperException, InterruptedException {
        if(context.getCurrentIteration() == GuaguaConstants.GUAGUA_INIT_STEP) {
            return;
        }
        byte[] data = getBytesFromZNode(appMasterNode, appMasterSplitNode);
        if(data != null && data.length > 0) {
            MASTER_RESULT lastMasterResult = getMasterSerializer().bytesToObject(data,
                    context.getMasterResultClassName());
            context.setLastMasterResult(lastMasterResult);
        }
    }

    protected class FailOverCoordinatorCommand extends BasicCoordinatorCommand {

        private final WorkerContext<MASTER_RESULT, WORKER_RESULT> context;

        public FailOverCoordinatorCommand(WorkerContext<MASTER_RESULT, WORKER_RESULT> context) {
            this.context = context;
        }

        @Override
        public void doExecute() throws KeeperException, InterruptedException {
            String masterBaseNode = getMasterBaseNode(context.getAppId()).toString();
            List<String> masterIterations = null;
            try {
                masterIterations = getZooKeeper().getChildrenExt(masterBaseNode, false, false, false, new Filter() {
                    @Override
                    public boolean filter(String path) {
                        try {
                            Integer.parseInt(path);
                            return false;
                        } catch (Exception e) {
                            return true;
                        }
                    }
                });
            } catch (KeeperException.NoNodeException e) {
                LOG.warn("No such node:{}", masterBaseNode);
            }
            if(masterIterations != null && masterIterations.size() > 0) {
                Collections.sort(masterIterations, new Comparator<String>() {
                    @Override
                    public int compare(String o1, String o2) {
                        return Integer.valueOf(o1).compareTo(Integer.valueOf(o2));
                    }
                });
                LOG.info("DEBUG: master children:{}", masterIterations);
                try {
                    int restartedIteration = Integer.valueOf(masterIterations.get(masterIterations.size() - 1));
                    context.setCurrentIteration(restartedIteration);
                    LOG.info("Container {} restarted at: {} step.", context.getContainerId(), restartedIteration);
                } catch (NumberFormatException e) {
                    context.setCurrentIteration(GuaguaConstants.GUAGUA_INIT_STEP);
                }
            }
        }
    }
}
