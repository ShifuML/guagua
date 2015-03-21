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
package ml.shifu.guagua.master;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import ml.shifu.guagua.BasicCoordinator;
import ml.shifu.guagua.GuaguaConstants;
import ml.shifu.guagua.GuaguaRuntimeException;
import ml.shifu.guagua.coordinator.zk.GuaguaZooKeeper.Filter;
import ml.shifu.guagua.io.Bytable;
import ml.shifu.guagua.io.HaltBytable;
import ml.shifu.guagua.util.NumberFormatUtils;
import ml.shifu.guagua.util.StringUtils;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link AbstractMasterCoordinator} has some common implementation for both async and sync worker coordinator.
 * 
 * <p>
 * Common functions include: znodes cleaning up, fail-over support and others.
 * 
 * @param <MASTER_RESULT>
 *            master result for computation in each iteration.
 * @param <WORKER_RESULT>
 *            worker result for computation in each iteration.
 */
public abstract class AbstractMasterCoordinator<MASTER_RESULT extends Bytable, WORKER_RESULT extends Bytable> extends
        BasicCoordinator<MASTER_RESULT, WORKER_RESULT> implements MasterInterceptor<MASTER_RESULT, WORKER_RESULT> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractMasterCoordinator.class);

    private String myBid;

    @Override
    public void postIteration(final MasterContext<MASTER_RESULT, WORKER_RESULT> context) {
        new BasicCoordinatorCommand() {
            @Override
            public void doExecute() throws KeeperException, InterruptedException {
                // update master halt status.
                // commented this line: since 0.5.0 only master will be allowed to halt whole guagua app, no matter what
                // halt status in worker result, it will be ingnored.
                updateMasterHaltStatus(context);

                // create worker znode in next iteration: '/_guagua/<jobId>/workers/2' to avoid re-create znode from
                // workers
                String workerBaseNode = null;
                try {
                    workerBaseNode = getWorkerBaseNode(context.getAppId(), context.getCurrentIteration() + 1)
                            .toString();
                    getZooKeeper().createExt(workerBaseNode, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, false);
                } catch (KeeperException.NodeExistsException e) {
                    LOG.warn("Node exists: {}", workerBaseNode);
                }

                // create master znode
                boolean isSplit = false;
                String appCurrentMasterNode = getCurrentMasterNode(context.getAppId(), context.getCurrentIteration())
                        .toString();
                String appCurrentMasterSplitNode = getCurrentMasterSplitNode(context.getAppId(),
                        context.getCurrentIteration()).toString();
                try {
                    byte[] bytes = getMasterSerializer().objectToBytes(context.getMasterResult());
                    isSplit = setBytesToZNode(appCurrentMasterNode, appCurrentMasterSplitNode, bytes,
                            CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException e) {
                    LOG.warn("Has such node:", e);
                }

                // remove -2 znode, no need, -1 is needed for fail-over.
                if(context.getCurrentIteration() >= 2) {
                    String znode = getMasterNode(context.getAppId(), context.getCurrentIteration() - 2).toString();
                    try {
                        getZooKeeper().deleteExt(znode, -1, false);
                        if(isSplit) {
                            znode = getCurrentMasterSplitNode(context.getAppId(), context.getCurrentIteration() - 2)
                                    .toString();
                            getZooKeeper().deleteExt(znode, -1, true);
                        }
                    } catch (KeeperException.NoNodeException e) {
                        if(System.nanoTime() % 20 == 0) {
                            LOG.warn("No such node:{}", znode);
                        }
                    }
                }

                LOG.info("master results write to znode.");
            }
        }.execute();
    }

    @Override
    public void postApplication(final MasterContext<MASTER_RESULT, WORKER_RESULT> context) {
        new BasicCoordinatorCommand() {
            @Override
            public void doExecute() throws KeeperException, InterruptedException {
                try {
                    // if clean up zk znodes cost two much running time, one can set zk cleanup flag. But to make sure
                    // clean the znodes manually after application.
                    String zkCleanUpEnabled = StringUtils.get(
                            context.getProps().getProperty(GuaguaConstants.GUAGUA_ZK_CLEANUP_ENABLE),
                            GuaguaConstants.GUAGUA_ZK_DEFAULT_CLEANUP_VALUE);
                    String appId = context.getAppId();
                    boolean isLastMaster = true;
                    if(NumberFormatUtils.getInt(context.getProps().getProperty(GuaguaConstants.GUAGUA_MASTER_NUMBER),
                            GuaguaConstants.DEFAULT_MASTER_NUMBER) > 1) {
                        String masterElectionPath = getBaseMasterElectionNode(appId).toString();
                        List<String> masterElectionNodes = getZooKeeper().getChildrenExt(masterElectionPath, false,
                                true, true);
                        isLastMaster = isLastMaster(masterElectionNodes);
                    }

                    if(isLastMaster && Boolean.TRUE.toString().equalsIgnoreCase(zkCleanUpEnabled)) {
                        final int currentIteration = context.getCurrentIteration();
                        final int workers = context.getWorkers();
                        final String endWorkersNode = getWorkerBaseNode(appId, currentIteration).toString();

                        new RetryCoordinatorCommand(isFixedTime(), getSleepTime()) {
                            @Override
                            public boolean retryExecution() throws KeeperException, InterruptedException {
                                try {
                                    List<String> workerChildern = getZooKeeper().getChildrenExt(endWorkersNode, false,
                                            false, true);
                                    int workersEndCompleted = workerChildern.size();
                                    // to avoid log flood
                                    if(System.nanoTime() % 10 == 0) {
                                        LOG.info("iteration {}, workers ended: {}, still {} workers are not synced.",
                                                currentIteration, workersEndCompleted, (workers - workersEndCompleted));
                                    }
                                    return workers == workersEndCompleted;
                                } catch (KeeperException.NoNodeException e) {
                                    // to avoid log flood
                                    if(System.nanoTime() % 10 == 0) {
                                        LOG.warn("No such node:{}", endWorkersNode);
                                    }
                                    return false;
                                }
                            }
                        }.execute();
                        // delete app znode
                        String appNode = getAppNode(appId).toString();

                        try {
                            getZooKeeper().deleteExt(appNode, -1, true);
                        } catch (KeeperException.NoNodeException e) {
                            if(System.nanoTime() % 20 == 0) {
                                LOG.warn("No such node:{}", appNode);
                            }
                        }
                    }
                } finally {
                    closeZooKeeper();
                }
            }

            private boolean isLastMaster(List<String> masterElectionNodes) {
                return masterElectionNodes == null || masterElectionNodes.size() == 0
                        || masterElectionNodes.get(masterElectionNodes.size() - 1).equals(getMyBid());
            }
        }.execute();
    }

    /**
     * {@link FailOverCommand} is used to read last iteration before task failed.
     * 
     * <p>
     * To read last iteration, just read all iterations from master znodes and get the maximal one.
     * 
     * <p>
     * Master znodes should be set as persistent type.
     * 
     */
    protected class FailOverCommand extends BasicCoordinatorCommand {

        private final MasterContext<MASTER_RESULT, WORKER_RESULT> context;

        public FailOverCommand(MasterContext<MASTER_RESULT, WORKER_RESULT> context) {
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
                    this.context.setCurrentIteration(restartedIteration);
                    LOG.info("Container {} restarted at: {} step.", context.getContainerId(), restartedIteration);
                } catch (NumberFormatException e) {
                    this.context.setCurrentIteration(GuaguaConstants.GUAGUA_INIT_STEP);
                }
            }
        }
    }

    /**
     * Set worker results from znodes.
     */
    protected void setWorkerResults(final MasterContext<MASTER_RESULT, WORKER_RESULT> context,
            final String appCurrentWorkersNode, final String appId, final int iteration) throws KeeperException,
            InterruptedException {
        // No need to get data from init step since in that step there is no results setting.
        if(context.getCurrentIteration() == GuaguaConstants.GUAGUA_INIT_STEP) {
            return;
        }
        final List<String> workerChildern = getZooKeeper().getChildrenExt(appCurrentWorkersNode, false, false, false);

        context.setWorkerResults(new Iterable<WORKER_RESULT>() {
            @Override
            public Iterator<WORKER_RESULT> iterator() {
                return new Iterator<WORKER_RESULT>() {

                    private Iterator<String> itr;

                    private volatile AtomicBoolean isStart = new AtomicBoolean();

                    @Override
                    public boolean hasNext() {
                        if(this.isStart.compareAndSet(false, true)) {
                            this.itr = workerChildern.iterator();
                        }
                        boolean hasNext = this.itr.hasNext();
                        if(!hasNext) {
                            // to make sure it can be iterated again, it shouldn't be a good case for iterator, we will
                            // iterate again to check if all workers are halt.
                            this.itr = workerChildern.iterator();
                            return false;
                        }
                        return hasNext;
                    }

                    @Override
                    public WORKER_RESULT next() {
                        String worker = this.itr.next();
                        String appCurrentWorkerSplitNode = getCurrentWorkerSplitNode(appId, worker, iteration)
                                .toString();
                        byte[] data = null;
                        try {
                            data = getBytesFromZNode(appCurrentWorkersNode + GuaguaConstants.ZOOKEEPER_SEPARATOR
                                    + worker, appCurrentWorkerSplitNode);
                        } catch (KeeperException e) {
                            throw new GuaguaRuntimeException(e);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        if(data != null) {
                            WORKER_RESULT workerResult = getWorkerSerializer().bytesToObject(data,
                                    context.getWorkerResultClassName());
                            return workerResult;
                        }
                        return null;
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        });
    }

    /**
     * Check whether GuaguaConstants.GUAGUA_WORKER_HALT_ENABLE) is enabled, if yes, check whether all workers are halted
     * and update master status.
     */
    protected void updateMasterHaltStatus(final MasterContext<MASTER_RESULT, WORKER_RESULT> context) {
        MASTER_RESULT result = context.getMasterResult();
        // a switch to make all workers have the right to terminate the application
        if(Boolean.TRUE.toString().equalsIgnoreCase(
                context.getProps().getProperty(GuaguaConstants.GUAGUA_WORKER_HALT_ENABLE,
                        GuaguaConstants.GUAGUA_WORKER_DEFAULT_HALT_ENABLE))) {
            if(isAllWorkersHalt(context.getWorkerResults()) && result instanceof HaltBytable) {
                ((HaltBytable) result).setHalt(true);
                context.setMasterResult(result);
            }
        }
    }

    /**
     * Check whether all workers are halted.
     */
    protected boolean isAllWorkersHalt(final Iterable<WORKER_RESULT> workerResults) {
        // This boolean is for a bug, if no element in worker results, return true is not correct, should return false.
        boolean isHasWorkerResults = false;
        for(WORKER_RESULT workerResult: workerResults) {
            isHasWorkerResults = true;
            if(!(workerResult instanceof HaltBytable) || !((HaltBytable) workerResult).isHalt()) {
                return false;
            }
        }
        return isHasWorkerResults ? true : false;
    }

    /**
     * Elect master from several backup masters.
     * 
     * <p>
     * Wait until it is the first bid, then it is elected as master.
     * 
     * <p>
     * Since fail-over in hadop map-reduce tasks is very fast. Using multiple-master is not a good choice expecially
     * time out is too large.
     * 
     * <p>
     * Multiple masters are used in environment in which no fail-over.
     * 
     */
    protected class MasterElectionCommand extends BasicCoordinatorCommand {

        private final String appId;

        public MasterElectionCommand(String appId) {
            this.appId = appId;
        }

        @Override
        public void doExecute() throws KeeperException, InterruptedException {
            final String masterElectionPath = getBaseMasterElectionNode(this.appId).toString();
            String masterElectionNode = getMasterElectionNode(this.appId, getZooKeeper().getZooKeeper().getSessionId())
                    .toString();
            try {
                getZooKeeper().createExt(masterElectionPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, true);
            } catch (KeeperException.NodeExistsException e) {
                LOG.warn("Node exists: {}", masterElectionPath);
            }

            setMyBid(getZooKeeper().createExt(masterElectionNode, null, Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL_SEQUENTIAL, true));

            new RetryCoordinatorCommand(isFixedTime(), getSleepTime()) {
                @Override
                public boolean retryExecution() throws KeeperException, InterruptedException {
                    List<String> masterChildArr = getZooKeeper().getChildrenExt(masterElectionPath, false, true, true);
                    // to avoid log flood
                    if(System.nanoTime() % 20 == 0) {
                        LOG.info("becomeMaster: First child is '{}' and my bid is '{}'", masterChildArr.get(0),
                                getMyBid());
                    }
                    return masterChildArr.get(0).equals(getMyBid());
                }
            }.execute();

            LOG.info("Become master.");
        }

    }

    public String getMyBid() {
        return myBid;
    }

    public void setMyBid(String myBid) {
        this.myBid = myBid;
    }

}
