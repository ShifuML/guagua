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
package ml.shifu.guagua.master;

import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import ml.shifu.guagua.GuaguaConstants;
import ml.shifu.guagua.GuaguaRuntimeException;
import ml.shifu.guagua.io.Bytable;
import ml.shifu.guagua.io.BytableWrapper;
import ml.shifu.guagua.io.HaltBytable;
import ml.shifu.guagua.io.NettyBytableDecoder;
import ml.shifu.guagua.io.NettyBytableEncoder;
import ml.shifu.guagua.util.BytableDiskList;
import ml.shifu.guagua.util.BytableMemoryDiskList;
import ml.shifu.guagua.util.MemoryDiskList;
import ml.shifu.guagua.util.NetworkUtils;
import ml.shifu.guagua.util.NumberFormatUtils;
import ml.shifu.guagua.util.ReflectionUtils;
import ml.shifu.guagua.util.StringUtils;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelState;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A master coordinator to coordinate with workers through Netty server.
 * 
 * <p>
 * Master still updates results to Zookeeper znodes for fail-over. While workers sends results to master through Netty
 * server connection.
 * 
 * <p>
 * Worker results are persisted into {@link MemoryDiskList}, the reason is that for big model, limited memory may not be
 * enough to store all worker results in memory.
 */
public class NettyMasterCoordinator<MASTER_RESULT extends Bytable, WORKER_RESULT extends Bytable> extends
        AbstractMasterCoordinator<MASTER_RESULT, WORKER_RESULT> {

    private static final Logger LOG = LoggerFactory.getLogger(NettyMasterCoordinator.class);

    /**
     * A server instance used to communicate with all workers.
     */
    private ServerBootstrap messageServer;

    /**
     * Message server port used to for Netty server to communicate with workers.
     */
    private int messageServerPort;

    private static final Object LOCK = new Object();

    /**
     * Worker results for each iteration. It should store each worker result in each iteration once.
     */
    // private List<BytableWrapper> iterResults = Collections.synchronizedList(new ArrayList<BytableWrapper>());
    private BytableMemoryDiskList<BytableWrapper> iterResults;

    /**
     * 'indexMap' used to store <containerId, index in iterResults> for search, guarded by {@link #LOCK}.
     */
    private Map<String, Integer> indexMap = new HashMap<String, Integer>();

    /**
     * Current iteration.
     */
    private int currentInteration;

    /**
     * Total iteration, this should not be change after initialization.
     */
    private int totalInteration;

    /**
     * A flag to see if we can update worker results map instance, guarded by {@link #LOCK}.
     */
    private boolean canUpdateWorkerResultMap = true;

    /**
     * Master result in last iteration, which is used to set stop message
     */
    private MASTER_RESULT masterResult = null;

    @Override
    protected void initialize(Properties props) {
        super.initialize(props);
        initIterResults(props);
    }

    private void initIterResults(Properties props) {
        synchronized(LOCK) {
            BytableDiskList<BytableWrapper> bytableDiskList = new BytableDiskList<BytableWrapper>(
                    System.currentTimeMillis() + "", BytableWrapper.class.getName());
            double memoryFraction = Double.valueOf(props.getProperty(
                    GuaguaConstants.GUAGUA_MASTER_WORKERESULTS_MEMORY_FRACTION,
                    GuaguaConstants.GUAGUA_MASTER_WORKERESULTS_DEFAULT_MEMORY_FRACTION));
            long memoryStoreSize = (long) (Runtime.getRuntime().maxMemory() * memoryFraction);
            this.iterResults = new BytableMemoryDiskList<BytableWrapper>(memoryStoreSize, bytableDiskList);
        }
    }

    /**
     * Do initialization and fail-over checking before all iterations.
     */
    @Override
    public void preApplication(final MasterContext<MASTER_RESULT, WORKER_RESULT> context) {
        // Initialize zookeeper and other props
        initialize(context.getProps());

        // init total iteration for later usage
        this.totalInteration = context.getTotalIteration();

        // Fail over checking to check current iteration.
        new FailOverCommand(context).execute();

        // Start master netty server
        startNettyServer(context.getProps());

        // if is init step, set currentIteration to 1 else set to current Iteration.
        synchronized(LOCK) {
            if(context.isInitIteration()) {
                this.currentInteration = GuaguaConstants.GUAGUA_INIT_STEP;
            } else {
                this.currentInteration = context.getCurrentIteration();
            }
        }
        // Create master initial znode with message server address.
        initMasterZnode(context);

        if(!context.isInitIteration()) {
            // if not init step, return, because of no need initialize twice for fail-over task
            return;
        }

        this.clear(context.getProps());

        LOG.info("All workers are initiliazed successfully.");
    }

    /**
     * Clear all status before next iteration.
     */
    private void clear(Properties props) {
        synchronized(LOCK) {
            // clear and wait for next iteration.
            this.iterResults.close();
            this.iterResults.clear();
            this.initIterResults(props);
            this.indexMap.clear();
            this.canUpdateWorkerResultMap = true;
        }
    }

    /**
     * Initialize master znode with master Netty server name <name:port>.
     */
    private void initMasterZnode(final MasterContext<MASTER_RESULT, WORKER_RESULT> context) {
        String znode = null;
        // create master init znode
        try {
            znode = getMasterBaseNode(context.getAppId()).toString();
            getZooKeeper().createExt(znode, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, true);
        } catch (KeeperException.NodeExistsException e) {
            LOG.warn("Node exists: {}", znode);
        } catch (Exception e) {
            throw new GuaguaRuntimeException(e);
        }

        // update master server address info to init master znode
        try {
            znode = getCurrentMasterNode(context.getAppId(), GuaguaConstants.GUAGUA_INIT_STEP).toString();
            if(getZooKeeper().exists(znode, false) == null) {
                String znodeValue = InetAddress.getLocalHost().getHostName() + ":"
                        + NettyMasterCoordinator.this.messageServerPort + ":" + 1;
                getZooKeeper().createExt(znode, znodeValue.getBytes(Charset.forName("UTF-8")), Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT, false);
                LOG.info("Master znode initialization with server info {}", znodeValue);
            } else {
                String existZnodeValue = new String(getZooKeeper().getData(znode, null, null), Charset.forName("UTF-8"));
                int version = NumberFormatUtils.getInt(existZnodeValue.split(":")[2], true);
                String znodeValue = InetAddress.getLocalHost().getHostName() + ":"
                        + NettyMasterCoordinator.this.messageServerPort + ":" + (version + 1);
                getZooKeeper().createOrSetExt(znode, znodeValue.getBytes(Charset.forName("UTF-8")),
                        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, false, -1);
                LOG.info("Master znode re-initialization with server info {}", znodeValue);
            }
        } catch (KeeperException.NodeExistsException e) {
            LOG.warn("Node exists: {}", znode);
        } catch (Exception e) {
            throw new GuaguaRuntimeException(e);
        }
    }

    /**
     * Start netty server which is used to communicate with workers.
     */
    private void startNettyServer(Properties props) {
        this.messageServerPort = NumberFormatUtils.getInt(props.getProperty(GuaguaConstants.GUAGUA_NETTY_SEVER_PORT),
                GuaguaConstants.GUAGUA_NETTY_SEVER_DEFAULT_PORT);
        this.messageServerPort = NetworkUtils.getValidServerPort(this.messageServerPort);
        this.messageServer = new ServerBootstrap(new NioServerSocketChannelFactory(
                Executors.newFixedThreadPool(GuaguaConstants.GUAGUA_NETTY_SERVER_DEFAULT_THREAD_COUNT),
                Executors.newCachedThreadPool(new MasterThreadFactory())));

        // Set up the pipeline factory.
        this.messageServer.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() throws Exception {
                return Channels.pipeline(new NettyBytableEncoder(), new NettyBytableDecoder(), new ServerHandler());
            }
        });

        // Bind and start to accept incoming connections.
        try {
            this.messageServer.bind(new InetSocketAddress(this.messageServerPort));
        } catch (ChannelException e) {
            LOG.warn(e.getMessage() + "; try to rebind again.");
            this.messageServerPort = NetworkUtils.getValidServerPort(this.messageServerPort);
            this.messageServer.bind(new InetSocketAddress(this.messageServerPort));
        }

        try {
            LOG.info("Master netty server is started at {}", InetAddress.getLocalHost().getHostName() + ":"
                    + InetAddress.getLocalHost().getHostAddress() + ":" + NettyMasterCoordinator.this.messageServerPort);
        } catch (UnknownHostException e) {
            throw new GuaguaRuntimeException(e);
        }
    }

    /**
     * The master thread factory. Main feature is to print error log of worker thread.
     */
    private static class MasterThreadFactory implements ThreadFactory {
        static final AtomicInteger poolNumber = new AtomicInteger(1);
        final ThreadGroup group;
        final AtomicInteger threadNumber = new AtomicInteger(1);
        final String namePrefix;

        MasterThreadFactory() {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            namePrefix = "pool-" + poolNumber.getAndIncrement() + "-thread-";
        }

        public Thread newThread(Runnable r) {
            Thread thread = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
            if(thread.isDaemon()) {
                thread.setDaemon(false);
            }
            if(thread.getPriority() != Thread.NORM_PRIORITY) {
                thread.setPriority(Thread.NORM_PRIORITY);
            }
            thread.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
                @Override
                public void uncaughtException(Thread t, Throwable e) {
                    LOG.warn("Error message in thread {} with error message {}, error root cause {}.", t, e,
                            e.getCause());
                    // print stack???
                }
            });
            return thread;
        }
    }

    /**
     * {@link ServerHandler} is used to receive {@link Bytable} message from worker..
     */
    private class ServerHandler extends SimpleChannelUpstreamHandler {

        @Override
        public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
            if(e instanceof ChannelStateEvent && ((ChannelStateEvent) e).getState() != ChannelState.INTEREST_OPS) {
                LOG.debug(e.toString());
            }
            super.handleUpstream(ctx, e);
        }

        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
            if(!(e.getMessage() instanceof Bytable)) {
                throw new IllegalStateException("Message should be bytable instance.");
            }

            BytableWrapper bytableWrapper = (BytableWrapper) e.getMessage();
            LOG.debug("Received container id {} with message:{}", bytableWrapper.getContainerId(), bytableWrapper);
            String containerId = bytableWrapper.getContainerId();
            synchronized(LOCK) {
                if(!NettyMasterCoordinator.this.canUpdateWorkerResultMap) {
                    LOG.info("Cannot update worker result with message: containerId {} iteration {} currentIteration",
                            containerId, bytableWrapper.getCurrentIteration(),
                            NettyMasterCoordinator.this.currentInteration);
                    return;
                }
                if(bytableWrapper.isStopMessage()) {
                    // only accept stop message in unregistered iteration(total iteration +1) or halt condition is
                    // accepted, stop messages not in unregistered iteration will be ignored.
                    MASTER_RESULT masterResult = NettyMasterCoordinator.this.masterResult;
                    if((bytableWrapper.getCurrentIteration() == NettyMasterCoordinator.this.totalInteration + 1)
                            || ((masterResult instanceof HaltBytable) && ((HaltBytable) masterResult).isHalt())) {
                        // for stop message, no need to check current iteration.
                        if(!NettyMasterCoordinator.this.indexMap.containsKey(containerId)) {
                            NettyMasterCoordinator.this.iterResults.append(bytableWrapper);
                            NettyMasterCoordinator.this.indexMap.put(containerId,
                                    (int) (NettyMasterCoordinator.this.iterResults.size() - 1));
                        } else {
                            // if already exits, no need update, we hope it is the same result as the worker restarted
                            // and result computed again. or result is not for current iteration, throw that result.
                        }
                    }
                } else {
                    if(!NettyMasterCoordinator.this.indexMap.containsKey(containerId)
                            && NettyMasterCoordinator.this.currentInteration == bytableWrapper.getCurrentIteration()) {
                        NettyMasterCoordinator.this.iterResults.append(bytableWrapper);
                        NettyMasterCoordinator.this.indexMap.put(containerId,
                                (int) (NettyMasterCoordinator.this.iterResults.size() - 1));
                    } else {
                        // if already exits, no need update, we hope it is the same result as the worker restarted and
                        // result computed again. or result is not for current iteration, throw that result.
                    }
                }
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
            e.getChannel().close();
        }
    }

    /**
     * Wait for all workers done in current iteration.
     */
    @Override
    public void preIteration(final MasterContext<MASTER_RESULT, WORKER_RESULT> context) {
        // set current iteration firstly
        synchronized(LOCK) {
            this.currentInteration = context.getCurrentIteration();
            this.canUpdateWorkerResultMap = true;
        }

        long start = System.nanoTime();
        new RetryCoordinatorCommand(isFixedTime(), getSleepTime()) {
            @Override
            public boolean retryExecution() throws KeeperException, InterruptedException {
                // long to int is assumed successful as no such many workers need using long
                int doneWorkers;
                synchronized(LOCK) {
                    doneWorkers = (int) NettyMasterCoordinator.this.iterResults.size();
                }
                // to avoid log flood
                if(System.nanoTime() % 30 == 0) {
                    LOG.info("Iteration {}, workers compelted: {}, still {} workers are not synced.",
                            context.getCurrentIteration(), doneWorkers, (context.getWorkers() - doneWorkers));
                }

                boolean isTerminated = false;
                if(context.isFirstIteration() || context.getCurrentIteration() == context.getTotalIteration()) {
                    // in the first iteration or last iteration, make sure all workers loading data successfully, use
                    // default 1.0 as worker ratio.
                    // isTerminated = isTerminated(doneWorkers, context.getWorkers(),
                    // context.isFirstIteration() ? context.getMinWorkersRatio()
                    // : GuaguaConstants.GUAGUA_DEFAULT_MIN_WORKERS_RATIO,
                    // GuaguaConstants.GUAGUA_DEFAULT_MIN_WORKERS_TIMEOUT);
                    isTerminated = isTerminated(doneWorkers, context.getWorkers(), context.getMinWorkersRatio(),
                            GuaguaConstants.GUAGUA_DEFAULT_MIN_WORKERS_TIMEOUT);
                    if(isTerminated) {
                        // update canUpdateWorkerResultsMap to false no accept other results
                        synchronized(LOCK) {
                            NettyMasterCoordinator.this.canUpdateWorkerResultMap = false;
                        }
                        LOG.info(
                                "Iteration {}, master waiting is terminated by workers {} doneWorkers {} minWorkersRatio {} minWorkersTimeOut {}.",
                                context.getCurrentIteration(), context.getWorkers(), doneWorkers,
                                context.getMinWorkersRatio(), GuaguaConstants.GUAGUA_DEFAULT_MIN_WORKERS_TIMEOUT);
                    }
                } else {
                    isTerminated = isTerminated(doneWorkers, context.getWorkers(), context.getMinWorkersRatio(),
                            context.getMinWorkersTimeOut());
                    if(isTerminated) {
                        // update canUpdateWorkerResultsMap to false no accept other results
                        synchronized(LOCK) {
                            NettyMasterCoordinator.this.canUpdateWorkerResultMap = false;
                        }
                        LOG.info(
                                "Iteration {}, master waiting is terminated by workers {} doneWorkers {} minWorkersRatio {} minWorkersTimeOut {}.",
                                context.getCurrentIteration(), context.getWorkers(), doneWorkers,
                                context.getMinWorkersRatio(), context.getMinWorkersTimeOut());
                    }
                }
                return isTerminated;
            }
        }.execute();

        // switch state to read and not accept other results
        synchronized(LOCK) {
            this.canUpdateWorkerResultMap = false;
            this.iterResults.switchState();
        }
        // set worker results.
        final int currentIter = this.currentInteration;
        context.setWorkerResults(new Iterable<WORKER_RESULT>() {
            @Override
            public Iterator<WORKER_RESULT> iterator() {
                return new Iterator<WORKER_RESULT>() {

                    private Iterator<BytableWrapper> localItr;

                    private volatile AtomicBoolean isStart = new AtomicBoolean();

                    private boolean isPrint = false;

                    BytableWrapper current = null;

                    @Override
                    public boolean hasNext() {
                        boolean hasNext;
                        synchronized(LOCK) {
                            // debug start
                            if(!isPrint) {
                                Iterator<BytableWrapper> ii = NettyMasterCoordinator.this.iterResults.iterator();
                                int curr = 0, notCurr = 0;
                                while(ii.hasNext()) {
                                    BytableWrapper next = ii.next();
                                    if(NettyMasterCoordinator.this.currentInteration == next.getCurrentIteration()) {
                                        curr += 1;
                                    } else {
                                        notCurr += 1;
                                        LOG.info("iter result next {}", next);
                                    }
                                }
                                LOG.info("iter result curr {} notcurr {}", curr, notCurr);

                                isPrint = true;
                            }
                            // debug end

                            if(this.isStart.compareAndSet(false, true)) {
                                this.localItr = NettyMasterCoordinator.this.iterResults.iterator();
                            }
                            hasNext = this.localItr.hasNext();
                            if(hasNext) {
                                this.current = this.localItr.next();
                                // check if iteration number is the same, remove unneccasary results.
                                while(this.current.getCurrentIteration() != currentIter) {
                                    hasNext = this.localItr.hasNext();
                                    if(hasNext) {
                                        this.current = this.localItr.next();
                                        continue;
                                    } else {
                                        // no elements, break
                                        break;
                                    }
                                }
                            }
                            if(!hasNext) {
                                // to make sure it can be iterated again, it shouldn't be a good case for iterator, we
                                // will iterate again to check if all workers are halt.
                                this.localItr = NettyMasterCoordinator.this.iterResults.iterator();
                                return false;
                            }
                        }
                        return hasNext;
                    }

                    @Override
                    public WORKER_RESULT next() {
                        synchronized(LOCK) {
                            return NettyMasterCoordinator.this.getWorkerSerializer().bytesToObject(
                                    this.current.getBytes(), context.getWorkerResultClassName());
                        }
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        });
        LOG.info("Application {} container {} iteration {} waiting ends with {}ms execution time.", context.getAppId(),
                context.getContainerId(), context.getCurrentIteration(),
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
    }

    /**
     * Update master computable result to master znode. At the same time clean znodes for old iterations. Iteration 0
     * and last iteration will not be removed for fail over.
     */
    @Override
    public void postIteration(final MasterContext<MASTER_RESULT, WORKER_RESULT> context) {
        new BasicCoordinatorCommand() {
            @Override
            public void doExecute() throws KeeperException, InterruptedException {
                // set master result in each iteration.
                NettyMasterCoordinator.this.masterResult = context.getMasterResult();
                // update master halt status.
                updateMasterHaltStatus(context);

                // create master znode
                boolean isSplit = false;
                String appCurrentMasterNode = getCurrentMasterNode(context.getAppId(), context.getCurrentIteration())
                        .toString();
                String appCurrentMasterSplitNode = getCurrentMasterSplitNode(context.getAppId(),
                        context.getCurrentIteration()).toString();
                LOG.debug("master result:{}", context.getMasterResult());
                try {
                    byte[] bytes = getMasterSerializer().objectToBytes(context.getMasterResult());
                    isSplit = setBytesToZNode(appCurrentMasterNode, appCurrentMasterSplitNode, bytes,
                            CreateMode.PERSISTENT);
                    // after master result and status set in zookeeper, clear resources here at once for next iteration.
                    // there is race condition here, after master znode is visible, worker computes result and send
                    // results to master, while at here current iteration is still not next iteration.
                    synchronized(LOCK) {
                        clear(context.getProps());
                        // update current iteration to avoid receive messages of last iteration in ServerHandler
                        NettyMasterCoordinator.this.currentInteration = context.getCurrentIteration() + 1;
                        NettyMasterCoordinator.this.canUpdateWorkerResultMap = true;
                    }
                } catch (KeeperException.NodeExistsException e) {
                    LOG.warn("Has such node:", e);
                }

                // cleaning up znode, 0 is needed for fail-over.
                int resultCleanUpInterval = NumberFormatUtils.getInt(
                        context.getProps().getProperty(GuaguaConstants.GUAGUA_CLEANUP_INTERVAL),
                        GuaguaConstants.GUAGUA_DEFAULT_CLEANUP_INTERVAL);
                if(context.getCurrentIteration() >= (resultCleanUpInterval + 1)) {
                    String znode = getMasterNode(context.getAppId(),
                            context.getCurrentIteration() - resultCleanUpInterval).toString();
                    try {
                        getZooKeeper().deleteExt(znode, -1, false);
                        if(isSplit) {
                            znode = getCurrentMasterSplitNode(context.getAppId(),
                                    context.getCurrentIteration() - resultCleanUpInterval).toString();
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

    /**
     * Wait for unregister message for all workers and then clean all znodes existing for this job.
     */
    @Override
    public void postApplication(final MasterContext<MASTER_RESULT, WORKER_RESULT> context) {
        // update current iteration for unregister iteration
        synchronized(LOCK) {
            this.currentInteration = context.getCurrentIteration();
        }
        new BasicCoordinatorCommand() {
            @Override
            public void doExecute() throws Exception, InterruptedException {
                try {
                    // if clean up zk znodes cost two much running time, one can set zk cleanup flag. But to make sure
                    // clean the znodes manually after application.
                    String zkCleanUpEnabled = StringUtils.get(
                            context.getProps().getProperty(GuaguaConstants.GUAGUA_ZK_CLEANUP_ENABLE),
                            GuaguaConstants.GUAGUA_ZK_DEFAULT_CLEANUP_VALUE);

                    final long start = System.nanoTime();
                    if(Boolean.TRUE.toString().equalsIgnoreCase(zkCleanUpEnabled)) {
                        new RetryCoordinatorCommand(isFixedTime(), getSleepTime()) {
                            @Override
                            public boolean retryExecution() throws KeeperException, InterruptedException {
                                int doneWorkers;
                                synchronized(LOCK) {
                                    // long to int is assumed successful as no such many workers need using long
                                    doneWorkers = (int) NettyMasterCoordinator.this.iterResults.size();
                                }
                                if(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) > 120 * 1000L) {
                                    LOG.info(
                                            "unregister step, worker(s) compelted: {}, still {} workers are not unregistered, but time out to terminate.",
                                            doneWorkers, (context.getWorkers() - doneWorkers));
                                    return true;
                                }
                                // to avoid log flood
                                if(System.nanoTime() % 30 == 0) {
                                    LOG.info(
                                            "unregister step, worker(s) compelted: {}, still {} workers are not unregistered.",
                                            doneWorkers, (context.getWorkers() - doneWorkers));
                                }

                                // master should wait for all workers done in post application.
                                return isTerminated(doneWorkers, context.getWorkers(),
                                        GuaguaConstants.GUAGUA_DEFAULT_MIN_WORKERS_RATIO,
                                        GuaguaConstants.GUAGUA_DEFAULT_MIN_WORKERS_TIMEOUT);
                            }
                        }.execute();

                        // delete app znode
                        String appNode = getAppNode(context.getAppId()).toString();
                        try {
                            getZooKeeper().deleteExt(appNode, -1, true);
                        } catch (KeeperException.NoNodeException e) {
                            if(System.nanoTime() % 20 == 0) {
                                LOG.warn("No such node:{}", appNode);
                            }
                        }
                    }
                } finally {
                    if(NettyMasterCoordinator.this.messageServer != null) {
                        Method shutDownMethod = ReflectionUtils.getMethod(
                                NettyMasterCoordinator.this.messageServer.getClass(), "shutdown");
                        if(shutDownMethod != null) {
                            shutDownMethod.invoke(NettyMasterCoordinator.this.messageServer, (Object[]) null);
                        }
                        NettyMasterCoordinator.this.messageServer.releaseExternalResources();
                    }
                    NettyMasterCoordinator.super.closeZooKeeper();
                    NettyMasterCoordinator.this.iterResults.close();
                    NettyMasterCoordinator.this.iterResults.clear();
                }
            }
        }.execute();
    }
}
