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
package ml.shifu.guagua.master;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
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
import ml.shifu.guagua.io.Combinable;
import ml.shifu.guagua.io.HaltBytable;
import ml.shifu.guagua.io.NettyBytableDecoder;
import ml.shifu.guagua.io.NettyBytableEncoder;
import ml.shifu.guagua.io.Serializer;
import ml.shifu.guagua.util.AppendList;
import ml.shifu.guagua.util.BytableDiskList;
import ml.shifu.guagua.util.BytableMemoryDiskList;
import ml.shifu.guagua.util.ClassUtils;
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
 * Master still updates results to ZooKeeper znodes for fail-over. While workers sends results to master through Netty
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
     * Internal lock used for multiple thread synchronization of message results reading and writing.
     */
    private static final Object LOCK = new Object();

    /**
     * A server instance used to communicate with all workers.
     */
    private ServerBootstrap messageServer;

    /**
     * Message server port used to for Netty server to communicate with workers.
     */
    private int messageServerPort;

    /**
     * Worker results for each iteration. It should store each worker result in each iteration once.
     */
    private AppendList<WorkerResultWrapper> iterResults;

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

    /**
     * Cache worker class name for de-serialization in {@link ServerHandler}.
     */
    private String workerClassName = null;

    /**
     * Worker serializer used for {@link WorkerResultWrapper}. Should be set to static as {@link WorkerResultWrapper} is
     * static class.
     */
    private static Serializer<Bytable> serializer;

    /**
     * Merge internal elements together to save memory.
     */
    private class MergeWorkerResultList extends LinkedList<WorkerResultWrapper> implements
            AppendList<WorkerResultWrapper> {

        public MergeWorkerResultList(int threshold) {
            if(threshold <= 0) {
                throw new IllegalArgumentException("Threshold cannot be <= 0.");
            }
            this.threshold = threshold;
        }

        private static final long serialVersionUID = -33662960498334446L;

        private int rawSize;

        private final int threshold;

        private int currIndex = 0;

        /*
         * (non-Javadoc)
         * 
         * @see java.util.LinkedList#add(java.lang.Object)
         */
        @Override
        public synchronized boolean add(WorkerResultWrapper e) {
            this.rawSize += 1;
            if(e.isWorkerCombinable()) {
                this.currIndex += 1;
                if(this.currIndex == this.threshold - 1) {
                    while(this.currIndex > 1) {
                        e.combine(this.removeLast());
                        this.currIndex -= 1;
                    }
                    if(currIndex != 1) {
                        throw new IllegalStateException();
                    }
                    return super.add(e);
                } else {
                    return super.add(e);
                }
            } else {
                return super.add(e);
            }
        }

        @Override
        public synchronized int size() {
            return this.rawSize;
        }

        public synchronized int mergedSize() {
            return super.size();
        }

        /*
         * (non-Javadoc)
         * 
         * @see ml.shifu.guagua.util.AppendList#append(java.lang.Object)
         */
        @Override
        public synchronized boolean append(WorkerResultWrapper t) {
            return add(t);
        }

        /*
         * (non-Javadoc)
         * 
         * @see ml.shifu.guagua.util.AppendList#switchState()
         */
        @Override
        public synchronized void switchState() {
            // no state change
        }

        @SuppressWarnings("unused")
        public synchronized void close() {
            // empty for calling
        }

    }

    @Override
    protected void initialize(Properties props) {
        super.initialize(props);
        initIterResults(props);
    }

    private boolean isWorkerCombinable(String workerClassName) {
        try {
            return workerClassName != null && Combinable.class.isAssignableFrom(Class.forName(workerClassName));
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    private void initIterResults(Properties props) {
        synchronized(LOCK) {
            boolean nonSpill = "true".equalsIgnoreCase(props.getProperty(GuaguaConstants.GUAGUA_MASTER_RESULT_NONSPILL,
                    "true"));
            if(nonSpill && isWorkerCombinable(props.getProperty(GuaguaConstants.GUAGUA_WORKER_RESULT_CLASS))) {
                int mergeThreshold = NumberFormatUtils.getInt(
                        props.getProperty(GuaguaConstants.GUAGUA_MASTER_RESULT_MERGE_THRESHOLD, "10"), 10);
                this.iterResults = new MergeWorkerResultList(mergeThreshold);
            } else {
                BytableDiskList<WorkerResultWrapper> bytableDiskList = new BytableDiskList<WorkerResultWrapper>(
                        System.currentTimeMillis() + "", WorkerResultWrapper.class.getName());
                double memoryFraction = Double.valueOf(props.getProperty(
                        GuaguaConstants.GUAGUA_MASTER_WORKERESULTS_MEMORY_FRACTION,
                        GuaguaConstants.GUAGUA_MASTER_WORKERESULTS_DEFAULT_MEMORY_FRACTION));
                long memoryStoreSize = (long) (Runtime.getRuntime().maxMemory() * memoryFraction);
                LOG.info("Memory size in BytableMemoryDiskList for worker result list: {}", memoryStoreSize);
                this.iterResults = new BytableMemoryDiskList<WorkerResultWrapper>(memoryStoreSize, bytableDiskList);
            }
        }
    }

    /**
     * Do initialization and fail-over checking before all iterations.
     */
    @SuppressWarnings("unchecked")
    @Override
    public void preApplication(final MasterContext<MASTER_RESULT, WORKER_RESULT> context) {
        // Initialize zookeeper and other props
        initialize(context.getProps());

        // initialize serrializer for WorkerResultWrapper
        NettyMasterCoordinator.serializer = (Serializer<Bytable>) getWorkerSerializer();

        // cache worker class name
        this.workerClassName = context.getWorkerResultClassName();

        // init total iteration for later usage
        this.totalInteration = context.getTotalIteration();

        // Fail over checking to check current iteration.
        new FailOverCommand(context).execute();
        // if not init iteration, which means fail over from failed iteration, currentIteration is set in
        // FailOverCommand; we need recover last master result for MasterComputable fail over.
        if(!context.isInitIteration()) {
            new BasicCoordinatorCommand() {
                @Override
                public void doExecute() throws KeeperException, InterruptedException {
                    String appId = context.getAppId();
                    int lastIteration = context.getCurrentIteration();
                    final String appMasterNode = getCurrentMasterNode(appId, lastIteration).toString();
                    final String appMasterSplitNode = getCurrentMasterSplitNode(appId, lastIteration).toString();
                    // actually, this is last master result for fault recovery
                    setMasterResult(context, appMasterNode, appMasterSplitNode);
                }
            }.execute();
        }

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
            this.closeIterResults();
            this.iterResults.clear();
            this.initIterResults(props);
            this.indexMap.clear();
            this.canUpdateWorkerResultMap = true;
        }
    }

    // close iterResults by reflection to make sure safe calling
    private void closeIterResults() {
        try {
            Method closeMethod = ClassUtils.getDeclaredMethod("close", this.iterResults.getClass());
            if(closeMethod != null) {
                closeMethod.invoke(this.iterResults, (Object[]) null);
            }
        } catch (NoSuchMethodException e) {
            // ignore
        } catch (Exception e) {
            throw new RuntimeException(e);
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
        this.messageServer = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newFixedThreadPool(
                GuaguaConstants.GUAGUA_NETTY_SERVER_DEFAULT_THREAD_COUNT, new MasterThreadFactory()),
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
            LOG.debug("Received message size {}",
                    bytableWrapper != null && bytableWrapper.getBytes() != null ? bytableWrapper.getBytes().length : 0);
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
                            WorkerResultWrapper wrw = new WorkerResultWrapper(bytableWrapper.getCurrentIteration(),
                                    null, null);
                            // stop message is very small, can be located into memory
                            NettyMasterCoordinator.this.iterResults.append(wrw);
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
                        String clazzName = NettyMasterCoordinator.this.workerClassName;

                        WORKER_RESULT wr = NettyMasterCoordinator.this.getWorkerSerializer().bytesToObject(
                                bytableWrapper.getBytes(), clazzName);
                        WorkerResultWrapper wrw = new WorkerResultWrapper(bytableWrapper.getCurrentIteration(), wr,
                                clazzName);
                        NettyMasterCoordinator.this.iterResults.append(wrw);
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
            LOG.error("error in service handler", e.getCause());
            e.getChannel().close();
            Throwable cause = e.getCause();
            if(cause != null && cause instanceof GuaguaRuntimeException) {
                throw (GuaguaRuntimeException) cause;
            } else {
                throw new GuaguaRuntimeException(e.getCause());
            }
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

                long timeOut = 0L;
                if(context.isFirstIteration() || context.getCurrentIteration() == context.getTotalIteration()) {
                    // in the first iteration or last iteration, make sure all workers loading data successfully, set
                    // timeout to 60s to wait for more workers to be finished.
                    timeOut = GuaguaConstants.GUAGUA_DEFAULT_MIN_WORKERS_TIMEOUT;
                } else {
                    timeOut = context.getMinWorkersTimeOut();
                }

                boolean isTerminated = isTerminated(doneWorkers, context.getWorkers(), context.getMinWorkersRatio(),
                        timeOut);
                if(isTerminated) {
                    // update canUpdateWorkerResultsMap to false no accept other results
                    synchronized(LOCK) {
                        NettyMasterCoordinator.this.canUpdateWorkerResultMap = false;
                    }
                    LOG.info("Iteration {}, master waiting is terminated by workers {} doneWorkers {} "
                            + "minWorkersRatio {} minWorkersTimeOut {}.", context.getCurrentIteration(),
                            context.getWorkers(), doneWorkers, context.getMinWorkersRatio(),
                            GuaguaConstants.GUAGUA_DEFAULT_MIN_WORKERS_TIMEOUT);
                }
                return isTerminated;
            }
        }.execute();

        // switch state to read and not accept other results
        synchronized(LOCK) {
            this.canUpdateWorkerResultMap = false;
            this.iterResults.switchState();
        }

        if(this.iterResults instanceof BytableMemoryDiskList) {
            LOG.info("Worker result memory count in iteration {} is {}.", this.currentInteration,
                    ((BytableMemoryDiskList<WorkerResultWrapper>) this.iterResults).getMemoryCount());
            LOG.info("Worker result dist count in iteration {} is {}.", this.currentInteration,
                    ((BytableMemoryDiskList<WorkerResultWrapper>) this.iterResults).getDiskCount());
        } else {
            LOG.info("Worker result merge list raw and merged count in iteration {} are {}, {}.",
                    this.currentInteration, ((MergeWorkerResultList) this.iterResults).size(),
                    ((MergeWorkerResultList) this.iterResults).mergedSize());
        }
        // set worker results.
        final int currentIter = this.currentInteration;
        context.setWorkerResults(new Iterable<WORKER_RESULT>() {
            @Override
            public Iterator<WORKER_RESULT> iterator() {
                return new Iterator<WORKER_RESULT>() {

                    private Iterator<WorkerResultWrapper> localItr;

                    private volatile AtomicBoolean isStart = new AtomicBoolean();

                    WorkerResultWrapper current = null;

                    @Override
                    public boolean hasNext() {
                        boolean hasNext;
                        synchronized(LOCK) {
                            if(this.isStart.compareAndSet(false, true)) {
                                this.localItr = NettyMasterCoordinator.this.iterResults.iterator();
                            }
                            hasNext = this.localItr.hasNext();
                            if(hasNext) {
                                this.current = this.localItr.next();
                                // check if iteration number is the same, remove unnecessary results.
                                while(this.current.currIter != currentIter) {
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

                    @SuppressWarnings("unchecked")
                    @Override
                    public WORKER_RESULT next() {
                        synchronized(LOCK) {
                            return (WORKER_RESULT) this.current.workerResult;
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
                final long start = System.nanoTime();
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
                LOG.debug("set results to zookeeper with time {}ms",
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));

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

                    // master unregister timeout in second, by default 200s
                    final int masterUnregisterTimeout = NumberFormatUtils.getInt(context.getProps().getProperty(
                            GuaguaConstants.GUAGUA_UNREGISTER_MASTER_TIMEROUT, "200000"));
                    LOG.info("guagua master un register timeout is {}", masterUnregisterTimeout);
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
                                if(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) > masterUnregisterTimeout) {
                                    LOG.info("unregister step, worker(s) compelted: {}, still {} workers are "
                                            + "not unregistered, but time out to terminate.", doneWorkers,
                                            (context.getWorkers() - doneWorkers));
                                    return true;
                                }
                                // to avoid log flood
                                if(System.nanoTime() % 30 == 0) {
                                    LOG.info("unregister step, worker(s) compelted: {}, still {} workers are "
                                            + "not unregistered.", doneWorkers, (context.getWorkers() - doneWorkers));
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
                    NettyMasterCoordinator.this.closeIterResults();
                    NettyMasterCoordinator.this.iterResults.clear();
                }
            }
        }.execute();
    }

    /**
     * Wrapper worker result for master merging.
     * 
     * <p>
     * {@link Combinable} is used to combine worker results when received.
     */
    private static class WorkerResultWrapper implements Bytable, Combinable<WorkerResultWrapper> {

        private int currIter;

        private Bytable workerResult;

        private String className;

        public WorkerResultWrapper(int currIter, Bytable workerResult, String className) {
            this.currIter = currIter;
            this.workerResult = workerResult;
            this.className = className;
        }

        public boolean isWorkerCombinable() {
            try {
                return className != null && workerResult != null
                        && Combinable.class.isAssignableFrom(Class.forName(className));
            } catch (ClassNotFoundException e) {
                return false;
            }
        }

        @Override
        public WorkerResultWrapper combine(WorkerResultWrapper wrw) {
            if(isWorkerCombinable()) {
                @SuppressWarnings("unchecked")
                Combinable<Bytable> cwr = ((Combinable<Bytable>) workerResult);
                cwr.combine(wrw.workerResult);
            }
            return this;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(this.currIter);
            if(this.className == null) {
                out.writeInt(0);
            } else {
                writeBytes(out, this.className.getBytes(Charset.forName("UTF-8")));
            }
            if(this.workerResult == null) {
                out.writeInt(0);
            } else {
                byte[] wrBytes = NettyMasterCoordinator.serializer.objectToBytes(this.workerResult);
                writeBytes(out, wrBytes);
            }
        }

        private void writeBytes(DataOutput out, byte[] bytes) throws IOException {
            out.writeInt(bytes.length);
            for(int i = 0; i < bytes.length; i++) {
                out.writeByte(bytes[i]);
            }
        }

        /*
         * (non-Javadoc)
         * 
         * @see ml.shifu.guagua.io.Bytable#readFields(java.io.DataInput)
         */
        @Override
        public void readFields(DataInput in) throws IOException {
            this.currIter = in.readInt();
            int classNameLen = in.readInt();
            if(classNameLen != 0) {
                byte[] classNameBytes = new byte[classNameLen];
                for(int i = 0; i < classNameBytes.length; i++) {
                    classNameBytes[i] = in.readByte();
                }
                this.className = new String(classNameBytes, Charset.forName("UTF-8"));
            } else {
                this.className = null;
            }
            int bytesSize = in.readInt();
            if(bytesSize != 0) {
                byte[] wrBytes = new byte[bytesSize];
                for(int i = 0; i < wrBytes.length; i++) {
                    wrBytes[i] = in.readByte();
                }
                this.workerResult = NettyMasterCoordinator.serializer.bytesToObject(wrBytes, className);
            }
        }

    }

}
