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
package ml.shifu.guagua.yarn;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import ml.shifu.guagua.GuaguaConstants;
import ml.shifu.guagua.hadoop.io.GuaguaInputSplit;
import ml.shifu.guagua.yarn.util.GsonUtils;
import ml.shifu.guagua.yarn.util.YarnUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelEvent;
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
import org.jboss.netty.handler.codec.serialization.ClassResolvers;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
 * {@link GuaguaAppMaster} is application master to launch master and worker tasks.
 * 
 * <p>
 * This app master is used to check and launch all tasks, not to run the master task of distributed training. Master
 * task is run on another container.
 * 
 * <p>
 * TODO: Web monitor is not supported in current app master.
 * 
 * <p>
 * Fail-over is added like mapreduce. If one container is failed, it will be launched until 4 times by default.
 * 
 * <p>
 * In each container, use a number start from 1 to mark as the id of the container for fail-over.
 */
public class GuaguaAppMaster {

    private static final Logger LOG = LoggerFactory.getLogger(GuaguaAppMaster.class);

    /** Exit code for YARN containers that were manually killed/aborted */
    private static final int YARN_ABORT_EXIT_STATUS = -100;
    /** Exit code for successfully run YARN containers */
    private static final int YARN_SUCCESS_EXIT_STATUS = 0;
    /** millis to sleep between heartbeats during long loops */
    private static final int SLEEP_BETWEEN_HEARTBEATS_MSECS = 900;

    /**
     * Container id for current master container
     */
    private ContainerId masterContainerId;

    /**
     * App attempt id
     */
    private ApplicationAttemptId appAttemptId;

    /**
     * Whether master is done.
     */
    private volatile boolean done;

    /**
     * Yarn conf
     */
    private Configuration yarnConf;

    /**
     * Number of completed containers.
     */
    private AtomicInteger completedCount;

    /**
     * Number of failed containers.
     */
    private AtomicInteger failedCount;

    /**
     * Number of allocated containers.
     */
    private AtomicInteger allocatedCount;

    /**
     * Number of successful containers.
     */
    private AtomicInteger successfulCount;

    /**
     * Number of completed containers.
     */
    private int containersToLaunch;

    /**
     * executor to launch container.
     */
    private ExecutorService executor;

    /**
     * executor to check whether task is time out.
     */
    private ExecutorService taskTimeoutExecutor;

    /**
     * Like mapred.task.timeout, if no update on this time, container will be killed.
     */
    private long taskTimeOut = GuaguaYarnConstants.DEFAULT_TIME_OUT;

    /**
     * Heap memory setting for worker container.
     */
    private int heapPerContainer;

    /**
     * Handle to communicate with resource manager.
     */
    private AMRMClientAsync<ContainerRequest> amRmClient;
    /**
     * Handle to communicate with the Node Manager
     */
    private NMClientAsync nmClientAsync;
    /**
     * Listen to process the response from the Node Manager
     */
    private NMCallbackHandler containerListener;

    /**
     * A reusable map of resources already in HDFS for each task to copy-to-local env and use to launch each
     * GuaguaYarnTask.
     */
    private static Map<String, LocalResource> localResources;

    /**
     * For status update for clients - yet to be implemented\\
     * Hostname of the container
     */
    private String appMasterHostname;
    /** Port on which the app master listens for status updates from clients */
    private int appMasterRpcPort = 1234;
    /** Tracking url to which app master publishes info for clients to monitor */
    private String appMasterTrackingUrl = "";

    /**
     * Setting container args
     */
    private String containerArgs;

    private List<InputSplit> inputSplits;

    private ApplicationId appId;

    private Map<Integer, List<Container>> partitionContainerMap;

    private Map<String, Integer> containerPartitionMap;

    private static enum PartitionStatus {
        INIT, SUCCESSFUL, FAILED, RETRY,
    }

    private Map<Integer, PartitionStatus> partitionStatusMap;

    private List<Integer> failedPartitions;

    private AtomicInteger partitionIndex;

    private int maxContainerAttempts;

    private int totalIterations;

    private ByteBuffer allTokens;

    private int rpcPort = GuaguaYarnConstants.DEFAULT_STATUS_RPC_PORT;

    private String rpcHostName;

    private Map<Integer, GuaguaIterationStatus> partitionProgress;

    private ServerBootstrap rpcServer;

    static {
        // pick up new conf XML file and populate it with stuff exported from client
        Configuration.addDefaultResource(GuaguaYarnConstants.GUAGUA_CONF_FILE);
    }

    /**
     * Construct the GuaguappMaster, populate fields using env vars and set up by YARN framework in this execution
     * container.
     * 
     * @param cId
     *            the ContainerId
     * @param aId
     *            the ApplicationAttemptId
     */
    public GuaguaAppMaster(ContainerId cId, ApplicationAttemptId aId, Configuration conf) {
        try {
            this.rpcHostName = this.appMasterHostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            LOG.error("Error in getting local host name.", e);
        }

        this.masterContainerId = cId; // future good stuff will need me to operate.
        this.appAttemptId = aId;
        this.appId = this.getAppAttemptId().getApplicationId();
        this.yarnConf = conf;
        this.completedCount = new AtomicInteger(0);
        this.failedCount = new AtomicInteger(0);
        this.allocatedCount = new AtomicInteger(0);
        this.successfulCount = new AtomicInteger(0);

        this.partitionContainerMap = new ConcurrentHashMap<Integer, List<Container>>();
        this.containerPartitionMap = new ConcurrentHashMap<String, Integer>();
        this.partitionStatusMap = new ConcurrentHashMap<Integer, GuaguaAppMaster.PartitionStatus>();
        this.partitionIndex = new AtomicInteger(0);
        this.failedPartitions = new CopyOnWriteArrayList<Integer>();;
        this.maxContainerAttempts = this.getYarnConf().getInt(GuaguaYarnConstants.GUAGUA_YARN_MAX_CONTAINER_ATTEMPTS,
                GuaguaYarnConstants.GUAGUA_YARN_DEFAULT_MAX_CONTAINER_ATTEMPTS);

        this.heapPerContainer = this.getYarnConf().getInt(GuaguaYarnConstants.GUAGUA_CHILD_MEMORY,
                GuaguaYarnConstants.GUAGUA_CHILD_DEFAULT_MEMORY);
        this.totalIterations = this.getYarnConf().getInt(GuaguaConstants.GUAGUA_ITERATION_COUNT, 1);
        String containerArgs = this.getYarnConf().get(GuaguaYarnConstants.GUAGUA_YARN_CONTAINER_ARGS);
        if(containerArgs == null) {
            containerArgs = GuaguaYarnConstants.GUAGUA_YARN_DEFAULT_CONTAINER_JAVA_OPTS;
        } else {
            containerArgs = GuaguaYarnConstants.GUAGUA_YARN_DEFAULT_CONTAINER_JAVA_OPTS + " " + containerArgs;
        }
        this.containerArgs = containerArgs;

        this.rpcPort = getYarnConf().getInt(GuaguaYarnConstants.GUAGUA_YARN_STATUS_RPC_PORT,
                GuaguaYarnConstants.DEFAULT_STATUS_RPC_PORT);

        this.partitionProgress = new ConcurrentHashMap<Integer, GuaguaIterationStatus>();

        this.executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        this.taskTimeoutExecutor = Executors.newSingleThreadExecutor();
        this.taskTimeOut = getYarnConf().getLong(GuaguaYarnConstants.GUAGUA_TASK_TIMEOUT,
                GuaguaYarnConstants.DEFAULT_TIME_OUT);

        LOG.info("{}:{}", taskTimeOut, GuaguaYarnConstants.DEFAULT_TIME_OUT);
        LOG.info("GuaguaAppMaster  for ContainerId {} ApplicationAttemptId {}", cId, aId);
    }

    /**
     * Coordinates all requests for guagua's worker/master task containers, and manages application liveness heartbeat,
     * completion status, teardown, etc.
     * 
     * @return success or failure
     */
    public boolean run() throws YarnException, IOException {
        boolean success = false;
        try {
            // 1. get input from conf, generate input splits like GuaguaMapReduce
            // 2. store all splits into conf and export to hdfs
            // 3. for each conntainer, according to container host and split host to select a partition to the container
            // add transfer partition as program args.
            // 4. Store <partition, failed container number>
            // 5. If one container failed, try to request another container
            // 6. for containers get, try failed partition firstly
            prepareInputSplits();

            // set tokens to make app master and task work well.
            getAllTokens();

            registerRMCallBackHandler();

            registerNMCallbackHandler();

            registerAMToRM();

            startRPCServer();

            startTaskTimeoutExecutor();

            madeAllContainerRequestToRM();

            LOG.info("Wait to finish ..");
            while(!isDone()) {
                try {
                    Thread.sleep(SLEEP_BETWEEN_HEARTBEATS_MSECS);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
            LOG.info("Done {}", isDone());
        } finally {
            shutdown();
            success = finish();
        }
        return success;
    }

    private void startTaskTimeoutExecutor() {
        this.taskTimeoutExecutor.submit(new Runnable() {
            @Override
            public void run() {
                while(true) {
                    try {
                        Thread.sleep(GuaguaAppMaster.this.taskTimeOut);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                    LOG.debug(GuaguaAppMaster.this.partitionProgress.toString());
                    for(Map.Entry<Integer, GuaguaIterationStatus> entry: GuaguaAppMaster.this.partitionProgress
                            .entrySet()) {
                        GuaguaIterationStatus status = entry.getValue();
                        // doesn't work in the first iteration
                        if(status.getTime() != 0l && status.getCurrentIteration() != 1
                                && (System.currentTimeMillis() - status.getTime()) > GuaguaAppMaster.this.taskTimeOut) {
                            List<Container> containers = GuaguaAppMaster.this.partitionContainerMap.get(entry.getKey());
                            Container container = containers.get(containers.size() - 1);
                            LOG.info(
                                    "Container {} is timeout with timeout period {}, will be killed by node manager {}.",
                                    container.getId(), GuaguaAppMaster.this.taskTimeOut, container.getNodeId());
                            GuaguaAppMaster.this.getNmClientAsync().stopContainerAsync(container.getId(),
                                    container.getNodeId());
                        }
                    }
                }
            }
        });
    }

    protected void shutdown() {
        // if we get here w/o problems, the executor is already long finished.
        if(null != getExecutor() && !getExecutor().isTerminated()) {
            LOG.info("Forcefully terminating executors with done ={}", isDone());
            getExecutor().shutdownNow(); // force kill, especially if got here by throw
        }
        if(this.rpcServer != null) {
            this.rpcServer.shutdown();
            this.rpcServer.releaseExternalResources();
        }
        if(this.taskTimeoutExecutor != null) {
            this.taskTimeoutExecutor.shutdownNow();
        }
    }

    /**
     * Start rpc server which is used to update progress.
     */
    private void startRPCServer() {
        this.rpcServer = new ServerBootstrap(new NioServerSocketChannelFactory(
                Executors.newFixedThreadPool(GuaguaYarnConstants.DEFAULT_STATUS_RPC_SERVER_THREAD_COUNT),
                Executors.newFixedThreadPool(GuaguaYarnConstants.DEFAULT_STATUS_RPC_SERVER_THREAD_COUNT)));

        // Set up the pipeline factory.
        this.rpcServer.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() throws Exception {
                return Channels.pipeline(new ObjectEncoder(),
                        new ObjectDecoder(ClassResolvers.cacheDisabled(getClass().getClassLoader())),
                        new ServerHandler());
            }
        });

        // Bind and start to accept incoming connections.
        this.rpcServer.bind(new InetSocketAddress(rpcPort));
    }

    /**
     * {@link ServerHandler} is used to receive message and update progress for this yarn app.
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
            GuaguaIterationStatus status = GsonUtils.fromJson(e.getMessage().toString(), GuaguaIterationStatus.class);
            LOG.debug("Receive RPC status:{}", status);
            GuaguaAppMaster.this.partitionProgress.put(status.getPartition(), status);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
            e.getChannel().close();
        }
    }

    /**
     * Prepare input splits for containers
     */
    private void prepareInputSplits() throws IOException {
        this.inputSplits = getNewSplits(getYarnConf());

        this.setContainersToLaunch(this.inputSplits.size());

        LOG.info("Input split size including master: {}", this.inputSplits.size());
    }

    public List<InputSplit> getNewSplits(Configuration conf) throws IOException {
        int masters = conf.getInt(GuaguaConstants.GUAGUA_MASTER_NUMBER, GuaguaConstants.DEFAULT_MASTER_NUMBER);
        int size = getYarnConf().getInt(GuaguaConstants.GUAGUA_WORKER_NUMBER, 0) + masters;
        List<InputSplit> newSplits = new ArrayList<InputSplit>(size);
        for(int i = 1; i <= size; i++) {
            newSplits.add(GsonUtils.fromJson(getYarnConf().get(GuaguaYarnConstants.GUAGUA_YARN_INPUT_SPLIT_PREFIX + i),
                    GuaguaInputSplit.class));
            this.partitionProgress.put(i, new GuaguaIterationStatus());
        }
        return newSplits;
    }

    /**
     * Populate allTokens with the tokens received
     */
    private void getAllTokens() throws IOException {
        Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
        DataOutputBuffer dob = new DataOutputBuffer();
        credentials.writeTokenStorageToStream(dob);
        // Now remove the AM->RM token so that containers cannot access it.
        Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
        while(iter.hasNext()) {
            Token<?> token = iter.next();
            if(LOG.isDebugEnabled()) {
                LOG.debug("Token type : {}", token.getKind());
            }
            if(token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
                iter.remove();
            }
        }
        this.allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    }

    /**
     * Register RM callback and start listening
     */
    private void registerRMCallBackHandler() {
        AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
        setAmRMClient(AMRMClientAsync.createAMRMClientAsync(1000, allocListener));
        getAmRMClient().init(getYarnConf());
        getAmRMClient().start();
    }

    /**
     * Register NM callback and start listening
     */
    private void registerNMCallbackHandler() {
        setContainerListener(new NMCallbackHandler());
        setNmClientAsync(new NMClientAsyncImpl(getContainerListener()));
        getNmClientAsync().init(getYarnConf());
        getNmClientAsync().start();
    }

    /**
     * Register AM to RM
     * 
     * @return AM register response
     */
    private RegisterApplicationMasterResponse registerAMToRM() throws YarnException {
        // register Application Master with the YARN Resource Manager so we can begin requesting resources.
        try {
            if(UserGroupInformation.isSecurityEnabled()) {
                LOG.info("SECURITY ENABLED ");
            }
            RegisterApplicationMasterResponse response = getAmRMClient().registerApplicationMaster(
                    this.appMasterHostname, this.appMasterRpcPort, this.appMasterTrackingUrl);
            return response;
        } catch (IOException ioe) {
            throw new IllegalStateException("GuaguaAppMaster failed to register with RM.", ioe);
        }
    }

    /**
     * Add all containers' request
     */
    private void madeAllContainerRequestToRM() {
        // Setup ask for containers from RM
        // Send request for containers to RM Until we get our fully allocated quota, we keep on polling RM for
        // containers. Keep looping until all the containers are launched and shell script executed on them ( regardless
        // of success/failure).
        for(int i = 0; i < getContainersToLaunch(); i++) {
            ContainerRequest containerAsk = setupContainerAskForRM();
            getAmRMClient().addContainerRequest(containerAsk);
        }
    }

    private void madeOneContainerRequestToRM() {
        ContainerRequest containerAsk = setupContainerAskForRM();
        getAmRMClient().addContainerRequest(containerAsk);
    }

    /**
     * Setup the request that will be sent to the RM for the container ask.
     * 
     * @return the setup ResourceRequest to be sent to RM
     */
    private ContainerRequest setupContainerAskForRM() {
        // setup requirements for hosts, request containers firstly and then check allocated containers and splits to
        // get data locality.
        // TODO, better here to requests according to hosts of splits.
        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(GuaguaYarnConstants.GUAGUA_YARN_DEFAULT_PRIORITY);

        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(getHeapPerContainer());
        capability.setVirtualCores(getYarnConf().getInt(GuaguaYarnConstants.GUAGUA_YARN_TASK_VCORES,
                GuaguaYarnConstants.GUAGUA_YARN_TASK_DEFAULT_VCORES));

        ContainerRequest request = new ContainerRequest(capability, null, null, pri);
        LOG.info("Requested container ask: {}", request.toString());
        return request;
    }

    /**
     * Call when the application is done
     * 
     * @return if all containers succeed
     */
    private boolean finish() {
        // When the application completes, it should stop all running containers
        LOG.info("Application completed. Stopping running containers");
        getNmClientAsync().stop();

        // When the application completes, it should send a finish application
        // signal to the RM
        LOG.info("Application completed. Signalling finish to RM");
        FinalApplicationStatus appStatus;
        String appMessage = null;
        boolean success = true;
        if(getSuccessfulCount().get() == getContainersToLaunch()) {
            appStatus = FinalApplicationStatus.SUCCEEDED;
        } else {
            appStatus = FinalApplicationStatus.FAILED;
            appMessage = String.format("Diagnostics total=%s, completed=%s, failed=%s.", getContainersToLaunch(),
                    getCompletedCount().get(), getFailedCount().get());
            success = false;
        }
        try {
            getAmRMClient().unregisterApplicationMaster(appStatus, appMessage, this.appMasterTrackingUrl);
        } catch (YarnException ex) {
            LOG.error("Failed to unregister application", ex);
        } catch (IOException e) {
            LOG.error("Failed to unregister application", e);
        }

        getAmRMClient().stop();
        return success;
    }

    /**
     * CallbackHandler to process RM async calls
     */
    private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
        @Override
        public void onContainersCompleted(List<ContainerStatus> completedContainers) {
            LOG.info("Got response from RM for container ask, completedCnt={}", completedContainers.size());
            for(ContainerStatus containerStatus: completedContainers) {
                LOG.info("Got container status for containerID={}, state={}, exitStatus={}, diagnostics={}.",
                        containerStatus.getContainerId(), containerStatus.getState(), containerStatus.getExitStatus(),
                        containerStatus.getDiagnostics());
                int partition = GuaguaAppMaster.this.containerPartitionMap.get(containerStatus.getContainerId()
                        .toString());
                if(GuaguaAppMaster.this.partitionContainerMap.get(partition).size() >= GuaguaAppMaster.this.maxContainerAttempts) {
                    setDone(true);
                    LOG.info("One partition {} has more than max attempt {} ", partition,
                            GuaguaAppMaster.this.maxContainerAttempts);
                    return;
                }
                switch(containerStatus.getExitStatus()) {
                    case YARN_SUCCESS_EXIT_STATUS:
                        GuaguaAppMaster.this.partitionStatusMap.put(partition, PartitionStatus.SUCCESSFUL);
                        getSuccessfulCount().incrementAndGet();
                        break;
                    case YARN_ABORT_EXIT_STATUS:
                        LOG.info("YARN_ABORT_EXIT_STATUS: Container id {} exits with {}",
                                containerStatus.getContainerId(), YARN_ABORT_EXIT_STATUS);
                        break; // not success or fail
                    default:
                        LOG.info("default: Container id {} exits with {}", containerStatus.getContainerId(),
                                containerStatus.getExitStatus());
                        GuaguaAppMaster.this.partitionStatusMap.put(partition, PartitionStatus.FAILED);
                        GuaguaAppMaster.this.failedPartitions.add(partition);
                        GuaguaAppMaster.this.madeOneContainerRequestToRM();
                        getFailedCount().incrementAndGet();
                        break;
                }
                getCompletedCount().incrementAndGet();
            }

            if(getSuccessfulCount().get() == getContainersToLaunch()) {
                setDone(true);
                LOG.info("All container compeleted. done = {} ", isDone());
            } else {
                LOG.info(
                        "After completion of one conatiner. current status is: completedCount:{} containersToLaunch:{} successfulCount:{} failedCount:{}.",
                        getCompletedCount().get(), getContainersToLaunch(), getSuccessfulCount().get(),
                        getFailedCount().get());
            }
        }

        @Override
        public void onContainersAllocated(List<Container> allocatedContainers) {
            LOG.info("Got response from RM for container ask, allocatedCnt={}", allocatedContainers.size());
            getAllocatedCount().addAndGet(allocatedContainers.size());
            LOG.info("Total allocated # of container so far {} : allocated out of required {}.", getAllocatedCount()
                    .get(), getContainersToLaunch());
            startContainerLaunchingThreads(allocatedContainers);
        }

        @Override
        public void onShutdownRequest() {
            setDone(true);
            getAmRMClient().stop();
        }

        @Override
        public void onNodesUpdated(List<NodeReport> updatedNodes) {
        }

        @Override
        public float getProgress() {
            // set progress to deliver to RM on next heartbeat
            int sum = 0, totalSum = 0;
            for(Map.Entry<Integer, GuaguaIterationStatus> entry: GuaguaAppMaster.this.partitionProgress.entrySet()) {
                sum += entry.getValue().getCurrentIteration();
                totalSum += GuaguaAppMaster.this.totalIterations;
            }
            return (sum * 1.0f) / totalSum;
        }

        @Override
        public void onError(Throwable e) {
            setDone(true);
            getAmRMClient().stop();
        }
    }

    /**
     * For each container successfully allocated, attempt to set up and launch a Guagua worker/master task.
     * 
     * @param allocatedContainers
     *            the containers we have currently allocated.
     */
    private void startContainerLaunchingThreads(final List<Container> allocatedContainers) {
        Map<String, List<Container>> hostContainterMap = getHostContainersMap(allocatedContainers);
        int size = allocatedContainers.size();
        while(size > 0) {
            int currentPartition = getCurrentPartition();
            if(currentPartition == -1) {
                LOG.warn("Request too many resources. TODO, remove containers no needed.");
                break;
            }
            Container container = getDataLocalityContainer(hostContainterMap, currentPartition);
            if(container == null) {
                container = allocatedContainers.get(0);
            }

            allocatedContainers.remove(container);

            LOG.info(
                    "Launching command on a new container., containerId={}, containerNode={}, containerPort={}, containerNodeURI={}, containerResourceMemory={}",
                    container.getId(), container.getNodeId().getHost(), container.getNodeId().getPort(),
                    container.getNodeHttpAddress(), container.getResource().getMemory());

            List<Container> list = this.partitionContainerMap.get(currentPartition);
            if(list == null) {
                list = new ArrayList<Container>();
            }
            list.add(container);
            this.partitionContainerMap.put(currentPartition, list);
            this.containerPartitionMap.put(container.getId().toString(), currentPartition);
            this.partitionStatusMap.put(currentPartition, PartitionStatus.INIT);
            LaunchContainerRunnable runnableLaunchContainer = new LaunchContainerRunnable(container,
                    getContainerListener(), currentPartition);
            getExecutor().execute(runnableLaunchContainer);

            size = allocatedContainers.size();
        }
    }

    private Map<String, List<Container>> getHostContainersMap(final List<Container> allocatedContainers) {
        Map<String, List<Container>> hostContainterMap = new HashMap<String, List<Container>>();
        for(Container container: allocatedContainers) {
            String host = container.getNodeId().getHost();
            List<Container> containers = hostContainterMap.get(host);
            if(containers == null) {
                containers = new ArrayList<Container>();
            }
            containers.add(container);
            hostContainterMap.put(host, containers);
        }
        return hostContainterMap;
    }

    /**
     * Find a container with the same host for input split. Not a good implementation for data locality. Check
     * map-reduce implementation.
     * 
     * TODO RACK-LOCAL implementation
     */
    private Container getDataLocalityContainer(Map<String, List<Container>> hostContainterMap, int currentPartition) {
        GuaguaInputSplit inputSplit = (GuaguaInputSplit) (this.inputSplits.get(currentPartition - 1));
        String host = null;
        FileSplit[] fileSplits = inputSplit.getFileSplits();
        if(fileSplits != null) {
            try {
                host = fileSplits[0].getLocations()[0];
            } catch (Exception mayNotHappen) {
                host = null;
            }
        }

        List<Container> containers = hostContainterMap.get(host);
        Container container = null;
        if(containers != null && !containers.isEmpty()) {
            container = containers.remove(0);
            hostContainterMap.put(host, containers);
            LOG.info("find a container {} with host {} for partition {} and split {}.", container, host,
                    currentPartition, inputSplit);
            return container;
        }

        // if not find a container, try to choose the first one.
        Set<Entry<String, List<Container>>> entrySet = hostContainterMap.entrySet();
        String firstHost = null;
        List<Container> firstContainers = null;
        for(Entry<String, List<Container>> entry: entrySet) {
            firstHost = entry.getKey();
            firstContainers = entry.getValue();
            if(firstContainers != null && !firstContainers.isEmpty()) {
                container = firstContainers.remove(0);
                break;
            }
        }
        hostContainterMap.put(firstHost, firstContainers);
        LOG.info("find a container {} with host {} for partition {} and split {}.", container, host, currentPartition,
                inputSplit);
        return container;
    }

    private int getCurrentPartition() {
        LOG.info("failed container request size:{} {}", this.failedPartitions.size(), this.failedPartitions);
        Iterator<Integer> it = this.failedPartitions.iterator();

        // Launch and start the container on a separate thread to keep the main thread unblocked as all containers
        // may not be allocated at one go.
        int currentPartition = 0;
        if(it.hasNext()) {
            currentPartition = it.next();
            // because we use CopyOnWriteArrayList, we can remove object in iteration
            this.failedPartitions.remove(Integer.valueOf(currentPartition));
            LOG.info("failed container request size after remove:{} {}", this.failedPartitions.size(),
                    this.failedPartitions);
        } else {
            LOG.info("partitionIndex{} containersToLaunch {}", this.partitionIndex.get(), this.containersToLaunch);
            if(this.partitionIndex.get() >= this.containersToLaunch) {
                return -1;
            }
            currentPartition = this.partitionIndex.addAndGet(1);
        }
        return currentPartition;
    }

    /**
     * Thread to connect to the {@link ContainerManager} and launch the container that will house one of our Guagua
     * worker (or master) tasks.
     */
    private class LaunchContainerRunnable implements Runnable {
        /** Allocated container */
        private Container container;
        /** NM listener */
        private NMCallbackHandler containerListener;

        private final int partition;

        /**
         * Constructor.
         * 
         * @param container
         *            Allocated container
         * @param containerListener
         *            container listener.
         */
        public LaunchContainerRunnable(final Container container, NMCallbackHandler containerListener, int partition) {
            this.container = container;
            this.containerListener = containerListener;
            this.partition = partition;
        }

        /**
         * Connects to CM, sets up container launch context for shell command and eventually dispatches the container
         * start request to the CM.
         */
        @Override
        public void run() {
            // Connect to ContainerManager
            // configure the launcher for the guagua task it will host
            ContainerLaunchContext ctx = buildContainerLaunchContext();
            // request CM to start this container as spec'd in ContainerLaunchContext
            this.containerListener.addContainer(this.container.getId(), this.container);
            getNmClientAsync().startContainerAsync(this.container, ctx);
        }

        /**
         * Boilerplate to set up the ContainerLaunchContext to tell the Container Manager how to launch our guagua task
         * in the execution container we have already allocated.
         * 
         * @return a populated ContainerLaunchContext object.
         */
        private ContainerLaunchContext buildContainerLaunchContext() {
            LOG.info("Setting up container launch container for containerid={}", container.getId());
            ContainerLaunchContext launchContext = Records.newRecord(ContainerLaunchContext.class);

            // args inject the CLASSPATH, heap MB, and TaskAttemptID for launched task
            final List<String> commands = generateShellExecCommand();
            LOG.info("Conatain launch Commands :{}" + commands);
            launchContext.setCommands(commands);
            // Set up tokens for the container too. We are populating them mainly for NodeManagers to be able to
            // download any files in the distributed file-system. The tokens are otherwise also useful in cases, for
            // e.g., when one is running a "hadoop dfs" like command
            launchContext.setTokens(allTokens.slice());

            // Set the environment variables to inject into remote task's container
            buildEnvironment(launchContext);

            // Set the local resources: just send the copies already in HDFS
            launchContext.setLocalResources(getTaskResourceMap());
            return launchContext;
        }

        /**
         * Generates our command line string used to launch our guagua tasks.
         * 
         * @return the BASH shell commands to launch the job.
         */
        private List<String> generateShellExecCommand() {
            String programArgs = new StringBuilder(300)
                    .append(getAppAttemptId().getApplicationId().getClusterTimestamp()).append(" ")
                    .append(getAppAttemptId().getApplicationId().getId()).append(" ")
                    .append(this.container.getId().getId()).append(" ").append(getAppAttemptId().getAttemptId())
                    .append(" ").append(this.partition).append(" ").append(GuaguaAppMaster.this.rpcHostName)
                    .append(" ").append(GuaguaAppMaster.this.rpcPort).toString();
            return YarnUtils.getCommand(GuaguaYarnTask.class.getName(), GuaguaAppMaster.this.containerArgs,
                    programArgs, getHeapPerContainer() + "");
        }

        /**
         * Utility to populate the environment vars we wish to inject into the new containter's env when the guagua BSP
         * task is executed.
         * 
         * @param launchContext
         *            the launch context which will set our environment vars in the app master's execution container.
         */
        private void buildEnvironment(final ContainerLaunchContext launchContext) {
            Map<String, String> classPathForEnv = Maps.newHashMap();
            // pick up the local classpath so when we instantiate a Configuration remotely.
            YarnUtils.addLocalClasspathToEnv(classPathForEnv, getYarnConf());
            // set this map of env vars into the launch context.
            launchContext.setEnvironment(classPathForEnv);
        }
    }

    /**
     * CallbackHandler to process NM async calls
     */
    private class NMCallbackHandler implements NMClientAsync.CallbackHandler {
        /** List of containers */
        private ConcurrentMap<ContainerId, Container> containers = new ConcurrentHashMap<ContainerId, Container>();

        /**
         * Add a container
         * 
         * @param containerId
         *            id of container
         * @param container
         *            container object
         */
        public void addContainer(ContainerId containerId, Container container) {
            this.containers.putIfAbsent(containerId, container);
        }

        @Override
        public void onContainerStopped(ContainerId containerId) {
            LOG.info("Succeeded to stop Container {}", containerId);
            this.containers.remove(containerId);
        }

        @Override
        public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
            LOG.info("Container Status: id={}, status={}", containerId, containerStatus);
        }

        @Override
        public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
            LOG.info("Succeeded to start Container {}", containerId);
            Container container = this.containers.get(containerId);
            if(container != null) {
                getNmClientAsync().getContainerStatusAsync(containerId, container.getNodeId());
            }
        }

        @Override
        public void onStartContainerError(ContainerId containerId, Throwable t) {
            LOG.error(String.format("Failed to start Container %s", containerId), t);
            this.containers.remove(containerId);
        }

        @Override
        public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
            LOG.error(String.format("Failed to query the status of Container %s", containerId), t);

        }

        @Override
        public void onStopContainerError(ContainerId containerId, Throwable t) {
            LOG.error(String.format("Failed to stop Container %s", containerId), t);
            this.containers.remove(containerId);
        }
    }

    /**
     * Lazily compose the map of jar and file names to LocalResource records for inclusion in GuaguaYarnTask container
     * requests. Can re-use the same map as guagua tasks need identical HDFS-based resources (jars etc.) to run.
     * 
     * @return the resource map for a ContainerLaunchContext
     */
    private synchronized Map<String, LocalResource> getTaskResourceMap() {
        // Set the local resources: just send the copies already in HDFS
        if(null == localResources) {
            localResources = Maps.newHashMap();
            try {
                // if you have to update the Conf for export to tasks, do it now
                // updateGuaguaConfForExport();
                localResources = YarnUtils.getLocalResourceMap(getYarnConf(), getAppId());
            } catch (IOException ioe) {
                // fail fast, this container will never launch.
                throw new IllegalStateException("Could not configure the container launch context for GuaguaYarnTask.",
                        ioe);
            }
        }
        // else, return the prepopulated copy to reuse for each GuaguaYarkTask
        return localResources;
    }

    public ContainerId getContainerId() {
        return masterContainerId;
    }

    public void setContainerId(ContainerId containerId) {
        this.masterContainerId = containerId;
    }

    public ApplicationAttemptId getAppAttemptId() {
        return appAttemptId;
    }

    public void setAppAttemptId(ApplicationAttemptId appAttemptId) {
        this.appAttemptId = appAttemptId;
    }

    public boolean isDone() {
        return done;
    }

    public void setDone(boolean done) {
        this.done = done;
    }

    public Configuration getYarnConf() {
        return yarnConf;
    }

    public void setYarnConf(YarnConfiguration yarnConf) {
        this.yarnConf = yarnConf;
    }

    public AtomicInteger getCompletedCount() {
        return completedCount;
    }

    public void setCompletedCount(AtomicInteger completedCount) {
        this.completedCount = completedCount;
    }

    public AtomicInteger getFailedCount() {
        return failedCount;
    }

    public void setFailedCount(AtomicInteger failedCount) {
        this.failedCount = failedCount;
    }

    public AtomicInteger getAllocatedCount() {
        return allocatedCount;
    }

    public void setAllocatedCount(AtomicInteger allocatedCount) {
        this.allocatedCount = allocatedCount;
    }

    public AtomicInteger getSuccessfulCount() {
        return successfulCount;
    }

    public void setSuccessfulCount(AtomicInteger successfulCount) {
        this.successfulCount = successfulCount;
    }

    public int getContainersToLaunch() {
        return containersToLaunch;
    }

    public void setContainersToLaunch(int containersToLaunch) {
        this.containersToLaunch = containersToLaunch;
    }

    public ExecutorService getExecutor() {
        return executor;
    }

    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    public int getHeapPerContainer() {
        return heapPerContainer;
    }

    public void setHeapPerContainer(int heapPerContainer) {
        this.heapPerContainer = heapPerContainer;
    }

    public AMRMClientAsync<ContainerRequest> getAmRMClient() {
        return amRmClient;
    }

    public void setAmRMClient(AMRMClientAsync<ContainerRequest> amRMClient) {
        this.amRmClient = amRMClient;
    }

    public NMClientAsync getNmClientAsync() {
        return nmClientAsync;
    }

    public void setNmClientAsync(NMClientAsync nmClientAsync) {
        this.nmClientAsync = nmClientAsync;
    }

    public NMCallbackHandler getContainerListener() {
        return containerListener;
    }

    public void setContainerListener(NMCallbackHandler containerListener) {
        this.containerListener = containerListener;
    }

    public String getContainerArgs() {
        return containerArgs;
    }

    public void setContainerArgs(String containerArgs) {
        this.containerArgs = containerArgs;
    }

    public ApplicationId getAppId() {
        return appId;
    }

    public void setAppId(ApplicationId appId) {
        this.appId = appId;
    }

    /**
     * Application entry point
     * 
     * @param args
     *            command-line args (set by GuaguaYarnClient, if any)
     */
    public static void main(final String[] args) {
        LOG.info("Starting GuaguaAppMaster. ");
        String containerIdString = System.getenv().get(Environment.CONTAINER_ID.name());
        if(containerIdString == null) {
            // container id should always be set in the env by the framework
            throw new IllegalArgumentException("ContainerId not found in env vars.");
        }
        ContainerId containerId = ConverterUtils.toContainerId(containerIdString);
        ApplicationAttemptId appAttemptId = containerId.getApplicationAttemptId();
        Configuration conf = new YarnConfiguration();
        String jobUserName = System.getenv(ApplicationConstants.Environment.USER.name());
        conf.set(MRJobConfig.USER_NAME, jobUserName);
        try {
            UserGroupInformation.setConfiguration(conf);
            // Security framework already loaded the tokens into current UGI, just use them
            Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
            LOG.info("Executing with tokens:");
            for(Token<?> token: credentials.getAllTokens()) {
                LOG.info(token.toString());
            }

            UserGroupInformation appMasterUgi = UserGroupInformation.createRemoteUser(jobUserName);
            appMasterUgi.addCredentials(credentials);

            // Now remove the AM->RM token so tasks don't have it
            Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
            while(iter.hasNext()) {
                Token<?> token = iter.next();
                if(token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
                    iter.remove();
                }
            }

            final GuaguaAppMaster appMaster = new GuaguaAppMaster(containerId, appAttemptId, conf);
            appMasterUgi.doAs(new PrivilegedAction<Void>() {
                @Override
                public Void run() {
                    boolean result = false;
                    try {
                        result = appMaster.run();
                    } catch (Throwable t) {
                        LOG.error("GuaguaAppMaster caught a top-level exception in main.", t);
                        System.exit(1);
                    }

                    if(result) {
                        LOG.info("Guagua Application Master completed successfully. exiting");
                        System.exit(0);
                    } else {
                        LOG.info("Guagua Application Master failed. exiting");
                        System.exit(2);
                    }
                    return null;
                }
            });

        } catch (Throwable t) {
            LOG.error("GuaguaAppMaster caught a top-level exception in main.", t);
            System.exit(1);
        }
    }
}
