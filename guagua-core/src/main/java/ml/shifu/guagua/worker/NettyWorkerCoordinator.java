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
package ml.shifu.guagua.worker;

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import ml.shifu.guagua.GuaguaConstants;
import ml.shifu.guagua.GuaguaRuntimeException;
import ml.shifu.guagua.coordinator.zk.GuaguaZooKeeper.Filter;
import ml.shifu.guagua.io.Bytable;
import ml.shifu.guagua.io.BytableWrapper;
import ml.shifu.guagua.io.HaltBytable;
import ml.shifu.guagua.io.NettyBytableDecoder;
import ml.shifu.guagua.io.NettyBytableEncoder;
import ml.shifu.guagua.util.NetworkUtils;
import ml.shifu.guagua.util.NumberFormatUtils;
import ml.shifu.guagua.util.ReflectionUtils;

import org.apache.zookeeper.KeeperException;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Worker coordinator coordinates master with Netty client channel.
 * 
 * <p>
 * Worker results and iteration info is not stored in to znode like SyncWorkerCoordinator. For a big task with much more
 * workers, this can decrease pressure on zookeeper. To leverage Netty, fast worker and master coordination is expected.
 * 
 * <p>
 * Master results are still stored into zookeeper for fail-over. Only one master per each job, this shouldn't be a
 * burden to zookeeper.
 */
public class NettyWorkerCoordinator<MASTER_RESULT extends Bytable, WORKER_RESULT extends Bytable> extends
        AbstractWorkerCoordinator<MASTER_RESULT, WORKER_RESULT> {

    private static final Logger LOG = LoggerFactory.getLogger(NettyWorkerCoordinator.class);

    private static final long GUAGUA_DEFAULT_WORKER_GETRESULT_TIMEOUT = 60 * 1000L;

    private static final String GUAGUA_WORKER_GETRESULT_TIMEOUT = "guagua.worker.getresult.timeout";

    /**
     * Master server address with format <name:port>.
     */
    private String masterServerAddress;

    /**
     * Netty client instance to communicate with master.
     */
    private ClientBootstrap messageClient;

    /**
     * Client channel used to connect to master server.
     */
    private Channel clientChannel;

    /**
     * If server is shutdown.
     */
    private AtomicBoolean isServerShutdownOrClientDisconnect = new AtomicBoolean(false);

    /**
     * If get master result time out. Set this to a field to make it updated in inner classes.
     */
    private boolean isTimeoutToGetCurrentMasterResult = false;

    /**
     * If master znode is cleaned, we may get exception on calling
     * {@link #setMasterResult(WorkerContext, String, String)}.
     */
    private boolean isMasterZnodeCleaned = false;

    /**
     * If get master server address time out. Set this to a field to make it updated in inner classes.
     */
    private boolean isTimeoutToGetMasterServerAddress = false;

    /**
     * Worker coordinator initialization.
     * 
     * <ul>
     * <li>1. Initialize Zookeeper instance to connect to zookeeper ensemble;</li>
     * <li>2. check fail over to recover from last failure point;</li>
     * <li>3. wait for master initialization and get master address from master initialization znode;</li>
     * <li>4. Recover last master result from master znode if fail-over task.</li>
     * </ul>
     */
    @Override
    public void preApplication(final WorkerContext<MASTER_RESULT, WORKER_RESULT> context) {
        // Initialize zookeeper and other props
        initialize(context.getProps());

        // Fail over check to get last successful iteration.
        new FailOverCoordinatorCommand(context).execute();

        // Wait for master init and get master server address.
        new BasicCoordinatorCommand() {
            @Override
            public void doExecute() throws KeeperException, InterruptedException {
                String appId = context.getAppId();
                final String appMasterNode = getCurrentMasterNode(appId, GuaguaConstants.GUAGUA_INIT_STEP).toString();
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

                NettyWorkerCoordinator.this.masterServerAddress = new String(getBytesFromZNode(appMasterNode, null),
                        Charset.forName("UTF-8"));
            }
        }.execute();

        // Connect to master server
        connectMasterServer();

        // If not start with iteration 0, it is fail over task, should recover from laster point.
        if(!context.isInitIteration()) {
            new BasicCoordinatorCommand() {
                @Override
                public void doExecute() throws KeeperException, InterruptedException {
                    String appId = context.getAppId();
                    int currentIteration = context.getCurrentIteration();
                    final String appMasterNode = getCurrentMasterNode(appId, currentIteration).toString();
                    final String appMasterSplitNode = getCurrentMasterSplitNode(appId, currentIteration).toString();
                    setMasterResult(context, appMasterNode, appMasterSplitNode);
                }
            }.execute();
        }
    }

    /**
     * Connect master server for message communication.
     */
    private void connectMasterServer() {
        this.messageClient = new ClientBootstrap(new NioClientSocketChannelFactory(Executors.newSingleThreadExecutor(),
                Executors.newSingleThreadExecutor()));

        // Set up the pipeline factory.
        this.messageClient.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() throws Exception {
                return Channels.pipeline(new NettyBytableEncoder(), new NettyBytableDecoder(), new ClientHandler());
            }
        });

        String[] namePortGroup = this.masterServerAddress.split(":");
        String masterServerName = namePortGroup[0];
        int masterServerPort = NumberFormatUtils.getInt(namePortGroup[1]);
        // Start the connection attempt.
        ChannelFuture future = this.messageClient.connect(new InetSocketAddress(masterServerName, masterServerPort));
        this.clientChannel = future.awaitUninterruptibly().getChannel();
        LOG.info("Connect to {}:{}", masterServerName, masterServerPort);
    }

    /**
     * ClientHandeler used to update progress to RPC server (AppMaster).
     */
    private class ClientHandler extends SimpleChannelUpstreamHandler {

        @Override
        public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
            super.handleUpstream(ctx, e);
        }

        @Override
        public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
            // Send the first message if this handler is a client-side handler.
            LOG.info("Channel connected:{}", e.getValue());
        }

        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
            LOG.info("Receive status:{}", e.getMessage());
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
            e.getChannel().close();
        }

        @Override
        public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
            // channel is disconnected, master server is down or client connection failed.
            LOG.info("Master server is down or channel client is disconnected with event {}", e);
            NettyWorkerCoordinator.this.isServerShutdownOrClientDisconnect.compareAndSet(false, true);
        }

    }

    @Override
    public void preIteration(final WorkerContext<MASTER_RESULT, WORKER_RESULT> context) {
        if(isServerShutdownOrClientDisconnect.get()) {
            final long masterServerRestartTimout = NumberFormatUtils.getLong(
                    context.getProps().getProperty("guagua.master.server.restart.timeout"), 60 * 1000L);
            // get new server address if master server is down or confirm previous server is alive.
            while(true) {
                this.isTimeoutToGetMasterServerAddress = false;
                // Wait for master init and get master server address.
                new BasicCoordinatorCommand() {
                    @Override
                    public void doExecute() throws KeeperException, InterruptedException {
                        String appId = context.getAppId();
                        final String appMasterNode = getCurrentMasterNode(appId, GuaguaConstants.GUAGUA_INIT_STEP)
                                .toString();

                        final long start = System.nanoTime();

                        // wait for master restart.
                        new RetryCoordinatorCommand(isFixedTime(), getSleepTime()) {
                            String newServerAddress = null;

                            @Override
                            public boolean retryExecution() throws KeeperException, InterruptedException {
                                try {
                                    if(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) >= masterServerRestartTimout) {
                                        NettyWorkerCoordinator.this.isTimeoutToGetMasterServerAddress = true;
                                        return true;
                                    }
                                    this.newServerAddress = new String(getBytesFromZNode(appMasterNode, null),
                                            Charset.forName("UTF-8"));
                                    boolean isServerChanged = !this.newServerAddress
                                            .equals(NettyWorkerCoordinator.this.masterServerAddress);
                                    if(isServerChanged) {
                                        NettyWorkerCoordinator.this.masterServerAddress = this.newServerAddress;
                                    }
                                    return isServerChanged;
                                } catch (KeeperException.NoNodeException e) {
                                    // to avoid log flood
                                    if(System.nanoTime() % 10 == 0) {
                                        LOG.warn("No such node:{}", appMasterNode);
                                    }
                                    return false;
                                }
                            }
                        }.execute();
                    }
                }.execute();

                if(NettyWorkerCoordinator.this.isTimeoutToGetMasterServerAddress) {
                    String[] namePortGroup = this.masterServerAddress.split(":");
                    String masterServerName = namePortGroup[0];
                    int masterServerPort = NumberFormatUtils.getInt(namePortGroup[1]);
                    try {
                        if(NetworkUtils.isServerAlive(InetAddress.getByName(masterServerName), masterServerPort)) {
                            break;
                        } else {
                            continue;
                        }
                    } catch (UnknownHostException e) {
                        throw new GuaguaRuntimeException(e);
                    }
                } else {
                    break;
                }
            }

            // connect to new master server.
            connectMasterServer();

            // if server is shutdown, master is down, fail over from last master step
            new FailOverCoordinatorCommand(context).execute();

            // reset master result to current iteration if master is down.
            if(!context.isInitIteration()) {
                new BasicCoordinatorCommand() {
                    @Override
                    public void doExecute() throws KeeperException, InterruptedException {
                        String appId = context.getAppId();
                        int lastIteration = context.getCurrentIteration();
                        final String appMasterNode = getCurrentMasterNode(appId, lastIteration).toString();
                        final String appMasterSplitNode = getCurrentMasterSplitNode(appId, lastIteration).toString();
                        setMasterResult(context, appMasterNode, appMasterSplitNode);
                    }
                }.execute();
            }

            // current iteration is last master successful iteration + 1
            context.setCurrentIteration(context.getCurrentIteration() + 1);

            // reset server shut down to false;
            isServerShutdownOrClientDisconnect.compareAndSet(true, false);
        }
        LOG.info("Start itertion {} with container id {} and app id {}.", context.getCurrentIteration(),
                context.getContainerId(), context.getAppId());
    }

    /**
     * Send worker results to master; wait for current master stop; get current master result.
     */
    @Override
    public void postIteration(final WorkerContext<MASTER_RESULT, WORKER_RESULT> context) {
        final long timeOutThreshold = NumberFormatUtils.getLong(
                context.getProps().getProperty(GUAGUA_WORKER_GETRESULT_TIMEOUT),
                GUAGUA_DEFAULT_WORKER_GETRESULT_TIMEOUT);
        while(true) {
            this.isTimeoutToGetCurrentMasterResult = false;
            this.isMasterZnodeCleaned = false;
            // check current iteration from zookeeper latest
            int latestIteraton = getLatestMasterIteration(context);

            if(context.getCurrentIteration() == latestIteraton + 1
                    && context.getCurrentIteration() <= context.getTotalIteration()) {
                new BasicCoordinatorCommand() {
                    @Override
                    public void doExecute() throws KeeperException, InterruptedException {
                        String appId = context.getAppId();
                        int currentIteration = context.getCurrentIteration();
                        final String appMasterNode = getCurrentMasterNode(appId, currentIteration).toString();

                        // send message
                        // TODO do we need to send several times.
                        BytableWrapper workerMessage = new BytableWrapper();
                        workerMessage.setBytes(NettyWorkerCoordinator.this.getWorkerSerializer().objectToBytes(
                                context.getWorkerResult()));
                        workerMessage.setCurrentIteration(context.getCurrentIteration());
                        workerMessage.setContainerId(context.getContainerId());
                        workerMessage.setStopMessage(false);
                        LOG.debug("Message:{}", workerMessage);
                        NettyWorkerCoordinator.this.clientChannel.write(workerMessage);

                        final long start = System.nanoTime();
                        // wait for master computation stop
                        new RetryCoordinatorCommand(isFixedTime(), getSleepTime()) {
                            @Override
                            public boolean retryExecution() throws KeeperException, InterruptedException {
                                try {
                                    if(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) >= timeOutThreshold) {
                                        NettyWorkerCoordinator.this.isTimeoutToGetCurrentMasterResult = true;
                                        return true;
                                    }
                                    return getZooKeeper().exists(appMasterNode, false) != null
                                            || NettyWorkerCoordinator.this.isServerShutdownOrClientDisconnect.get();
                                } catch (KeeperException.NoNodeException e) {
                                    // to avoid log flood
                                    if(System.nanoTime() % 10 == 0) {
                                        LOG.warn("No such node:{}", appMasterNode);
                                    }
                                    return false;
                                }
                            }
                        }.execute();

                        if(!NettyWorkerCoordinator.this.isTimeoutToGetCurrentMasterResult) {
                            LOG.info("Application {} container {} iteration {} waiting ends with {}ms execution time.",
                                    context.getAppId(), context.getContainerId(), context.getCurrentIteration(),
                                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));

                            // set master result for next iteration.
                            if(!NettyWorkerCoordinator.this.isServerShutdownOrClientDisconnect.get()) {
                                String appMasterSplitNode = getCurrentMasterSplitNode(appId, currentIteration)
                                        .toString();
                                try {
                                    setMasterResult(context, appMasterNode, appMasterSplitNode);
                                } catch (KeeperException.NoNodeException e) {
                                    // this exception may happen after checking znode existing, cleaned by master znode
                                    NettyWorkerCoordinator.this.isMasterZnodeCleaned = true;
                                    LOG.warn("No such node:{}", appMasterNode);
                                }
                                LOG.info("Master computation is done.");
                            }
                        }
                    }
                }.execute();

                if(NettyWorkerCoordinator.this.isTimeoutToGetCurrentMasterResult
                        || NettyWorkerCoordinator.this.isMasterZnodeCleaned) {
                    // if time out to get master result, continue and retry the whole postIteration logic (while(true)).
                    continue;
                } else {
                    // break while loop
                    break;
                }
            } else {
                // current iteration is last master successful iteration
                LOG.info("Application {} container {}, current iteration is switched to {}.", context.getAppId(),
                        context.getContainerId(), latestIteraton);
                context.setCurrentIteration(latestIteraton);

                // set master result
                if(!context.isInitIteration()) {
                    new BasicCoordinatorCommand() {
                        @Override
                        public void doExecute() throws KeeperException, InterruptedException {
                            String appId = context.getAppId();
                            int lastIteration = context.getCurrentIteration();
                            final String appMasterNode = getCurrentMasterNode(appId, lastIteration).toString();
                            final String appMasterSplitNode = getCurrentMasterSplitNode(appId, lastIteration)
                                    .toString();
                            try {
                                setMasterResult(context, appMasterNode, appMasterSplitNode);
                            } catch (KeeperException.NoNodeException e) {
                                // this exception may happen after checking znode existing, cleaned by master znode
                                NettyWorkerCoordinator.this.isMasterZnodeCleaned = true;
                                LOG.warn("No such node:{}", appMasterNode);
                            }
                        }
                    }.execute();
                }
                // break while loop or if master znode is already cleaned
                if(NettyWorkerCoordinator.this.isMasterZnodeCleaned) {
                    continue;
                } else {
                    break;
                }
            }
        }
    }

    /**
     * Send stop message to master and then clean resources.
     */
    @Override
    public void postApplication(final WorkerContext<MASTER_RESULT, WORKER_RESULT> context) {
        new BasicCoordinatorCommand() {
            @Override
            public void doExecute() throws Exception, InterruptedException {
                try {
                    // send stop message to server
                    MASTER_RESULT masterResult = context.getLastMasterResult();
                    if((context.getCurrentIteration() == context.getTotalIteration() + 1)
                            || ((masterResult instanceof HaltBytable) && ((HaltBytable) masterResult).isHalt())) {
                        // only send stop message if it is last iteration or isHalt is true, if exception in iteration,
                        // guagua will stop here to call postApplication
                        BytableWrapper stopMessage = new BytableWrapper();
                        stopMessage.setCurrentIteration(context.getCurrentIteration());
                        stopMessage.setContainerId(context.getContainerId());
                        stopMessage.setStopMessage(true);
                        ChannelFuture future = NettyWorkerCoordinator.this.clientChannel.write(stopMessage);
                        future.await(30, TimeUnit.SECONDS);
                        // wait 2s to send stop message out.
                        Thread.sleep(2 * 1000L);
                    }
                } finally {
                    NettyWorkerCoordinator.this.clientChannel.close();
                    Method shutDownMethod = ReflectionUtils.getMethod(
                            NettyWorkerCoordinator.this.messageClient.getClass(), "shutdown");
                    if(shutDownMethod != null) {
                        shutDownMethod.invoke(NettyWorkerCoordinator.this.messageClient, (Object[]) null);
                    }
                    NettyWorkerCoordinator.this.messageClient.releaseExternalResources();
                    closeZooKeeper();
                }
            }
        }.execute();
    }

    private int getLatestMasterIteration(final WorkerContext<MASTER_RESULT, WORKER_RESULT> context) {
        try {
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
                    return Integer.valueOf(masterIterations.get(masterIterations.size() - 1));
                } catch (NumberFormatException e) {
                    throw new GuaguaRuntimeException(e);
                }
            }
        } catch (InterruptedException e) {
            // transfer interrupt state to caller thread.
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            throw new GuaguaRuntimeException(e);
        }
        throw new GuaguaRuntimeException("Cannot get valid latest master iteration.");
    }

}
