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
package ml.shifu.guagua.worker;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import ml.shifu.guagua.GuaguaConstants;
import ml.shifu.guagua.io.Bytable;
import ml.shifu.guagua.io.BytableWrapper;
import ml.shifu.guagua.io.NettyBytableDecoder;
import ml.shifu.guagua.io.NettyBytableEncoder;
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
    public static class ClientHandler extends SimpleChannelUpstreamHandler {

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
    }

    @Override
    public void preIteration(WorkerContext<MASTER_RESULT, WORKER_RESULT> context) {
        LOG.info("Start itertion {} with container id {} and app id {}.", context.getCurrentIteration(),
                context.getContainerId(), context.getAppId());
    }

    /**
     * Send worker results to master; wait for current master stop; get current master result.
     */
    @Override
    public void postIteration(final WorkerContext<MASTER_RESULT, WORKER_RESULT> context) {
        new BasicCoordinatorCommand() {
            @Override
            public void doExecute() throws KeeperException, InterruptedException {
                String appId = context.getAppId();
                int currentIteration = context.getCurrentIteration();
                final String appMasterNode = getCurrentMasterNode(appId, currentIteration).toString();

                // send message
                BytableWrapper workerMessage = new BytableWrapper();
                workerMessage.setBytes(NettyWorkerCoordinator.this.getWorkerSerializer().objectToBytes(
                        context.getWorkerResult()));
                workerMessage.setCurrentIteration(context.getCurrentIteration());
                workerMessage.setContainerId(context.getContainerId());
                workerMessage.setStopMessage(false);
                LOG.debug("Message:{}", workerMessage);
                NettyWorkerCoordinator.this.clientChannel.write(workerMessage);

                long start = System.nanoTime();
                // wait for master computation stop
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

                // set master result for next iteration.
                String appMasterSplitNode = getCurrentMasterSplitNode(appId, currentIteration).toString();
                setMasterResult(context, appMasterNode, appMasterSplitNode);

                LOG.info("Master computation is done.");
            }
        }.execute();
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
                    BytableWrapper stopMessage = new BytableWrapper();
                    stopMessage.setCurrentIteration(context.getCurrentIteration());
                    stopMessage.setContainerId(context.getContainerId());
                    stopMessage.setStopMessage(true);
                    ChannelFuture future = NettyWorkerCoordinator.this.clientChannel.write(stopMessage);
                    future.await(5, TimeUnit.SECONDS);
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

}
