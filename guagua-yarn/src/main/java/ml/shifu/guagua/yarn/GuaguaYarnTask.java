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
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.Executors;

import ml.shifu.guagua.GuaguaConstants;
import ml.shifu.guagua.GuaguaRuntimeException;
import ml.shifu.guagua.GuaguaService;
import ml.shifu.guagua.hadoop.io.GuaguaInputSplit;
import ml.shifu.guagua.io.Bytable;
import ml.shifu.guagua.io.GuaguaFileSplit;
import ml.shifu.guagua.master.GuaguaMasterService;
import ml.shifu.guagua.util.Progressable;
import ml.shifu.guagua.worker.GuaguaWorkerService;
import ml.shifu.guagua.yarn.util.GsonUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.zookeeper.common.IOUtils;
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
import org.jboss.netty.handler.codec.serialization.ClassResolvers;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link GuaguaYarnTask} is a entry point to run both master and workers.
 * 
 * <p>
 * {@link #partition} should be passed as the last parameter in main. And it should be not be changed if we try another
 * task.
 * 
 * <p>
 * Input split are now storing in guagua-conf.xml. We read data from there and check whether this task is master or
 * worker.
 */
public class GuaguaYarnTask<MASTER_RESULT extends Bytable, WORKER_RESULT extends Bytable> {

    private static final Logger LOG = LoggerFactory.getLogger(GuaguaYarnTask.class);

    static {
        // pick up new conf XML file and populate it with stuff exported from client
        Configuration.addDefaultResource(GuaguaYarnConstants.GUAGUA_CONF_FILE);
    }
    /**
     * Partition is never changed, it is the index of all spits. If a fail-over task, it should keep the partition
     * unchanged.
     */
    private int partition;

    /**
     * Application attempt id
     */
    private ApplicationAttemptId appAttemptId;

    /**
     * Container id
     */
    private ContainerId containerId;

    /**
     * Application id
     */
    private ApplicationId appId;

    /**
     * Yarn conf
     */
    private Configuration yarnConf;

    /**
     * Whether this task is master
     */
    private boolean isMaster;

    /**
     * Service instance to run master or worker service.
     */
    private GuaguaService guaguaService;

    /**
     * Input split for worker tasks
     */
    private GuaguaInputSplit inputSplit;

    /**
     * RPC port used to connect to RPC server.
     */
    private int rpcPort = GuaguaYarnConstants.DEFAULT_STATUS_RPC_PORT;

    /**
     * RPC server host name
     */
    private String rpcHostName;

    /**
     * Client channel used to connect to RPC server.
     */
    private Channel rpcClientChannel;

    /**
     * Netty client instance.
     */
    private ClientBootstrap rpcClient;

    /**
     * Constructor with yarn task related parameters.
     */
    public GuaguaYarnTask(ApplicationAttemptId appAttemptId, ContainerId containerId, int partition,
            String rpcHostName, String rpcPort, Configuration conf) {
        this.appAttemptId = appAttemptId;
        this.containerId = containerId;
        this.partition = partition;
        this.rpcHostName = rpcHostName;
        this.rpcPort = Integer.parseInt(rpcPort);
        LOG.info("current partition:{}", this.getPartition());
        this.appId = this.getAppAttemptId().getApplicationId();
        this.yarnConf = conf;
        this.inputSplit = GsonUtils.fromJson(
                this.getYarnConf().get(GuaguaYarnConstants.GUAGUA_YARN_INPUT_SPLIT_PREFIX + partition),
                GuaguaInputSplit.class);
        LOG.info("current input split:{}", this.getInputSplit());
    }

    /**
     * Set up guagua service
     */
    protected void setup() {
        this.setMaster(this.getInputSplit().isMaster());
        if(this.isMaster()) {
            this.setGuaguaService(new GuaguaMasterService<MASTER_RESULT, WORKER_RESULT>());
        } else {
            this.setGuaguaService(new GuaguaWorkerService<MASTER_RESULT, WORKER_RESULT>());
            List<GuaguaFileSplit> splits = new LinkedList<GuaguaFileSplit>();
            for(FileSplit fileSplit: getInputSplit().getFileSplits()) {
                splits.add(new GuaguaFileSplit(fileSplit.getPath().toString(), fileSplit.getStart(), fileSplit
                        .getLength()));
            }
            this.getGuaguaService().setSplits(splits);
        }
        Properties props = replaceConfToProps();
        this.getGuaguaService().setAppId(this.getAppId().toString());
        this.getGuaguaService().setContainerId(this.getPartition() + "");
        this.getGuaguaService().init(props);
        this.getGuaguaService().start();

        initRPCClient();
    }

    /**
     * Connect to app master for status RPC report.
     */
    private void initRPCClient() {
        this.rpcClient = new ClientBootstrap(new NioClientSocketChannelFactory(Executors.newSingleThreadExecutor(),
                Executors.newSingleThreadExecutor()));

        // Set up the pipeline factory.
        this.rpcClient.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() throws Exception {
                return Channels.pipeline(new ObjectEncoder(),
                        new ObjectDecoder(ClassResolvers.cacheDisabled(getClass().getClassLoader())),
                        new ClientHandler());
            }
        });

        // Start the connection attempt.
        ChannelFuture future = this.rpcClient.connect(new InetSocketAddress(this.rpcHostName, this.rpcPort));
        LOG.info("Connect to {}:{}", this.rpcHostName, this.rpcPort);
        this.rpcClientChannel = future.awaitUninterruptibly().getChannel();
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

    /**
     * We have to replace {@link Configuration} to {@link Properties} because of no dependency on hadoop in guagua-core.
     */
    private Properties replaceConfToProps() {
        Properties properties = new Properties();
        for(Entry<String, String> entry: getYarnConf()) {
            properties.put(entry.getKey(), entry.getValue());
            if(LOG.isInfoEnabled()) {
                if(entry.getKey().toString().startsWith(GuaguaConstants.GUAGUA)) {
                    LOG.debug("{}:{}", entry.getKey(), entry.getValue());
                }
            }
        }
        return properties;
    }

    @SuppressWarnings({ "unchecked", "unused" })
    private <T> T getSplitDetails(Path file, long offset) throws IOException {
        FileSystem fs = file.getFileSystem(getYarnConf());
        FSDataInputStream inFile = null;
        T split = null;
        try {
            inFile = fs.open(file);
            inFile.seek(offset);
            String className = Text.readString(inFile);
            Class<T> cls;
            try {
                cls = (Class<T>) getYarnConf().getClassByName(className);
            } catch (ClassNotFoundException ce) {
                IOException wrap = new IOException(String.format("Split class %s not found", className));
                wrap.initCause(ce);
                throw wrap;
            }
            SerializationFactory factory = new SerializationFactory(getYarnConf());
            Deserializer<T> deserializer = (Deserializer<T>) factory.getDeserializer(cls);
            deserializer.open(inFile);
            split = deserializer.deserialize(null);
        } finally {
            IOUtils.closeStream(inFile);
        }
        return split;
    }

    /**
     * Run master or worker service.
     */
    public void run() {
        try {
            this.setup();
            this.getGuaguaService().run(new Progressable() {
                @Override
                public void progress(int currentIteration, int totalIteration, String status, boolean isLastUpdate,
                        boolean isKill) {
                    // if is last update in current iteration, progress and status should be updated
                    if(isLastUpdate) {
                        LOG.info("Application progress: {}%.", (currentIteration * 100 / totalIteration));
                        GuaguaIterationStatus gi = new GuaguaIterationStatus(GuaguaYarnTask.this.partition,
                                currentIteration, totalIteration);
                        gi.setKillContainer(isKill);
                        LOG.info("Send GuaguaIterationStatus: {}.", gi);
                        rpcClientChannel.write(GsonUtils.toJson(gi));
                    }
                }
            });
        } catch (Exception e) {
            LOG.error("Error in guagua main run method.", e);
            throw new GuaguaRuntimeException(e);
        } finally {
            // cleanup should be called in finally segment to make sure resources are cleaned up at last.
            this.cleanup();
        }
    }

    /**
     * Clean up resources used
     */
    protected void cleanup() {
        if(this.rpcClient != null) {
            this.rpcClient.shutdown();
            this.rpcClient.releaseExternalResources();
        }
        if(this.rpcClientChannel != null) {
            this.rpcClientChannel.close();
        }
        this.getGuaguaService().stop();
    }

    public GuaguaService getGuaguaService() {
        return guaguaService;
    }

    public void setGuaguaService(GuaguaService guaguaService) {
        this.guaguaService = guaguaService;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public ApplicationAttemptId getAppAttemptId() {
        return appAttemptId;
    }

    public void setAppAttemptId(ApplicationAttemptId appAttemptId) {
        this.appAttemptId = appAttemptId;
    }

    public ContainerId getContainerId() {
        return containerId;
    }

    public void setContainerId(ContainerId containerId) {
        this.containerId = containerId;
    }

    public boolean isMaster() {
        return isMaster;
    }

    public void setMaster(boolean isMaster) {
        this.isMaster = isMaster;
    }

    public Configuration getYarnConf() {
        return yarnConf;
    }

    public void setYarnConf(YarnConfiguration yarnConf) {
        this.yarnConf = yarnConf;
    }

    public ApplicationId getAppId() {
        return appId;
    }

    public void setAppId(ApplicationId appId) {
        this.appId = appId;
    }

    public GuaguaInputSplit getInputSplit() {
        return inputSplit;
    }

    public void setInputSplit(GuaguaInputSplit inputSplit) {
        this.inputSplit = inputSplit;
    }

    public static void main(String[] args) {
        LOG.info("args:{}", Arrays.toString(args));
        if(args.length != 7) {
            throw new IllegalStateException(String.format(
                    "GuaguaYarnTask could not construct a TaskAttemptID for the Guagua job from args: %s",
                    Arrays.toString(args)));
        }

        String containerIdString = System.getenv().get(Environment.CONTAINER_ID.name());
        if(containerIdString == null) {
            // container id should always be set in the env by the framework
            throw new IllegalArgumentException("ContainerId not found in env vars.");
        }
        ContainerId containerId = ConverterUtils.toContainerId(containerIdString);
        ApplicationAttemptId appAttemptId = containerId.getApplicationAttemptId();

        try {
            Configuration conf = new YarnConfiguration();
            String jobUserName = System.getenv(ApplicationConstants.Environment.USER.name());
            conf.set(MRJobConfig.USER_NAME, jobUserName);
            UserGroupInformation.setConfiguration(conf);
            // Security framework already loaded the tokens into current UGI, just use them
            Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
            LOG.info("Executing with tokens:");
            for(Token<?> token: credentials.getAllTokens()) {
                LOG.info(token.toString());
            }

            UserGroupInformation appTaskUGI = UserGroupInformation.createRemoteUser(jobUserName);
            appTaskUGI.addCredentials(credentials);
            @SuppressWarnings("rawtypes")
            final GuaguaYarnTask<?, ?> guaguaYarnTask = new GuaguaYarnTask(appAttemptId, containerId,
                    Integer.parseInt(args[args.length - 3]), args[args.length - 2], args[args.length - 1], conf);
            appTaskUGI.doAs(new PrivilegedAction<Void>() {
                @Override
                public Void run() {
                    guaguaYarnTask.run();
                    return null;
                }
            });
        } catch (Throwable t) {
            LOG.error("GuaguaYarnTask threw a top-level exception, failing task", t);
            System.exit(2);
        }
        System.exit(0);
    }
}
