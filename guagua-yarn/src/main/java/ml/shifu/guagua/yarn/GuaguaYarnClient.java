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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;

import ml.shifu.guagua.GuaguaConstants;
import ml.shifu.guagua.GuaguaRuntimeException;
import ml.shifu.guagua.coordinator.zk.ZooKeeperUtils;
import ml.shifu.guagua.io.Bytable;
import ml.shifu.guagua.io.HaltBytable;
import ml.shifu.guagua.master.MasterComputable;
import ml.shifu.guagua.worker.WorkerComputable;
import ml.shifu.guagua.yarn.util.GsonUtils;
import ml.shifu.guagua.yarn.util.InputSplitUtils;
import ml.shifu.guagua.yarn.util.YarnUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.split.JobSplitWriter;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
 * {@link GuaguaYarnClient} is used to submit master-workers computation app on yarn cluster.
 * 
 * <p>
 * TODO clean app resources in HDFS no matter successful or not
 */
// TODO create GuaguaConfiguration class for mapreduce and yarn
public class GuaguaYarnClient extends Configured {

    private static final Logger LOG = LoggerFactory.getLogger(GuaguaYarnClient.class);

    private static final DecimalFormat DF = (DecimalFormat) (NumberFormat.getInstance());

    /** Sleep time between silent progress checks */
    private static final int JOB_STATUS_INTERVAL_MSECS = 2000;

    private static String embededZooKeeperServer = null;

    static {
        DF.setMaximumFractionDigits(2);
        DF.setGroupingUsed(false);
    }

    private YarnClient yarnClient;

    /**
     * Queue name
     */
    private String amQueue = GuaguaYarnConstants.GUAGUA_YARN_DEFAULT_QUEUE_NAME;

    /**
     * App Id
     */
    private ApplicationId appId;

    /**
     * App name
     */
    private String appName;

    /**
     * Counter used to check whether to print info.
     */
    private int reportCounter;

    /**
     * Application starting time.
     */
    private long startTime;

    /**
     * Default constructor. Use {@link YarnConfiguration} as default conf setting.
     */
    public GuaguaYarnClient() {
        this(new YarnConfiguration());
    }

    /**
     * Constructor with {@link Configuration} setting.
     */
    public GuaguaYarnClient(Configuration conf) {
        setConf(conf);
    }

    public static void addInputPath(Configuration conf, Path path) throws IOException {
        path = path.getFileSystem(conf).makeQualified(path);
        String dirStr = org.apache.hadoop.util.StringUtils.escapeString(path.toString());
        String dirs = conf.get(GuaguaYarnConstants.GUAGUA_YARN_INPUT_DIR);
        conf.set(GuaguaYarnConstants.GUAGUA_YARN_INPUT_DIR, dirs == null ? dirStr : dirs + "," + dirStr);
    }

    private static void printUsage() {
        GuaguaOptionsParser.printGenericCommandUsage(System.out);
        System.out.println("For detailed invalid parameter, please check:");
    }

    /**
     * Find a jar that contains a class of the same name, if any. It will return a jar file, even if that is not the
     * first thing on the class path that has a class with the same name.
     * 
     * @param my_class
     *            the class to find.
     * @return a jar file that contains the class, or null.
     * @throws IOException
     *             in case when read class file.
     */
    private static String findContainingJar(Class<?> my_class) {
        ClassLoader loader = my_class.getClassLoader();
        String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";
        try {
            for(Enumeration<?> itr = loader.getResources(class_file); itr.hasMoreElements();) {
                URL url = (URL) itr.nextElement();
                if("jar".equals(url.getProtocol())) {
                    String toReturn = url.getPath();
                    if(toReturn.startsWith("file:")) {
                        toReturn = toReturn.substring("file:".length());
                    }
                    // URLDecoder is a misnamed class, since it actually decodes
                    // x-www-form-urlencoded MIME type rather than actual
                    // URL encoding (which the file path has). Therefore it would
                    // decode +s to ' 's which is incorrect (spaces are actually
                    // either unencoded or encoded as "%20"). Replace +s first, so
                    // that they are kept sacred during the decoding process.
                    toReturn = toReturn.replaceAll("\\+", "%2B");
                    toReturn = URLDecoder.decode(toReturn, "UTF-8");
                    return toReturn.replaceAll("!.*$", "");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    private void copyResourcesToFS() throws IOException {
        LOG.debug("Copying resources to filesystem");
        YarnUtils.exportGuaguaConfiguration(getConf(), getAppId());
        YarnUtils.copyLocalResourcesToFs(getConf(), getAppId()); // Local
                                                                 // resources
        // log4j can be ignored with a warning
        try {
            YarnUtils.copyLocalResourceToFs(GuaguaYarnConstants.GUAGUA_LOG4J_PROPERTIES,
                    GuaguaYarnConstants.GUAGUA_LOG4J_PROPERTIES, getConf(), getAppId()); // Log4j
        } catch (FileNotFoundException ex) {
            LOG.warn("log4j.properties file not found, you had better to provide a log4j.properties for yarn app.");
        }
    }

    private List<InputSplit> createNewSplits() throws IOException {
        List<InputSplit> newSplits = null;
        boolean combinable = getConf().getBoolean(GuaguaConstants.GUAGUA_SPLIT_COMBINABLE, false);
        long blockSize = FileSystem.get(getConf()).getDefaultBlockSize(null);
        long combineSize = getConf().getLong(GuaguaConstants.GUAGUA_SPLIT_MAX_COMBINED_SPLIT_SIZE, blockSize);
        if(combineSize == 0) {
            combineSize = blockSize;
        }
        if(combinable) {
            List<InputSplit> splits = InputSplitUtils.getFileSplits(getConf(), combineSize);
            LOG.info("combine size:{}, splits:{}", combineSize, splits);
            newSplits = InputSplitUtils.getFinalCombineGuaguaSplits(splits, combineSize);
        } else {
            newSplits = new ArrayList<InputSplit>();
            for(InputSplit inputSplit: InputSplitUtils.getFileSplits(getConf(), combineSize)) {
                FileSplit fs = (FileSplit) inputSplit;
                newSplits.add(new GuaguaInputSplit(false, new FileSplit[] { fs }));
            }
        }
        // add master
        int masters = getConf().getInt(GuaguaConstants.GUAGUA_MASTER_NUMBER, GuaguaConstants.DEFAULT_MASTER_NUMBER);
        for(int i = 0; i < masters; i++) {
            newSplits.add(new GuaguaInputSplit(true, (FileSplit) null));
        }
        int mapperSize = newSplits.size();
        LOG.info("inputs size including master: {}", mapperSize);
        LOG.debug("input splits: {}", newSplits);
        getConf().set(GuaguaConstants.GUAGUA_WORKER_NUMBER, (mapperSize - masters) + "");
        return newSplits;
    }

    @SuppressWarnings("unchecked")
    private <T extends InputSplit> List<InputSplit> writeNewSplits(Path jobSubmitDir) throws IOException,
            InterruptedException {
        List<InputSplit> splits = createNewSplits();
        T[] array = (T[]) splits.toArray(new InputSplit[splits.size()]);

        // sort the splits into order based on size, so that the biggest
        // go first
        Arrays.sort(array, new SplitComparator());
        JobSplitWriter.createSplitFiles(jobSubmitDir, getConf(), jobSubmitDir.getFileSystem(getConf()), array);
        return splits;
    }

    private static GuaguaOptionsParser parseOpts(String[] args, Configuration conf) throws IOException,
            ClassNotFoundException {
        GuaguaOptionsParser parser = new GuaguaOptionsParser(conf, args);
        conf.set(GuaguaYarnConstants.GUAGUA_YARN_APP_LIB_JAR, conf.get("tmpjars"));
        String jar = findContainingJar(Class.forName(conf.get(GuaguaConstants.MASTER_COMPUTABLE_CLASS,
                GuaguaYarnClient.class.getName())));
        if(jar != null) {
            conf.set(GuaguaYarnConstants.GUAGUA_YARN_APP_JAR, jar);
        }
        CommandLine cmdLine = parser.getCommandLine();
        checkInputSetting(conf, cmdLine);
        checkZkServerSetting(conf, cmdLine);
        checkWorkerClassSetting(conf, cmdLine);
        checkMasterClassName(conf, cmdLine);
        checkIterationCountSetting(conf, cmdLine);
        checkResultClassSetting(conf, cmdLine);
        checkAppName(conf, cmdLine);
        return parser;
    }

    private static void checkAppName(Configuration conf, CommandLine cmdLine) {
        String name = "guagua";
        if(cmdLine.hasOption("-n")) {
            name = cmdLine.getOptionValue("n");
        }
        conf.set(GuaguaYarnConstants.GUAGUA_YARN_APP_NAME, name);
    }

    private static void checkResultClassSetting(Configuration conf, CommandLine cmdLine) {
        Class<?> masterResultClass;
        if(!cmdLine.hasOption("-mr")) {
            printUsage();
            throw new IllegalArgumentException("Master result class name should be provided by '-mr' parameter.");
        } else {
            String resultClassName = cmdLine.getOptionValue("mr").trim();
            try {
                masterResultClass = Class.forName(resultClassName);
            } catch (ClassNotFoundException e) {
                printUsage();
                throw new IllegalArgumentException(String.format(
                        "Master result class %s set by '-mr' can not be found in class path.", resultClassName), e);
            }
            if(Writable.class.isAssignableFrom(masterResultClass)) {
                conf.set(GuaguaConstants.GUAGUA_MASTER_IO_SERIALIZER, GuaguaWritableSerializer.class.getName());
                conf.set(GuaguaConstants.GUAGUA_MASTER_RESULT_CLASS, resultClassName);
            } else if(Bytable.class.isAssignableFrom(masterResultClass)) {
                conf.set(GuaguaConstants.GUAGUA_MASTER_RESULT_CLASS, resultClassName);
            } else {
                printUsage();
                throw new IllegalArgumentException(
                        "Master result class name provided by '-mr' parameter should implement 'com.paypal.guagua.io.Bytable' or 'org.apache.hadoop.io.Writable'.");
            }
        }

        Class<?> workerResultClass;
        if(!cmdLine.hasOption("-wr")) {
            printUsage();
            throw new IllegalArgumentException("Worker result class name should be provided by '-wr' parameter.");
        } else {
            String resultClassName = cmdLine.getOptionValue("wr").trim();
            try {
                workerResultClass = Class.forName(resultClassName);
            } catch (ClassNotFoundException e) {
                printUsage();
                throw new IllegalArgumentException(String.format(
                        "Worker result class %s set by '-wr' can not be found in class path.", resultClassName), e);
            }
            if(Writable.class.isAssignableFrom(workerResultClass)) {
                conf.set(GuaguaConstants.GUAGUA_WORKER_IO_SERIALIZER, GuaguaWritableSerializer.class.getName());
                conf.set(GuaguaConstants.GUAGUA_WORKER_RESULT_CLASS, resultClassName);
            } else if(Bytable.class.isAssignableFrom(workerResultClass)) {
                conf.set(GuaguaConstants.GUAGUA_WORKER_RESULT_CLASS, resultClassName);
            } else {
                printUsage();
                throw new IllegalArgumentException(
                        "Worker result class name provided by '-wr' parameter should implement 'com.paypal.guagua.io.Bytable' or 'org.apache.hadoop.io.Writable'.");
            }
        }

        if(HaltBytable.class.isAssignableFrom(masterResultClass)
                && !HaltBytable.class.isAssignableFrom(workerResultClass)
                || HaltBytable.class.isAssignableFrom(workerResultClass)
                && !HaltBytable.class.isAssignableFrom(masterResultClass)) {
            printUsage();
            throw new IllegalArgumentException("Worker and master result classes should both implementent HaltBytable.");
        }
    }

    private static void checkIterationCountSetting(Configuration conf, CommandLine cmdLine) {
        if(!cmdLine.hasOption("-c")) {
            System.err.println("WARN: Total iteration number is not set, default 10 will be used.");
            System.err.println("WARN: Total iteration number can be provided by '-c' parameter with non-empty value.");
            conf.setInt(GuaguaConstants.GUAGUA_ITERATION_COUNT, GuaguaConstants.GUAGUA_DEFAULT_ITERATION_COUNT);
        } else {
            int totalIteration = 0;
            try {
                totalIteration = Integer.parseInt(cmdLine.getOptionValue("c").trim());
            } catch (NumberFormatException e) {
                printUsage();
                throw new IllegalArgumentException("Total iteration number set by '-c' should be a valid number.");
            }
            conf.setInt(GuaguaConstants.GUAGUA_ITERATION_COUNT, totalIteration);
        }
    }

    private static void checkMasterClassName(Configuration conf, CommandLine cmdLine) throws ClassNotFoundException {
        if(!cmdLine.hasOption("-m")) {
            printUsage();
            throw new IllegalArgumentException("Master class name should be provided by '-m' parameter.");
        }

        String masterClassOptionValue = cmdLine.getOptionValue("m");
        if(masterClassOptionValue == null || masterClassOptionValue.length() == 0) {
            printUsage();
            throw new IllegalArgumentException(
                    "Master class name should be provided by '-m' parameter with non-empty value.");
        }
        Class<?> masterClass;
        try {
            masterClass = Class.forName(masterClassOptionValue.trim());
        } catch (ClassNotFoundException e) {
            printUsage();
            throw new IllegalArgumentException(String.format(
                    "The master class %s set by '-m' can not be found in class path.", masterClassOptionValue.trim()),
                    e);
        }
        if(!MasterComputable.class.isAssignableFrom(masterClass)) {
            printUsage();
            throw new IllegalArgumentException(
                    "Master class name provided by '-m' should implement 'com.paypal.guagua.master.MasterComputable' interface.");
        }

        conf.set(GuaguaConstants.MASTER_COMPUTABLE_CLASS, masterClassOptionValue.trim());
    }

    private static void checkWorkerClassSetting(Configuration conf, CommandLine cmdLine) {
        if(!cmdLine.hasOption("-w")) {
            printUsage();
            throw new IllegalArgumentException("Worker class name should be provided by '-w' parameter.");
        }
        String workerClassOptionValue = cmdLine.getOptionValue("w");
        if(workerClassOptionValue == null || workerClassOptionValue.length() == 0) {
            printUsage();
            throw new IllegalArgumentException(
                    "Worker class name should be provided by '-w' parameter with non-empty value.");
        }
        Class<?> workerClass;
        try {
            workerClass = Class.forName(workerClassOptionValue.trim());
        } catch (ClassNotFoundException e) {
            printUsage();
            throw new IllegalArgumentException(String.format(
                    "The worker class %s set by '-w' can not be found in class path.", workerClassOptionValue.trim()),
                    e);
        }
        if(!WorkerComputable.class.isAssignableFrom(workerClass)) {
            printUsage();
            throw new IllegalArgumentException(
                    "Worker class name provided by '-w' should implement 'com.paypal.guagua.worker.WorkerComputable' interface.");
        }
        conf.set(GuaguaConstants.WORKER_COMPUTABLE_CLASS, workerClassOptionValue.trim());
    }

    private static void checkZkServerSetting(Configuration conf, CommandLine cmdLine) {
        if(!cmdLine.hasOption("-z")) {
            System.err.println("WARN: ZooKeeper server is not set, embeded ZooKeeper server will be started.");
            System.err.println("WARN: For big data guagua application, independent ZooKeeper instance is recommended.");
            System.err.println("WARN: Zookeeper servers can be provided by '-z' parameter with non-empty value.");

            synchronized(GuaguaYarnClient.class) {
                if(embededZooKeeperServer == null) {
                    // 1. start embed zookeeper server in one thread.
                    int embedZkClientPort = ZooKeeperUtils.startEmbedZooKeeper();
                    // 2. check if it is started.
                    ZooKeeperUtils.checkIfEmbedZooKeeperStarted(embedZkClientPort);
                    try {
                        embededZooKeeperServer = InetAddress.getLocalHost().getHostName() + ":" + embedZkClientPort;
                    } catch (UnknownHostException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            // 3. set local embed zookeeper server address
            conf.set(GuaguaConstants.GUAGUA_ZK_SERVERS, embededZooKeeperServer);

            return;
        } else {
            String zkServers = cmdLine.getOptionValue("z");
            if(zkServers == null || zkServers.length() == 0) {
                throw new IllegalArgumentException(
                        "Zookeeper servers should be provided by '-z' parameter with non-empty value.");
            }
            if(ZooKeeperUtils.checkServers(zkServers)) {
                conf.set(GuaguaConstants.GUAGUA_ZK_SERVERS, zkServers.trim());
            } else {
                throw new RuntimeException("Your specifed zookeeper instance is not alive, please check.");
            }
        }
    }

    private static void checkInputSetting(Configuration conf, CommandLine cmdLine) throws IOException {
        if(!cmdLine.hasOption("-i")) {
            printUsage();
            throw new IllegalArgumentException("Input should be provided by '-i' parameter.");
        }

        String inputs = cmdLine.getOptionValue("i").trim();
        // TODO how about complicated input like: *.txt
        FileStatus fileStatus;
        try {
            fileStatus = FileSystem.get(conf).getFileStatus(new Path(inputs));
            LOG.info("Input files: {}", fileStatus);
        } catch (FileNotFoundException e) {
            printUsage();
            throw new IllegalArgumentException(String.format("Input %s doesn't exist.", inputs), e);
        }

        addInputPath(conf, fileStatus.getPath());
    }

    /**
     * To submit an app to yarn cluster and monitor the status.
     */
    public int run(String[] args) throws Exception {
        LOG.info("Running Client");
        this.yarnClient.start();

        // request an application id from the RM. Get a new application id firstly.
        YarnClientApplication app = this.yarnClient.createApplication();
        GetNewApplicationResponse getNewAppResponse = app.getNewApplicationResponse();

        // check cluster status.
        checkPerNodeResourcesAvailable(getNewAppResponse);

        // configure our request for an exec container
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        this.setAppId(appContext.getApplicationId());
        LOG.info("Obtained new application ID: {}", this.getAppId());

        // set app id and app name
        appContext.setApplicationId(this.getAppId());
        this.setAppName(getConf().get(GuaguaYarnConstants.GUAGUA_YARN_APP_NAME));
        appContext.setApplicationName(this.getAppName());

        prepareInputSplits();
        // copy local resources to hdfs app folder
        copyResourcesToFS();

        // TODO configurable
        appContext.setMaxAppAttempts(GuaguaYarnConstants.GUAGAU_APP_MASTER_DEFAULT_ATTMPTS);
        appContext.setQueue(getConf().get(GuaguaYarnConstants.GUAGUA_YARN_QUEUE_NAME,
                GuaguaYarnConstants.GUAGUA_YARN_DEFAULT_QUEUE_NAME));

        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(getConf().getInt(GuaguaYarnConstants.GUAGUA_YARN_MASTER_MEMORY,
                GuaguaYarnConstants.GUAGUA_YARN_DEFAULT_MASTER_MEMORY));
        capability.setVirtualCores(getConf().getInt(GuaguaYarnConstants.GUAGUA_YARN_MASTER_VCORES,
                GuaguaYarnConstants.GUAGUA_YARN_MASTER_DEFAULT_VCORES));
        appContext.setResource(capability);

        // Set the priority for the application master
        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(getConf().getInt(GuaguaYarnConstants.GUAGUA_YARN_MASTER_PRIORITY,
                GuaguaYarnConstants.GUAGUA_YARN_DEFAULT_PRIORITY));
        appContext.setPriority(pri);

        ContainerLaunchContext containerContext = buildContainerLaunchContext();
        appContext.setAMContainerSpec(containerContext);
        try {
            LOG.info("Submitting application to ASM");
            // obtain an "updated copy" of the appId for status checks/job kill later
            this.setAppId(this.yarnClient.submitApplication(appContext));
            LOG.info("Got new appId after submission : {}", getAppId());
        } catch (YarnException yre) {
            // try another time
            LOG.info("Submitting application again to ASM");
            // obtain an "updated copy" of the appId for status checks/job kill later
            this.setAppId(this.yarnClient.submitApplication(appContext));
            LOG.info("Got new appId after submission : {}", getAppId());
        }
        LOG.info("GuaguaAppMaster container request was submitted to ResourceManager for job: {}", getAppName());

        return awaitYarnJobCompletion();
    }

    private int awaitYarnJobCompletion() throws YarnException, IOException {
        boolean done;
        ApplicationReport report = null;
        try {
            do {
                try {
                    Thread.sleep(JOB_STATUS_INTERVAL_MSECS);
                } catch (InterruptedException ir) {
                    Thread.currentThread().interrupt();
                }
                report = this.yarnClient.getApplicationReport(getAppId());
                done = checkProgress(report);
            } while(!done);
        } catch (IOException ex) {
            final String diagnostics = (null == report) ? "" : "Diagnostics: " + report.getDiagnostics();
            LOG.error(String.format("Fatal fault encountered, failing %s. %s", getAppName(), diagnostics), ex);
            try {
                LOG.error("FORCIBLY KILLING Application from AppMaster.");
                this.yarnClient.killApplication(getAppId());
            } catch (YarnException yre) {
                LOG.error("Exception raised in attempt to kill application.", yre);
            }
            return -1;
        }
        return printFinalJobReport();
    }

    /**
     * Assess whether job is already finished/failed and 'done' flag needs to be
     * set, prints progress display for client if all is going well.
     * 
     * @param report
     *            the application report to assess.
     * @return true if job report indicates the job run is over.
     */
    private boolean checkProgress(final ApplicationReport report) {
        YarnApplicationState jobState = report.getYarnApplicationState();

        LOG.info(
                "Got applicaton report for appId={}, state={}, progress={}%, amDiag={}, masterHost={}, masterRpcPort={}, queue={}, startTime={}, clientToken={}, finalState={}, trackingUrl={}, user={}",
                this.appId.getId(), report.getYarnApplicationState().toString(), DF.format(report.getProgress() * 100),
                report.getDiagnostics(), report.getHost(), report.getRpcPort(), report.getQueue(),
                report.getStartTime(), report.getClientToAMToken(), report.getFinalApplicationStatus().toString(),
                report.getTrackingUrl(), report.getUser());
        switch(jobState) {
            case FINISHED:
                LOG.info("Application finished in {} ms", (System.currentTimeMillis() - getStartTime()));
                return true;
            case KILLED:
                LOG.error("{} reports KILLED state, diagnostics show: {}", getAppName(), report.getDiagnostics());
                return true;
            case FAILED:
                LOG.error("{} reports FAILED state, diagnostics show: {}", getAppName(), report.getDiagnostics());
                return true;
            default:
                if(this.reportCounter++ % 5 == 0) {
                    displayJobReport(report);
                }
                return false;
        }
    }

    /**
     * Display a formatted summary of the job progress report from the AM.
     * 
     * @param report
     *            the report to display.
     */
    private void displayJobReport(final ApplicationReport report) {
        if(null == report) {
            throw new IllegalStateException(String.format(
                    "[*] Latest ApplicationReport for job %s was not received by the local client.", getAppName()));
        }
        final float elapsed = (System.currentTimeMillis() - report.getStartTime()) / 1000.0f;
        LOG.info("{}, Elapsed: {}", getAppName(), String.format("%.2f secs", elapsed));
        LOG.info("{}, State: {} , Containers: used/reserved/needed-resources {}/{}/{}", report
                .getCurrentApplicationAttemptId(), report.getYarnApplicationState().name(), report
                .getApplicationResourceUsageReport().getNumUsedContainers(), report.getApplicationResourceUsageReport()
                .getNumReservedContainers(), report.getApplicationResourceUsageReport().getNeededResources());
    }

    private int printFinalJobReport() throws YarnException, IOException {
        try {
            ApplicationReport report = this.yarnClient.getApplicationReport(getAppId());
            FinalApplicationStatus finalAppStatus = report.getFinalApplicationStatus();
            final long secs = (report.getFinishTime() - report.getStartTime()) / 1000L;
            final String time = String.format("%d minutes, %d seconds.", secs / 60L, secs % 60L);
            LOG.info("Completed {}: {}, total running time: {}", getAppName(), finalAppStatus.name(), time);
            return finalAppStatus == FinalApplicationStatus.SUCCEEDED ? 0 : -1;
        } catch (YarnException yre) {
            LOG.error(String.format("Exception encountered while attempting to request a final job report for %s.",
                    getAppId()), yre);
            return -1;
        }
    }

    private List<InputSplit> inputSplits;

    /**
     * Prepare input splits for containers
     */
    private void prepareInputSplits() throws IOException, InterruptedException {
        this.inputSplits = writeNewSplits(YarnUtils.getAppDirectory(FileSystem.get(getConf()), getAppId()));
        LOG.debug("Input split: {}", this.inputSplits.size());

        // sort by length size, the one with the biggest size will be the first one to get container
        Collections.sort(this.inputSplits, new SplitComparator());

        // update partition and input splits to conf file
        int partition = 0;
        for(InputSplit inputSplit: this.inputSplits) {
            getConf().set(GuaguaYarnConstants.GUAGUA_YARN_INPUT_SPLIT_PREFIX + (++partition),
                    GsonUtils.toJson(inputSplit));
        }

        YarnUtils.exportGuaguaConfiguration(getConf(), getAppId());

        LOG.info("Input split size including master: {}", this.inputSplits.size());
    }

    private static class SplitComparator implements Comparator<InputSplit> {
        @Override
        public int compare(InputSplit o1, InputSplit o2) {
            try {
                long len1 = o1.getLength();
                long len2 = o2.getLength();
                return len1 < len2 ? 1 : (len1 == len2 ? 0 : -1);
            } catch (IOException ie) {
                throw new GuaguaRuntimeException(ie);
            } catch (InterruptedException ie) {
                throw new GuaguaRuntimeException(ie);
            }
        }
    }

    public boolean init(String[] args) {
        try {
            this.yarnClient = YarnClient.createYarnClient();
            this.yarnClient.init(getConf());
        } catch (Throwable e) {
            LOG.error("Error in yarn client initiliazation.", e);
            return false;
        }
        return true;
    }

    /**
     * Compose the ContainerLaunchContext for the Application Master.
     * 
     * @return the CLC object populated and configured.
     */
    private ContainerLaunchContext buildContainerLaunchContext() throws IOException {
        ContainerLaunchContext appMasterContainer = Records.newRecord(ContainerLaunchContext.class);
        appMasterContainer.setEnvironment(buildEnvironment());
        appMasterContainer.setLocalResources(buildLocalResourceMap());
        appMasterContainer.setCommands(buildAppMasterExecCommand());
        setToken(appMasterContainer);
        return appMasterContainer;
    }

    /**
     * Build full master java command
     */
    private List<String> buildAppMasterExecCommand() {
        String appMasterArgs = this.getConf().get(GuaguaYarnConstants.GUAGUA_YARN_MASTER_ARGS);
        if(appMasterArgs == null) {
            appMasterArgs = GuaguaYarnConstants.GUAGUA_YARN_DEFAULT_CONTAINER_JAVA_OPTS;
        } else {
            appMasterArgs = GuaguaYarnConstants.GUAGUA_YARN_DEFAULT_CONTAINER_JAVA_OPTS + " " + appMasterArgs;
        }
        return YarnUtils.getCommand(
                GuaguaAppMaster.class.getName(),
                appMasterArgs,
                null,
                getConf().getInt(GuaguaYarnConstants.GUAGUA_YARN_MASTER_MEMORY,
                        GuaguaYarnConstants.GUAGUA_YARN_DEFAULT_MASTER_MEMORY)
                        + "");
    }

    /**
     * Create the mapping of environment vars that will be visible to the
     * ApplicationMaster in its remote app container.
     * 
     * @return a map of environment vars to set up for the AppMaster.
     */
    private Map<String, String> buildEnvironment() {
        Map<String, String> environment = Maps.newHashMap();
        LOG.info("Set the environment for the application master");
        YarnUtils.addLocalClasspathToEnv(environment, getConf());
        LOG.info("Environment for AM : {}", environment);
        return environment;
    }

    /**
     * Create the mapping of files and JARs to send to the GuaguaAppMaster and from there on to the worker tasks.
     * 
     * @return the map of jars to local resource paths for transport to the host
     *         container that will run our AppMaster.
     */
    private Map<String, LocalResource> buildLocalResourceMap() throws IOException {
        return YarnUtils.getLocalResourceMap(getConf(), getAppId());
    }

    /**
     * Utility to make sure we have the cluster resources we need to run this job. If they are not available, we should
     * die here before too much setup.
     * 
     * @param cluster
     *            the GetNewApplicationResponse from the YARN RM.
     */
    private void checkPerNodeResourcesAvailable(final GetNewApplicationResponse cluster) throws YarnException,
            IOException {
        checkAndAdjustPerTaskHeapSize(cluster);
    }

    /**
     * Set delegation tokens for AM container
     * 
     * @param amContainer
     *            AM container
     */
    private void setToken(ContainerLaunchContext amContainer) throws IOException {
        // Setup security tokens
        if(UserGroupInformation.isSecurityEnabled()) {
            Credentials credentials = new Credentials();
            String tokenRenewer = getConf().get(YarnConfiguration.RM_PRINCIPAL);
            if(tokenRenewer == null || tokenRenewer.length() == 0) {
                throw new IOException("Can't get Master Kerberos principal for the RM to use as renewer");
            }
            FileSystem fs = FileSystem.get(getConf());
            // For now, only getting tokens for the default file-system.
            final Token<?>[] tokens = fs.addDelegationTokens(tokenRenewer, credentials);
            if(tokens != null) {
                for(Token<?> token: tokens) {
                    LOG.info("Got dt for " + fs.getUri() + "; " + token);
                }
            }
            DataOutputBuffer dob = new DataOutputBuffer();
            credentials.writeTokenStorageToStream(dob);
            ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
            amContainer.setTokens(fsTokens);
        }
    }

    /**
     * Adjust the user-supplied <code>-yh</code> and <code>-w</code> settings if they are too small or large for the
     * current cluster, and re-record the new settings in the Configuration for export.
     * 
     * @param gnar
     *            the GetNewAppResponse from the YARN ResourceManager.
     */
    private void checkAndAdjustPerTaskHeapSize(final GetNewApplicationResponse gnar) {
        final int maxCapacity = gnar.getMaximumResourceCapability().getMemory();
        // make sure heap size is OK for this cluster's available containers
        int guaguaYarnMem = getConf().getInt(GuaguaYarnConstants.GUAGUA_CHILD_MEMORY,
                GuaguaYarnConstants.GUAGUA_CHILD_DEFAULT_MEMORY);
        if(guaguaYarnMem > maxCapacity) {
            LOG.warn("Guagua's request of heap MB per-task is more than the minimum; downgrading guagua to {} MB.",
                    maxCapacity);
            guaguaYarnMem = maxCapacity;
        }

        getConf().setInt(GuaguaYarnConstants.GUAGUA_CHILD_MEMORY, guaguaYarnMem);
    }

    public ApplicationId getAppId() {
        return appId;
    }

    public void setAppId(ApplicationId appId) {
        this.appId = appId;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getAmQueue() {
        return amQueue;
    }

    public void setAmQueue(String amQueue) {
        this.amQueue = amQueue;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public static void main(String[] args) {
        if(args.length == 0
                || (args.length == 1 && (args[0].equals("h") || args[0].equals("-h") || args[0].equals("-help") || args[0]
                        .equals("help")))) {
            GuaguaOptionsParser.printGenericCommandUsage(System.out);
            System.exit(0);
        }

        long startTime = System.currentTimeMillis();
        int result = 0;
        try {
            GuaguaYarnClient client = new GuaguaYarnClient();
            client.setStartTime(startTime);
            GuaguaOptionsParser parser = parseOpts(args, client.getConf());
            LOG.info("Initializing client.");
            String[] remainingArgs = parser.getRemainingArgs();
            if(!client.init(remainingArgs)) {
                System.exit(-1);
            }
            result = client.run(remainingArgs);
        } catch (Throwable t) {
            LOG.error("Error running yarn client", t);
            System.exit(1);
        }
        if(result == 0) {
            LOG.info("Application completed successfully");
        } else {
            LOG.error("Application failed, please check the diagnosis info.");
        }
        System.exit(result);
    }

}
