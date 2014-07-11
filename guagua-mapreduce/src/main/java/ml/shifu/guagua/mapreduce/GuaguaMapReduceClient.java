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
package ml.shifu.guagua.mapreduce;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import ml.shifu.guagua.GuaguaConstants;
import ml.shifu.guagua.coordinator.zk.ZooKeeperUtils;
import ml.shifu.guagua.io.Bytable;
import ml.shifu.guagua.io.HaltBytable;
import ml.shifu.guagua.master.MasterComputable;
import ml.shifu.guagua.worker.WorkerComputable;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link GuaguaMapReduceClient} is the entry point for guagua mapreduce implementation application.
 * 
 * <p>
 * To use it in normal Hadoop mode. Use {@link #main(String[])} as entry point.
 * 
 * <p>
 * To run jobs in parallel:
 * 
 * <pre>
 * GuaguaMapReduceClient client = new GuaguaMapReduceClient();
 * client.addJob(args);
 * client.addJob(args);
 * client.run();
 * </pre>
 */
public class GuaguaMapReduceClient {

    private static final Logger LOG = LoggerFactory.getLogger(GuaguaMapReduceClient.class);

    private static final String INIT_JOB_ID_PREFIX = "Guagua-MapReduce-";

    /**
     * Make guagua run jobs in parallel.
     */
    private JobControl jc;

    /**
     * Default constructor. Construct default JobControl instance.
     */
    public GuaguaMapReduceClient() {
        this.jc = new JobControl(INIT_JOB_ID_PREFIX);
    }

    /**
     * Add new job to JobControl instance.
     */
    public synchronized void addJob(String[] args) throws IOException {
        this.jc.addJob(new ControlledJob(creatJob(args), null));
    }

    /**
     * Run all jobs added to JobControl.
     */
    public synchronized void run() throws IOException {
        // Initially, all jobs are in wait state.
        List<ControlledJob> jobsWithoutIds = this.jc.getWaitingJobList();
        int totalMRJobs = jobsWithoutIds.size();
        LOG.info("{} map-reduce job(s) waiting for submission.", jobsWithoutIds.size());
        Thread jcThread = new Thread(this.jc, "Guagua-MapReduce-JobControl");
        jcThread.start();

        JobClient jobClient = new JobClient(new JobConf(new Configuration()));
        double lastProg = -1;

        Set<String> sucessfulJobs = new HashSet<String>();

        while(!this.jc.allFinished()) {
            try {
                jcThread.join(1000);
            } catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();
            }
            List<ControlledJob> jobsAssignedIdInThisRun = new ArrayList<ControlledJob>(totalMRJobs);

            for(ControlledJob job: jobsWithoutIds) {
                if(job.getJob().getJobID() != null) {
                    jobsAssignedIdInThisRun.add(job);
                    LOG.info("Job {} is started.", job.getJob().getJobID().toString());
                } else {
                    // This job is not assigned an id yet.
                }
            }
            jobsWithoutIds.removeAll(jobsAssignedIdInThisRun);

            List<ControlledJob> successfulJobs = jc.getSuccessfulJobList();
            for(ControlledJob controlledJob: successfulJobs) {
                String jobId = controlledJob.getJob().getJobID().toString();
                if(!sucessfulJobs.contains(jobId)) {
                    LOG.info("Job {} is sucessful.", jobId);
                    sucessfulJobs.add(jobId);
                }
            }
            double prog = calculateProgress(jc, jobClient) / totalMRJobs;
            notifyProgress(prog, lastProg);
            lastProg = prog;

            try {
                Thread.sleep(2 * 1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        List<ControlledJob> successfulJobs = jc.getSuccessfulJobList();

        LOG.info("Sucessful jobs:");
        for(ControlledJob controlledJob: successfulJobs) {
            LOG.info("Job: {} ", controlledJob);
        }
        if(totalMRJobs == successfulJobs.size()) {
            LOG.info("Guagua jobs: 100% complete");
        } else {
            List<ControlledJob> failedJobs = jc.getFailedJobList();
            if(failedJobs.size() > 0) {
                LOG.info("Failed jobs:");
                for(ControlledJob controlledJob: failedJobs) {
                    LOG.warn("Job: {} ", controlledJob);
                }
            }
        }
        this.jc.stop();
    }

    /**
     * Log the progress and notify listeners if there is sufficient progress
     * 
     * @param prog
     *            current progress
     * @param lastProg
     *            progress last time
     */
    private void notifyProgress(double prog, double lastProg) {
        if(prog >= (lastProg + 0.01)) {
            int perCom = (int) (prog * 100);
            if(perCom != 100) {
                LOG.info("Guagua jobs: {}% complete", perCom);
            }
        }
    }

    /**
     * Compute the progress of the current job submitted through the JobControl object jc to the JobClient jobClient
     * 
     * @param jc
     *            The JobControl object that has been submitted
     * @param jobClient
     *            The JobClient to which it has been submitted
     * @return The progress as a percentage in double format
     * @throws IOException
     *             In case any IOException connecting to JobTracker.
     */
    protected double calculateProgress(JobControl jc, JobClient jobClient) throws IOException {
        double prog = 0.0;
        prog += jc.getSuccessfulJobList().size();

        List<ControlledJob> runnJobs = jc.getRunningJobList();
        for(ControlledJob cjob: runnJobs) {
            prog += progressOfRunningJob(cjob, jobClient);
        }
        return prog;
    }

    /**
     * Returns the progress of a Job j which is part of a submitted JobControl object. The progress is for this Job. So
     * it has to be scaled down by the num of jobs that are present in the JobControl.
     * 
     * @param cjob
     *            - The Job for which progress is required
     * @param jobClient
     *            - the JobClient to which it has been submitted
     * @return Returns the percentage progress of this Job
     * @throws IOException
     *             In case any IOException connecting to JobTracker.
     */
    protected double progressOfRunningJob(ControlledJob cjob, JobClient jobClient) throws IOException {
        @SuppressWarnings("deprecation")
        RunningJob rj = jobClient.getJob(cjob.getJob().getJobID().toString());
        if(rj == null && cjob.getJobState() == ControlledJob.State.SUCCESS)
            return 1;
        else if(rj == null)
            return 0;
        else {
            return rj.mapProgress();
        }
    }

    public static void addInputPath(Configuration conf, Path path) throws IOException {
        path = path.getFileSystem(conf).makeQualified(path);
        String dirStr = StringUtils.escapeString(path.toString());
        String dirs = conf.get(GuaguaMapReduceConstants.MAPRED_INPUT_DIR);
        conf.set(GuaguaMapReduceConstants.MAPRED_INPUT_DIR, dirs == null ? dirStr : dirs + "," + dirStr);
    }

    /**
     * Create Hadoop job according to arguments from main.
     */
    public synchronized Job creatJob(String[] args) throws IOException {
        Configuration conf = new Configuration();
        // set it here to make it can be over-written. Set task timeout to a long period 15 minutes.
        conf.setInt(GuaguaMapReduceConstants.MAPRED_TASK_TIMEOUT, 900000);

        GuaguaOptionsParser parser = new GuaguaOptionsParser(conf, args);
        CommandLine cmdLine = parser.getCommandLine();

        checkInputSetting(conf, cmdLine);
        checkZkServerSetting(conf, cmdLine);
        checkWorkerClassSetting(conf, cmdLine);
        checkMasterClassSetting(conf, cmdLine);
        checkIterationCountSetting(conf, cmdLine);
        checkResultClassSetting(conf, cmdLine);
        String name = checkMapReduceNameSetting(cmdLine);
        @SuppressWarnings("rawtypes")
        Class<? extends InputFormat> inputFormatClass = checkInputFormatSetting(cmdLine);

        // set map reduce prameters for specified master-workers achitecture
        // speculative execution should be disabled
        conf.setBoolean(GuaguaMapReduceConstants.MAPRED_MAP_TASKS_SPECULATIVE_EXECUTION, false);
        conf.setBoolean(GuaguaMapReduceConstants.MAPRED_REDUCE_TASKS_SPECULATIVE_EXECUTION, false);

        // Set cache to 0.
        conf.setInt(GuaguaMapReduceConstants.IO_SORT_MB, 0);
        // Most users won't hit this hopefully and can set it higher if desired
        conf.setInt(GuaguaMapReduceConstants.MAPREDUCE_JOB_COUNTERS_LIMIT, 512);
        conf.setInt(GuaguaMapReduceConstants.MAPRED_JOB_REDUCE_MEMORY_MB, 0);
        // Set the ping interval to 5 minutes instead of one minute (DEFAULT_PING_INTERVAL)
        // Client.setPingInterval(conf, 60000 * 5);
        // append concurrent gc to avoid long gc stop-the-world
        String childJavaOpts = conf.get(GuaguaMapReduceConstants.MAPRED_CHILD_JAVA_OPTS, "");
        if(childJavaOpts == null || childJavaOpts.length() == 0) {
            conf.set(GuaguaMapReduceConstants.MAPRED_CHILD_JAVA_OPTS,
                    GuaguaMapReduceConstants.MAPRED_DEFAULT_CHILD_JAVA_OPTS);
        } else {
            String newChildJavaOpts = GuaguaMapReduceConstants.MAPRED_DEFAULT_CHILD_JAVA_OPTS + " " + childJavaOpts;
            conf.set(GuaguaMapReduceConstants.MAPRED_CHILD_JAVA_OPTS, newChildJavaOpts.trim());
        }

        Job job = new Job(conf, name);
        job.setJarByClass(GuaguaMapReduceClient.class);
        job.setMapperClass(GuaguaMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(inputFormatClass);
        job.setOutputFormatClass(GuaguaOutputFormat.class);
        job.setNumReduceTasks(0);
        return job;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static Class<? extends InputFormat> checkInputFormatSetting(CommandLine cmdLine) {
        Class<? extends InputFormat> inputFormatClass = GuaguaInputFormat.class;
        if(cmdLine.hasOption("-inputformat")) {
            String inputFormatClassName = cmdLine.getOptionValue("inputformat");
            try {
                inputFormatClass = (Class<? extends InputFormat>) Class.forName(inputFormatClassName.trim());
            } catch (ClassNotFoundException e) {
                printUsage();
                throw new IllegalArgumentException(String.format(
                        "The inputformat class %s set by '-inputformat' can not be found in class path.",
                        inputFormatClassName.trim()), e);
            } catch (ClassCastException e) {
                printUsage();
                throw new IllegalArgumentException(
                        "Mapreduce input format class set by 'inputformat' should extend 'org.apache.hadoop.mapreduce.InputFormat' class.");
            }
        }
        return inputFormatClass;
    }

    private static String checkMapReduceNameSetting(CommandLine cmdLine) {
        String name = "Guagua Master-Workers Job";
        if(cmdLine.hasOption("-n")) {
            name = cmdLine.getOptionValue("n");
        }
        return name;
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

    private static int checkIterationCountSetting(Configuration conf, CommandLine cmdLine) {
        if(!cmdLine.hasOption("-c")) {
            printUsage();
            throw new IllegalArgumentException("Iteration count should be provided by '-c' parameter.");
        }

        int totalIteration = 0;
        try {
            totalIteration = Integer.parseInt(cmdLine.getOptionValue("c").trim());
        } catch (NumberFormatException e) {
            printUsage();
            throw new IllegalArgumentException("Total iteration number set by '-c' should be a valid number.");
        }
        conf.setInt(GuaguaConstants.GUAGUA_ITERATION_COUNT, totalIteration);
        return totalIteration;
    }

    private static void checkMasterClassSetting(Configuration conf, CommandLine cmdLine) {
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

    private static void printUsage() {
        GuaguaOptionsParser.printGenericCommandUsage(System.out);
        System.out.println("For detailed invalid parameter, please check:");
    }

    private static void checkZkServerSetting(Configuration conf, CommandLine cmdLine) {
        if(!cmdLine.hasOption("-z")) {
            System.err.println("WARN: ZooKeeper server is not set, embeded ZooKeeper server will be started.");
            System.err.println("WARN: For big data guagua application, independent ZooKeeper instance is recommended.");
            System.err.println("WARN: Zookeeper servers can be provided by '-z' parameter with non-empty value.");

            // 1. start embed zookeeper server in one thread.
            int embedZkClientPort = ZooKeeperUtils.startEmbedZooKeeper();
            // 2. check if it is started.
            ZooKeeperUtils.checkIfEmbedZooKeeperStarted(embedZkClientPort);
            // 3. set local embed zookeeper server address
            try {
                conf.set(GuaguaConstants.GUAGUA_ZK_SERVERS, InetAddress.getLocalHost().getHostName() + ":"
                        + embedZkClientPort);
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
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
        addInputPath(conf, new Path(cmdLine.getOptionValue("i").trim()));
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if(args.length == 0
                || (args.length == 1 && (args[0].equals("h") || args[0].equals("-h") || args[0].equals("-help") || args[0]
                        .equals("help")))) {
            GuaguaOptionsParser.printGenericCommandUsage(System.out);
            System.exit(0);
        }

        GuaguaMapReduceClient client = new GuaguaMapReduceClient();
        Job job = client.creatJob(args);
        job.waitForCompletion(true);
    }

}
