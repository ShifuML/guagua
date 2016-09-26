/*
 * Copyright [2013-2014] PayPal Software Foundation
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import ml.shifu.guagua.GuaguaConstants;
import ml.shifu.guagua.GuaguaRuntimeException;
import ml.shifu.guagua.coordinator.zk.ZooKeeperUtils;
import ml.shifu.guagua.hadoop.io.GuaguaOptionsParser;
import ml.shifu.guagua.hadoop.io.GuaguaWritableSerializer;
import ml.shifu.guagua.hadoop.util.HDPUtils;
import ml.shifu.guagua.io.Bytable;
import ml.shifu.guagua.io.HaltBytable;
import ml.shifu.guagua.master.MasterComputable;
import ml.shifu.guagua.util.ReflectionUtils;
import ml.shifu.guagua.worker.WorkerComputable;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
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
 * 
 * <p>
 * WARNING: In one GuaguaMapReduceClient instance, {@link #addJob(String[])} to make sure job names are no duplicated.
 * 
 * <p>
 * If one job is failed, it will be re-submitted again and try, if failed times over two, no re-try.
 */
public class GuaguaMapReduceClient {

    static {
        // pick up new conf XML file and populate it with stuff exported from client
        Configuration.addDefaultResource(GuaguaConstants.GUAGUA_SITE_FILE);
    }

    private static final Logger LOG = LoggerFactory.getLogger(GuaguaMapReduceClient.class);

    private static final String INIT_JOB_ID_PREFIX = "Guagua-MapReduce-";

    private static String embededZooKeeperServer = null;

    /**
     * Make guagua run jobs in parallel.
     */
    private JobControl jc;

    private int jobIndex = 0;

    private Map<String, Integer> jobIndexMap = new HashMap<String, Integer>();

    private Map<Integer, Integer> jobRunningTimes = new HashMap<Integer, Integer>();

    private Map<Integer, String[]> jobIndexParams = new HashMap<Integer, String[]>();

    private Set<String> failedCheckingJobs = new HashSet<String>();

    private static Map<String, Long> firstMasterSuccessTimeMap = new HashMap<String, Long>();

    private static Set<String> killedSuccessJobSet = new HashSet<String>();

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
        Job job = createJob(args);
        this.jc.addJob(new ControlledJob(job, null));
        if(this.jobIndexMap.containsKey(job.getJobName())) {
            throw new IllegalStateException("Job name should be unique. please check name with: " + job.getJobName());
        }
        this.jobIndexMap.put(job.getJobName(), this.jobIndex);
        this.jobIndexParams.put(this.jobIndex, args);
        this.jobRunningTimes.put(this.jobIndex, 1);
        this.jobIndex += 1;
    }

    /**
     * Run all jobs added to JobControl.
     */
    public void run() throws IOException {
        // Initially, all jobs are in wait state.
        List<ControlledJob> jobsWithoutIds = this.jc.getWaitingJobList();
        int totalNeededMRJobs = jobsWithoutIds.size();
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
            List<ControlledJob> jobsAssignedIdInThisRun = new ArrayList<ControlledJob>(totalNeededMRJobs);

            for(ControlledJob job: jobsWithoutIds) {
                if(job.getJob().getJobID() != null) {
                    jobsAssignedIdInThisRun.add(job);
                    LOG.info("Job {} is started.", job.getJob().getJobID().toString());
                } else {
                    // This job is not assigned an id yet.
                }
            }
            jobsWithoutIds.removeAll(jobsAssignedIdInThisRun);

            List<ControlledJob> runningJobs = jc.getRunningJobList();
            for(ControlledJob controlledJob: runningJobs) {
                String jobId = controlledJob.getJob().getJobID().toString();
                Counters counters = getCounters(controlledJob.getJob());
                Counter doneMaster = counters.findCounter(GuaguaMapReduceConstants.GUAGUA_STATUS,
                        GuaguaMapReduceConstants.MASTER_SUCCESS);
                Counter doneWorkers = counters.findCounter(GuaguaMapReduceConstants.GUAGUA_STATUS,
                        GuaguaMapReduceConstants.DONE_WORKERS);
                if((doneMaster != null && doneMaster.getValue() > 0)
                        || (doneWorkers != null && doneWorkers.getValue() > 0)) {
                    // master is done, while workers may be not, wait for at most 2 minutes check
                    // or workers are all done, while master is not, wait for at most 2 minutes check
                    Long initTime = firstMasterSuccessTimeMap.get(jobId);
                    if(initTime == null) {
                        firstMasterSuccessTimeMap.put(jobId, System.currentTimeMillis());
                    } else {
                        if(System.currentTimeMillis() - initTime >= 2 * 60 * 1000L) {
                            killedSuccessJobSet.add(jobId);
                            killJob(controlledJob.getJob().getConfiguration(), jobId, "Kill job " + jobId
                                    + "because of master is already finished, job " + jobId
                                    + " is treated as successful as we got models. ");
                            // wait extra 1s to wait for job to be stopped
                            try {
                                Thread.sleep(1 * 1000L);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    }
                }
            }

            List<ControlledJob> successfulJobs = jc.getSuccessfulJobList();
            for(ControlledJob controlledJob: successfulJobs) {
                String jobId = controlledJob.getJob().getJobID().toString();
                if(!sucessfulJobs.contains(jobId)) {
                    LOG.info("Job {} is successful.", jobId);
                    sucessfulJobs.add(jobId);
                }
            }

            List<ControlledJob> failedJobs = jc.getFailedJobList();
            for(ControlledJob controlledJob: failedJobs) {
                String failedJobId = controlledJob.getJob().getJobID().toString();
                if(killedSuccessJobSet.contains(failedJobId)) {
                    if(!sucessfulJobs.contains(failedJobId)) {
                        LOG.info("Job {} is successful.", failedJobId);
                        sucessfulJobs.add(failedJobId);
                    }
                    continue;
                }
                if(!this.failedCheckingJobs.contains(failedJobId)) {
                    this.failedCheckingJobs.add(failedJobId);
                    String jobName = controlledJob.getJob().getJobName();
                    Integer jobIndex = this.jobIndexMap.get(jobName);
                    Integer runTimes = this.jobRunningTimes.get(jobIndex);
                    if(runTimes <= 1) {
                        LOG.warn("Job {} is failed, will be submitted again.", jobName);
                        Job newJob = createJob(this.jobIndexParams.get(jobIndex));
                        this.jc.addJob(new ControlledJob(newJob, null));
                        this.jobRunningTimes.put(jobIndex, runTimes + 1);
                        this.jobIndexMap.put(newJob.getJobName(), jobIndex);
                        jobsWithoutIds = this.jc.getWaitingJobList();
                    } else {
                        LOG.warn("Job {} is failed twice, will not be submitted again.", jobName);
                    }
                }
            }
            double prog = calculateProgress(jc, jobClient) / totalNeededMRJobs;
            notifyProgress(prog, lastProg);
            lastProg = prog;

            try {
                Thread.sleep(2 * 1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        List<ControlledJob> successfulJobs = jc.getSuccessfulJobList();
        List<ControlledJob> failedJobs = jc.getFailedJobList();
        LOG.debug("success {}; failed {}; total needed {}", successfulJobs.size(), failedJobs.size(), totalNeededMRJobs);
        for(ControlledJob controlledJob: successfulJobs) {
            LOG.info("Sucessful job:");
            LOG.info("Job: {} ", controlledJob);
        }
        if(totalNeededMRJobs == successfulJobs.size()) {
            LOG.info("Guagua jobs: 100% complete");
            // add failed jobs to debug since all jobs are finished.
            failedJobs = jc.getFailedJobList();
            if(failedJobs != null && failedJobs.size() > 0) {
                for(ControlledJob controlledJob: failedJobs) {
                    Counters counters = getCounters(controlledJob.getJob());
                    if(counters != null) {
                        Counter doneMaster = counters.findCounter(GuaguaMapReduceConstants.GUAGUA_STATUS,
                                GuaguaMapReduceConstants.MASTER_SUCCESS);
                        Counter doneWorkers = counters.findCounter(GuaguaMapReduceConstants.GUAGUA_STATUS,
                                GuaguaMapReduceConstants.DONE_WORKERS);
                        if((doneMaster != null && doneMaster.getValue() > 0)
                                || (doneWorkers != null && doneWorkers.getValue() > 0)) {
                            LOG.info("Successful job although failed state (job is treated as successful):");
                            LOG.warn("Job: {} ", toFakedStateString(controlledJob));
                        } else {
                            LOG.info("Failed job:");
                            LOG.warn("Job: {} ", controlledJob);
                        }
                    }
                }
            }
        } else {
            failedJobs = jc.getFailedJobList();
            if(failedJobs != null && failedJobs.size() > 0) {
                for(ControlledJob controlledJob: failedJobs) {
                    Counters counters = getCounters(controlledJob.getJob());
                    if(counters != null) {
                        Counter doneMaster = counters.findCounter(GuaguaMapReduceConstants.GUAGUA_STATUS,
                                GuaguaMapReduceConstants.MASTER_SUCCESS);
                        Counter doneWorkers = counters.findCounter(GuaguaMapReduceConstants.GUAGUA_STATUS,
                                GuaguaMapReduceConstants.DONE_WORKERS);
                        if((doneMaster != null && doneMaster.getValue() > 0)
                                || (doneWorkers != null && doneWorkers.getValue() > 0)) {
                            LOG.info("Successful job although failed state (job is treated as successful):");
                            LOG.warn("Job: {} ", toFakedStateString(controlledJob));
                        } else {
                            LOG.info("Failed job:");
                            LOG.warn("Job: {} ", controlledJob);
                        }
                    }
                }
            }
        }
        this.jc.stop();
    }

    private static Counters getCounters(Job job) {
        try {
            return job.getCounters();
        } catch (Exception e) {
            // no matter IOException or IllegalStateException, just return null;
            return null;
        }
    }

    private static void killJob(Configuration conf, String jobIdStr, String reason) {
        LOG.info(reason);
        // "Kill job because of master is already finished
        try {
            org.apache.hadoop.mapred.JobClient jobClient = new org.apache.hadoop.mapred.JobClient(
                    (org.apache.hadoop.mapred.JobConf) conf);
            JobID jobId = JobID.forName(jobIdStr);
            RunningJob job = jobClient.getJob(jobId);
            job.killJob();
        } catch (IOException ioe) {
            throw new GuaguaRuntimeException(ioe);
        }
    }

    public String toFakedStateString(ControlledJob controlledJob) {
        StringBuffer sb = new StringBuffer();
        sb.append("job name:\t").append(controlledJob.getJob().getJobName()).append("\n");
        sb.append("job id:\t").append(controlledJob.getJobID()).append("\n");
        sb.append("job state:\t").append("SUCCESS").append("\n");
        sb.append("job mapred id:\t").append(controlledJob.getJob().getJobID()).append("\n");
        sb.append("job message:\t").append(" successful job").append("\n");
        sb.append("job has no depending job:\t").append("\n");
        return sb.toString();
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
     * it has to be scaled down by the number of jobs that are present in the JobControl.
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
    @SuppressWarnings("deprecation")
    public synchronized Job createJob(String[] args) throws IOException {
        Configuration conf = new Configuration();
        // set it here to make it can be over-written. Set task timeout to a long period 20 minutes.
        conf.setInt(GuaguaMapReduceConstants.MAPRED_TASK_TIMEOUT,
                conf.getInt(GuaguaMapReduceConstants.MAPRED_TASK_TIMEOUT, 1800000));
        conf.setInt(GuaguaMapReduceConstants.MAPREDUCE_TASK_TIMEOUT,
                conf.getInt(GuaguaMapReduceConstants.MAPREDUCE_TASK_TIMEOUT, 1800000));
        GuaguaOptionsParser parser = new GuaguaOptionsParser(conf, args);

        // process a bug on hdp 2.2.4
        String hdpVersion = HDPUtils.getHdpVersionForHDP224();
        if(hdpVersion != null && hdpVersion.length() != 0) {
            conf.set("hdp.version", hdpVersion);
            HDPUtils.addFileToClassPath(HDPUtils.findContainingFile("hdfs-site.xml"), conf);
            HDPUtils.addFileToClassPath(HDPUtils.findContainingFile("core-site.xml"), conf);
            HDPUtils.addFileToClassPath(HDPUtils.findContainingFile("mapred-site.xml"), conf);
            HDPUtils.addFileToClassPath(HDPUtils.findContainingFile("yarn-site.xml"), conf);
        }
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

        // set map reduce parameters for specified master-workers architecture
        // speculative execution should be disabled
        conf.setBoolean(GuaguaMapReduceConstants.MAPRED_MAP_TASKS_SPECULATIVE_EXECUTION, false);
        conf.setBoolean(GuaguaMapReduceConstants.MAPRED_REDUCE_TASKS_SPECULATIVE_EXECUTION, false);
        // set mapreduce.job.max.split.locations to 100 to suppress warnings
        int maxSplits = conf.getInt(GuaguaMapReduceConstants.MAPREDUCE_JOB_MAX_SPLIT_LOCATIONS, 100);
        if(maxSplits < 100) {
            maxSplits = 100;
        }
        conf.setInt(GuaguaMapReduceConstants.MAPREDUCE_JOB_MAX_SPLIT_LOCATIONS, maxSplits);

        // Set cache to 0.
        conf.setInt(GuaguaMapReduceConstants.IO_SORT_MB, 0);
        // Most users won't hit this hopefully and can set it higher if desired
        conf.setInt(GuaguaMapReduceConstants.MAPREDUCE_JOB_COUNTERS_LIMIT,
                conf.getInt(GuaguaMapReduceConstants.MAPREDUCE_JOB_COUNTERS_LIMIT, 512));
        conf.setInt(GuaguaMapReduceConstants.MAPRED_JOB_REDUCE_MEMORY_MB, 0);

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
                throw new IllegalArgumentException("Mapreduce input format class set by 'inputformat' should extend "
                        + "'org.apache.hadoop.mapreduce.InputFormat' class.");
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
                if(!ReflectionUtils.hasEmptyParameterConstructor(masterResultClass)) {
                    throw new IllegalArgumentException(
                            "Master result class should have default constuctor without any parameters.");
                }
            } else {
                printUsage();
                throw new IllegalArgumentException(
                        "Master result class name provided by '-mr' parameter should implement "
                                + "'com.paypal.guagua.io.Bytable' or 'org.apache.hadoop.io.Writable'.");
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
                if(!ReflectionUtils.hasEmptyParameterConstructor(workerResultClass)) {
                    throw new IllegalArgumentException(
                            "Worker result class should have default constuctor without any parameters.");
                }
            } else {
                printUsage();
                throw new IllegalArgumentException(
                        "Worker result class name provided by '-wr' parameter should implement "
                                + "'com.paypal.guagua.io.Bytable' or 'org.apache.hadoop.io.Writable'.");
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
            System.err.println("WARN: Total iteration number is not set, default 50 will be used.");
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
        if(!ReflectionUtils.hasEmptyParameterConstructor(masterClass)) {
            throw new IllegalArgumentException("Master class should have default constuctor without any parameters.");
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
        if(!ReflectionUtils.hasEmptyParameterConstructor(workerClass)) {
            throw new IllegalArgumentException("Worker class should have default constuctor without any parameters.");
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
            System.err
                    .println("WARN: For big data guagua application with fail-over zookeeper servers, independent ZooKeeper instances are recommended.");
            System.err.println("WARN: Zookeeper servers can be provided by '-z' parameter with non-empty value.");

            conf.set(GuaguaConstants.GUAGUA_ZK_EMBEDED, "true");
            // change default embedded zookeeper server to master zonde
            boolean isZkInClient = conf.getBoolean(GuaguaConstants.GUAGUA_ZK_EMBEDBED_IS_IN_CLIENT, false);
            if(isZkInClient) {
                synchronized(GuaguaMapReduceClient.class) {
                    if(embededZooKeeperServer == null) {
                        // 1. start embed zookeeper server in one thread.
                        int embedZkClientPort = 0;
                        try {
                            embedZkClientPort = ZooKeeperUtils.startEmbedZooKeeper();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
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
            } else {
                conf.set(
                        GuaguaConstants.GUAGUA_MASTER_SYSTEM_INTERCEPTERS,
                        conf.get(
                                GuaguaConstants.GUAGUA_MASTER_SYSTEM_INTERCEPTERS,
                                "ml.shifu.guagua.master.MasterTimer,ml.shifu.guagua.master.MemoryStatsMasterInterceptor,ml.shifu.guagua.hadoop.ZooKeeperMasterInterceptor,ml.shifu.guagua.master.NettyMasterCoordinator "));
                conf.set(
                        GuaguaConstants.GUAGUA_WORKER_SYSTEM_INTERCEPTERS,
                        conf.get(
                                GuaguaConstants.GUAGUA_WORKER_SYSTEM_INTERCEPTERS,
                                "ml.shifu.guagua.worker.WorkerTimer,ml.shifu.guagua.worker.MemoryStatsWorkerInterceptor,ml.shifu.guagua.hadoop.ZooKeeperWorkerInterceptor,ml.shifu.guagua.worker.NettyWorkerCoordinator"));
                System.err.println("WARN: Zookeeper server will be started in master node of cluster");
            }
            return;
        } else {
            conf.set(GuaguaConstants.GUAGUA_ZK_EMBEDED, "false");
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
        Job job = client.createJob(args);
        job.waitForCompletion(true);
    }

}
