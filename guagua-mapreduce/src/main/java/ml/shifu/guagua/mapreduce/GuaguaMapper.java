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
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import ml.shifu.guagua.GuaguaConstants;
import ml.shifu.guagua.GuaguaRuntimeException;
import ml.shifu.guagua.GuaguaService;
import ml.shifu.guagua.hadoop.io.GuaguaInputSplit;
import ml.shifu.guagua.io.Bytable;
import ml.shifu.guagua.io.GuaguaFileSplit;
import ml.shifu.guagua.master.GuaguaMasterService;
import ml.shifu.guagua.util.Progressable;
import ml.shifu.guagua.worker.GuaguaWorkerService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link GuaguaMapper} is the Hadoop Mapper implementation for both guagua master and guagua workers.
 * 
 * <p>
 * Use <code>(GuaguaInputSplit) context.getInputSplit()</code> to check whether this task is guagua master or guagua
 * worker.
 * 
 * <p>
 * {@link #guaguaService} is the interface for both guagua Master and Worker implementation. According to
 * {@link #isMaster}, master service and worker service will be determined.
 * 
 * <p>
 * Only mapper, no reducer for guagua MapReduce implementation. And in this mapper
 * {@link #run(org.apache.hadoop.mapreduce.Mapper.Context)} is override while
 * {@link #map(Object, Object, org.apache.hadoop.mapreduce.Mapper.Context)} is not since we don't need to iterate mapper
 * raw input.
 */
public class GuaguaMapper<MASTER_RESULT extends Bytable, WORKER_RESULT extends Bytable> extends
        Mapper<LongWritable, Text, Text, Text> {

    private static final Logger LOG = LoggerFactory.getLogger(GuaguaMapper.class);

    /**
     * Whether the mapper task is master.
     */
    private boolean isMaster;

    /**
     * Service instance to call real guagua master or guagua worker logic.
     */
    private GuaguaService guaguaService;

    @Override
    protected void setup(Context context) throws java.io.IOException, InterruptedException {
        GuaguaInputSplit inputSplit = (GuaguaInputSplit) context.getInputSplit();
        this.setMaster(inputSplit.isMaster());
        if(this.isMaster()) {
            context.setStatus("Master initializing ...");
            this.setGuaguaService(new GuaguaMasterService<MASTER_RESULT, WORKER_RESULT>());
        } else {
            context.setStatus("Worker initializing ...");
            this.setGuaguaService(new GuaguaWorkerService<MASTER_RESULT, WORKER_RESULT>());

            List<GuaguaFileSplit> splits = new LinkedList<GuaguaFileSplit>();
            for(int i = 0; i < inputSplit.getFileSplits().length; i++) {
                FileSplit fs = inputSplit.getFileSplits()[i];
                GuaguaFileSplit gfs = new GuaguaFileSplit(fs.getPath().toString(), fs.getStart(), fs.getLength());
                if(inputSplit.getExtensions() != null && i < inputSplit.getExtensions().length) {
                    gfs.setExtension(inputSplit.getExtensions()[i]);
                }
                splits.add(gfs);
            }
            this.getGuaguaService().setSplits(splits);
        }
        Properties props = replaceConfToProps(context.getConfiguration());
        this.getGuaguaService().setAppId(context.getConfiguration().get(GuaguaMapReduceConstants.MAPRED_JOB_ID));
        this.getGuaguaService().setContainerId(
                context.getConfiguration().get(GuaguaMapReduceConstants.MAPRED_TASK_PARTITION));
        this.getGuaguaService().init(props);
        this.getGuaguaService().start();
    }

    /**
     * We have to replace {@link Configuration} to {@link Properties} because of no dependency on hadoop in guagua-core.
     */
    private Properties replaceConfToProps(Configuration configuration) {
        Properties properties = new Properties();
        for(Entry<String, String> entry: configuration) {
            properties.put(entry.getKey(), entry.getValue());
            if(LOG.isDebugEnabled()) {
                if(entry.getKey().startsWith(GuaguaConstants.GUAGUA)) {
                    LOG.debug("{}:{}", entry.getKey(), entry.getValue());
                }
            }
        }
        return properties;
    }

    /**
     * Run guagua service according {@link #isMaster} setting. Iteration, coordination will be included in service
     * running.
     * 
     * <p>
     * {@link #cleanup(org.apache.hadoop.mapreduce.Mapper.Context)} is called in finally block to make sure resources
     * can be cleaned.
     * 
     * <p>
     * Guagua try best to update progress for each iteration. And also task status will be updated in each iteration in
     * hadoop job web ui.
     */
    @Override
    public void run(final Context context) throws IOException, InterruptedException {
        Exception e = null;
        try {
            this.setup(context);
            final int iterations = context.getConfiguration().getInt(GuaguaConstants.GUAGUA_ITERATION_COUNT, -1);
            this.getGuaguaService().run(new Progressable() {

                @Override
                public void progress(int iteration, int totalIteration, String status, boolean isLastUpdate,
                        boolean isKill) {
                    if(isKill) {
                        failTask(null, context.getConfiguration());
                        return;
                    }
                    context.progress();
                    // set currentItertion to GuaguaRecordReader to make sure GuaguaRecordReader can update progress
                    GuaguaMRRecordReader.setCurrentIteration(iteration);
                    // update progress.
                    try {
                        context.nextKeyValue();
                    } catch (IOException e) {
                        throw new GuaguaRuntimeException(e);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    if(isLastUpdate) {
                        LOG.info("Application progress: {}%.", (iteration * 100 / iterations));
                    }

                    // Status will be displayed in Hadoop job ui.
                    if(status != null && status.length() != 0) {
                        context.setStatus(status);
                    }
                }
            });
        } catch (Throwable t) {
            LOG.error("Error in guagua main run method.", t);
            failTask(t, context.getConfiguration());
            e = new GuaguaRuntimeException(t);
        } finally {
            try {
                this.cleanup(context);
            } catch (Throwable t) {
                failTask(t, context.getConfiguration());
                e = new GuaguaRuntimeException(t);
            }
        }

        if(e == null && !this.isMaster) {
            // update worker done counters
            context.getCounter(GuaguaMapReduceConstants.GUAGUA_STATUS, GuaguaMapReduceConstants.DONE_WORKERS)
                    .increment(1L);
        }
        if(e == null && this.isMaster) {
            // update master done counters
            context.getCounter(GuaguaMapReduceConstants.GUAGUA_STATUS, GuaguaMapReduceConstants.MASTER_SUCCESS)
                    .increment(1);
        }
    }

    public static boolean isJobFinised(Configuration conf, int retryCount) throws InterruptedException, IOException {
        int i = 0;
        while(i < retryCount) {
            Thread.sleep(5000);
            org.apache.hadoop.mapred.JobClient jobClient = new org.apache.hadoop.mapred.JobClient(
                    (org.apache.hadoop.mapred.JobConf) conf);
            JobID jobId = JobID.forName(conf.get(GuaguaMapReduceConstants.MAPRED_JOB_ID));
            RunningJob job = jobClient.getJob(jobId);
            Counter counter = job.getCounters().findCounter(GuaguaMapReduceConstants.GUAGUA_STATUS,
                    GuaguaMapReduceConstants.DONE_WORKERS);
            long workerNum = conf.getLong(GuaguaConstants.GUAGUA_WORKER_NUMBER, 0L);
            LOG.info("done workers {} and all workers {}", counter.getValue(), workerNum);
            if(counter.getValue() == workerNum) {
                return true;
            }
            i += 1;
        }
        return false;
    }

    /**
     * In our cluster with hadoop-0.20.2-cdh3u4a, runtime exception is thrown to Child but mapper status doesn't change
     * to failed. We fail this task to make sure our fail-over can make job successful.
     */
    private void failTask(Throwable t, Configuration conf) {
        LOG.error("failtask: Killing task: {} ", conf.get(GuaguaMapReduceConstants.MAPRED_TASK_ID));
        try {
            org.apache.hadoop.mapred.JobClient jobClient = new org.apache.hadoop.mapred.JobClient(
                    (org.apache.hadoop.mapred.JobConf) conf);
            JobID jobId = JobID.forName(conf.get(GuaguaMapReduceConstants.MAPRED_JOB_ID));
            RunningJob job = jobClient.getJob(jobId);
            job.killTask(TaskAttemptID.forName(conf.get(GuaguaMapReduceConstants.MAPRED_TASK_ID)), true);
        } catch (IOException ioe) {
            throw new GuaguaRuntimeException(ioe);
        }
    }

    @SuppressWarnings("unused")
    private void killJob(Configuration conf) {
        LOG.info("Kill job because of master is already finished");
        try {
            org.apache.hadoop.mapred.JobClient jobClient = new org.apache.hadoop.mapred.JobClient(
                    (org.apache.hadoop.mapred.JobConf) conf);
            JobID jobId = JobID.forName(conf.get(GuaguaMapReduceConstants.MAPRED_JOB_ID));
            RunningJob job = jobClient.getJob(jobId);
            job.killJob();
        } catch (IOException ioe) {
            throw new GuaguaRuntimeException(ioe);
        }
    }

    @Override
    protected void cleanup(org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        this.getGuaguaService().stop();
    }

    public boolean isMaster() {
        return isMaster;
    }

    public void setMaster(boolean isMaster) {
        this.isMaster = isMaster;
    }

    public GuaguaService getGuaguaService() {
        return guaguaService;
    }

    public void setGuaguaService(GuaguaService guaguaService) {
        this.guaguaService = guaguaService;
    }

}
