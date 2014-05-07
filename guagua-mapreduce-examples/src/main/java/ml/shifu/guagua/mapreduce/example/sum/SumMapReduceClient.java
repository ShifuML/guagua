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
package ml.shifu.guagua.mapreduce.example.sum;

import ml.shifu.guagua.GuaguaConstants;
import ml.shifu.guagua.mapreduce.GuaguaInputFormat;
import ml.shifu.guagua.mapreduce.GuaguaMapReduceClient;
import ml.shifu.guagua.mapreduce.GuaguaMapReduceConstants;
import ml.shifu.guagua.mapreduce.GuaguaMapper;
import ml.shifu.guagua.mapreduce.GuaguaOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Entry point for Guagua Sum job.
 * 
 * @Deprecated Use {@link GuaguaMapReduceClient} please.
 * 
 */
@Deprecated
public class SumMapReduceClient {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length < 3) {
            throw new IllegalArgumentException(
                    "NNMapReduceClient: Must have at least 2 arguments <guagua.iteration.count> <guagua.zk.servers> <input path or folder>. ");
        }
        conf.set(GuaguaConstants.WORKER_COMPUTABLE_CLASS, SumWorker.class.getName());
        conf.set(GuaguaConstants.MASTER_COMPUTABLE_CLASS, SumMaster.class.getName());
        conf.set(GuaguaConstants.GUAGUA_ITERATION_COUNT, otherArgs[0]);

        conf.set(GuaguaConstants.GUAGUA_ZK_SERVERS, otherArgs[1]);
        conf.setInt(GuaguaConstants.GUAGUA_ZK_SESSION_TIMEOUT, 300 * 1000);
        conf.setInt(GuaguaConstants.GUAGUA_ZK_MAX_ATTEMPTS, 5);
        conf.setInt(GuaguaConstants.GUAGUA_ZK_RETRY_WAIT_MILLS, 1000);

        // if you set result class to hadoop Writable, you must use GuaguaWritableSerializer, this can be avoided by
        // using GuaguaMapReduceClient
        conf.set(GuaguaConstants.GUAGUA_MASTER_RESULT_CLASS, LongWritable.class.getName());
        conf.set(GuaguaConstants.GUAGUA_WORKER_RESULT_CLASS, LongWritable.class.getName());
        conf.set(GuaguaConstants.GUAGUA_MASTER_IO_SERIALIZER, "ml.shifu.guagua.mapreduce.GuaguaWritableSerializer");
        conf.set(GuaguaConstants.GUAGUA_WORKER_IO_SERIALIZER, "ml.shifu.guagua.mapreduce.GuaguaWritableSerializer");

        conf.setBoolean(GuaguaMapReduceConstants.MAPRED_MAP_TASKS_SPECULATIVE_EXECUTION, false);
        conf.setBoolean(GuaguaMapReduceConstants.MAPRED_REDUCE_TASKS_SPECULATIVE_EXECUTION, false);
        conf.setInt(GuaguaMapReduceConstants.MAPRED_TASK_TIMEOUT, 3600000);
        conf.setInt(GuaguaMapReduceConstants.IO_SORT_MB, 0);

        Job job = new Job(conf, "Guagua Sum Master-Workers Job");
        job.setJarByClass(SumMapReduceClient.class);
        job.setMapperClass(GuaguaMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(GuaguaInputFormat.class);
        job.setOutputFormatClass(GuaguaOutputFormat.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
        job.waitForCompletion(true);
    }
}
