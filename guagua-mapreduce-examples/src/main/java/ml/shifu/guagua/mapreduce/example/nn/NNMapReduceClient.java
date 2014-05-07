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
package ml.shifu.guagua.mapreduce.example.nn;

import java.io.IOException;

import ml.shifu.guagua.GuaguaConstants;
import ml.shifu.guagua.mapreduce.GuaguaMapReduceClient;
import ml.shifu.guagua.mapreduce.GuaguaMapReduceConstants;
import ml.shifu.guagua.mapreduce.GuaguaMapper;
import ml.shifu.guagua.mapreduce.GuaguaOutputFormat;
import ml.shifu.guagua.mapreduce.example.nn.meta.NNParams;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Main class for NN distributed computation.
 * 
 * @Deprecated Use {@link GuaguaMapReduceClient} please.
 * 
 */
@Deprecated
public class NNMapReduceClient {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length < 6) {
            throw new IllegalArgumentException(
                    "NNMapReduceClient: Must have at least 5 arguments <guagua.iteration.count> <guagua.zk.servers> <nn.test.scale> <nn.record.scales> <input path or folder> <guagua.nn.output>. ");
        }
        conf.set(GuaguaConstants.WORKER_COMPUTABLE_CLASS, NNWorker.class.getName());
        conf.set(GuaguaConstants.MASTER_COMPUTABLE_CLASS, NNMaster.class.getName());
        conf.set(GuaguaConstants.GUAGUA_ITERATION_COUNT, otherArgs[0]);

        conf.set(GuaguaConstants.GUAGUA_ZK_SERVERS, otherArgs[1]);

        conf.set(NNConstants.NN_TEST_SCALE, otherArgs[2]);
        conf.set(NNConstants.NN_RECORD_SCALE, otherArgs[3]);

        conf.set(GuaguaConstants.GUAGUA_MASTER_RESULT_CLASS, NNParams.class.getName());
        conf.set(GuaguaConstants.GUAGUA_WORKER_RESULT_CLASS, NNParams.class.getName());

        conf.setInt(NNConstants.GUAGUA_NN_INPUT_NODES, NNConstants.GUAGUA_NN_DEFAULT_INPUT_NODES);
        conf.setInt(NNConstants.GUAGUA_NN_HIDDEN_NODES, NNConstants.GUAGUA_NN_DEFAULT_HIDDEN_NODES);
        conf.setInt(NNConstants.GUAGUA_NN_OUTPUT_NODES, NNConstants.GUAGUA_NN_DEFAULT_OUTPUT_NODES);
        conf.set(NNConstants.GUAGUA_NN_ALGORITHM, NNConstants.GUAGUA_NN_DEFAULT_ALGORITHM);
        conf.setInt(NNConstants.GUAGUA_NN_THREAD_COUNT, NNConstants.GUAGUA_NN_DEFAULT_THREAD_COUNT);
        conf.set(NNConstants.GUAGUA_NN_LEARNING_RATE, NNConstants.GUAGUA_NN_DEFAULT_LEARNING_RATE);

        conf.set(NNConstants.GUAGUA_NN_OUTPUT, otherArgs[5]);

        conf.set(GuaguaConstants.GUAGUA_MASTER_INTERCEPTERS, NNOutput.class.getName());

        conf.setBoolean(GuaguaMapReduceConstants.MAPRED_MAP_TASKS_SPECULATIVE_EXECUTION, false);
        conf.setBoolean(GuaguaMapReduceConstants.MAPRED_REDUCE_TASKS_SPECULATIVE_EXECUTION, false);
        conf.setInt(GuaguaMapReduceConstants.MAPRED_TASK_TIMEOUT, 3600000);
        conf.setInt(GuaguaMapReduceConstants.IO_SORT_MB, 0);

        Job job = new Job(conf, "Guagua NN Master-Workers Job");
        job.setJarByClass(NNMapReduceClient.class);
        job.setMapperClass(GuaguaMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(NNInputFormat.class);
        job.setOutputFormatClass(GuaguaOutputFormat.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path(otherArgs[4]));
        job.waitForCompletion(true);
    }

}
