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
package ml.shifu.guagua.mapreduce.example.sum;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import ml.shifu.guagua.hadoop.io.GuaguaSequenceRecordReader;
import ml.shifu.guagua.hadoop.io.GuaguaWritableAdapter;
import ml.shifu.guagua.io.GuaguaFileSplit;
import ml.shifu.guagua.worker.AbstractWorkerComputable;
import ml.shifu.guagua.worker.WorkerContext;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link SumSequenceFileWorker} is used to accumulate the sum value for each line.
 * 
 * <p>
 * Each line of input should be number.
 * 
 * <p>
 * The master's sum value will be added to current iteration.
 */
public class SumSequenceFileWorker
        extends
        AbstractWorkerComputable<GuaguaWritableAdapter<LongWritable>, GuaguaWritableAdapter<LongWritable>, GuaguaWritableAdapter<Text>, GuaguaWritableAdapter<Text>> {

    private static final Logger LOG = LoggerFactory.getLogger(SumSequenceFileWorker.class);

    private List<Long> list;

    @Override
    public void init(
            WorkerContext<GuaguaWritableAdapter<LongWritable>, GuaguaWritableAdapter<LongWritable>> workerContext) {
        this.list = new LinkedList<Long>();
    }

    @Override
    public GuaguaWritableAdapter<LongWritable> doCompute(
            WorkerContext<GuaguaWritableAdapter<LongWritable>, GuaguaWritableAdapter<LongWritable>> workerContext) {
        long sum = workerContext.getLastMasterResult() == null ? 0l : workerContext.getLastMasterResult().getWritable()
                .get();
        for(long longValue: this.list) {
            sum += longValue;
        }
        LOG.info("worker: {} ; sum: {}", workerContext, sum);
        return new GuaguaWritableAdapter<LongWritable>(new LongWritable(sum));
    }

    @Override
    public void load(GuaguaWritableAdapter<Text> currentKey, GuaguaWritableAdapter<Text> currentValue,
            WorkerContext<GuaguaWritableAdapter<LongWritable>, GuaguaWritableAdapter<LongWritable>> workerContext) {
        this.list.add(Long.parseLong(currentValue.getWritable().toString()));
    }

    @Override
    public void initRecordReader(GuaguaFileSplit fileSplit) throws IOException {
        this.setRecordReader(new GuaguaSequenceRecordReader<Text, Text>(fileSplit, Text.class, Text.class));
        // this.setRecordReader(new GuaguaSequenceAsTextRecordReader(fileSplit));
    }
}
