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
package ml.shifu.guagua.example.sum;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import ml.shifu.guagua.ComputableMonitor;
import ml.shifu.guagua.hadoop.io.GuaguaLineRecordReader;
import ml.shifu.guagua.hadoop.io.GuaguaWritableAdapter;
import ml.shifu.guagua.io.GuaguaFileSplit;
import ml.shifu.guagua.util.MemoryDiskList;
import ml.shifu.guagua.worker.AbstractWorkerComputable;
import ml.shifu.guagua.worker.WorkerContext;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link SumWorker} is used to accumulate the sum value for each line.
 * 
 * <p>
 * Each line of input should be number.
 * 
 * <p>
 * The master's sum value will be added to current iteration.
 */
@ComputableMonitor(timeUnit = TimeUnit.SECONDS, duration = 60)
public class SumWorker
        extends
        AbstractWorkerComputable<GuaguaWritableAdapter<LongWritable>, GuaguaWritableAdapter<LongWritable>, GuaguaWritableAdapter<LongWritable>, GuaguaWritableAdapter<Text>> {

    private static final Logger LOG = LoggerFactory.getLogger(SumWorker.class);

    /**
     * A list to store data into memory and disk.
     */
    private MemoryDiskList<Long> list;

    @Override
    public void init(WorkerContext<GuaguaWritableAdapter<LongWritable>, GuaguaWritableAdapter<LongWritable>> context) {
        double memoryFraction = Double.valueOf(context.getProps().getProperty("guagua.data.memoryFraction", "0.5"));
        String tmpFolder = context.getProps().getProperty("guagua.data.tmpfolder", System.getProperty("user.dir"));
        this.list = new MemoryDiskList<Long>((long) (Runtime.getRuntime().maxMemory() * memoryFraction), tmpFolder
                + File.separator + System.currentTimeMillis());
        // cannot find a good place to close these two data set, using Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                SumWorker.this.list.close();
            }
        }));
    }

    @Override
    public GuaguaWritableAdapter<LongWritable> doCompute(
            WorkerContext<GuaguaWritableAdapter<LongWritable>, GuaguaWritableAdapter<LongWritable>> context) {
        long sum = context.getLastMasterResult() == null ? 0l : context.getLastMasterResult().getWritable().get();
        this.list.reOpen();

        for(Long longValue: this.list) {
            sum += longValue;
        }

        try {
            Thread.sleep(5 * 1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        LOG.info("worker: {} ; sum: {}", context, sum);
        return new GuaguaWritableAdapter<LongWritable>(new LongWritable(sum));
    }

    @Override
    public void load(GuaguaWritableAdapter<LongWritable> currentKey, GuaguaWritableAdapter<Text> currentValue,
            WorkerContext<GuaguaWritableAdapter<LongWritable>, GuaguaWritableAdapter<LongWritable>> context) {
        this.list.append(Long.parseLong(currentValue.getWritable().toString()));
    }

    @Override
    protected void postLoad(
            WorkerContext<GuaguaWritableAdapter<LongWritable>, GuaguaWritableAdapter<LongWritable>> context) {
        this.list.switchState();
    }

    @Override
    public void initRecordReader(GuaguaFileSplit fileSplit) throws IOException {
        this.setRecordReader(new GuaguaLineRecordReader(fileSplit));
    }
}
