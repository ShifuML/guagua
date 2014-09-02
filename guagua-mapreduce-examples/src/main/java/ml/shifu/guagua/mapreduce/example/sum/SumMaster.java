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

import ml.shifu.guagua.ComputableMonitor;
import ml.shifu.guagua.io.HaltBytable;
import ml.shifu.guagua.mapreduce.GuaguaWritableAdapter;
import ml.shifu.guagua.master.MasterComputable;
import ml.shifu.guagua.master.MasterContext;

import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sum all workers' results together.
 * 
 * <p>
 * If sum value is larger than 1000000L, use {@link HaltBytable} to stop iteration.
 */
@ComputableMonitor
public class SumMaster implements
        MasterComputable<GuaguaWritableAdapter<LongWritable>, GuaguaWritableAdapter<LongWritable>> {

    private static final Logger LOG = LoggerFactory.getLogger(SumMaster.class);

    @Override
    public GuaguaWritableAdapter<LongWritable> compute(
            MasterContext<GuaguaWritableAdapter<LongWritable>, GuaguaWritableAdapter<LongWritable>> context) {
        long sum = 0l;
        if(context.getWorkerResults() == null) {
            LOG.info("Master accumulates worker results with null or empty.");
            return null;
        }
        for(GuaguaWritableAdapter<LongWritable> longWritable: context.getWorkerResults()) {
            if(longWritable != null) {
                sum += longWritable.getWritable().get();
            }
        }
        LOG.info("master:{}", sum);

        GuaguaWritableAdapter<LongWritable> result = new GuaguaWritableAdapter<LongWritable>(new LongWritable(sum));
        if(sum > 1000000L) {
            result.setHalt(true);
        }

        return result;
    }

}
