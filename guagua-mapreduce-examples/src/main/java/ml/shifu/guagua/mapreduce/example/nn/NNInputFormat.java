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
package ml.shifu.guagua.mapreduce.example.nn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import ml.shifu.guagua.GuaguaConstants;
import ml.shifu.guagua.hadoop.io.GuaguaInputSplit;
import ml.shifu.guagua.mapreduce.GuaguaMRRecordReader;
import ml.shifu.guagua.mapreduce.GuaguaMapReduceConstants;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copy from GuaguaInputformat for test scalablity. We can set nn.test.scale to use more mappers for test.
 */
public class NNInputFormat extends TextInputFormat {

    private static final Logger LOG = LoggerFactory.getLogger(NNInputFormat.class);

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        List<InputSplit> splits = super.getSplits(job);
        List<InputSplit> newSplits = new ArrayList<InputSplit>();
        for(int i = 0; i < job.getConfiguration().getInt(NNConstants.NN_TEST_SCALE, 1); i++) {
            for(InputSplit inputSplit: splits) {
                if(isNotPigOrHadoopMetaFile(((FileSplit) inputSplit).getPath())) {
                    newSplits.add(new GuaguaInputSplit(false, new FileSplit[] { (FileSplit) inputSplit }));
                }
            }
        }
        newSplits.add(new GuaguaInputSplit(true, (FileSplit) null));
        int mapperSize = newSplits.size();
        LOG.info("inputs size including master: {}", mapperSize);
        LOG.debug("input splits inclduing: {}", newSplits);
        job.getConfiguration().set(GuaguaConstants.GUAGUA_WORKER_NUMBER, (mapperSize - 1) + "");
        return newSplits;
    }

    /**
     * Whether it is not pig or hadoop meta output file.
     */
    protected boolean isNotPigOrHadoopMetaFile(Path path) {
        return path.toString().indexOf(GuaguaMapReduceConstants.HADOOP_SUCCESS) < 0
                && path.toString().indexOf(GuaguaMapReduceConstants.PIG_HEADER) < 0
                && path.toString().indexOf(GuaguaMapReduceConstants.PIG_SCHEMA) < 0;
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        // bzip2 can be split.
        if(file.getName().endsWith(GuaguaMapReduceConstants.BZ2)) {
            return true;
        }
        // other compression can not be split, maybe for lzo I should add it to split list.
        CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        return codec == null;
    }

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new GuaguaMRRecordReader(context.getConfiguration().getInt(GuaguaConstants.GUAGUA_ITERATION_COUNT, -1));
    }

}
