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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * {@link GuaguaMRRecordReader} is used as a mock for mapreduce reader interface, not real reading data.
 * 
 * <p>
 * To update progress, {@link #currentIteration} and {@link #totalIterations} should be set. {@link #currentIteration}
 * only can be set in GuaguaMapper.run.
 * 
 * <p>
 * Why set {@link #currentIteration} to static? The reason is that currentIteration for task cannot be transferred to
 * {@link #GuaguaRecordReader} because of no API from MapperContext. So static field here is used to update current
 * iteration.
 * 
 * <p>
 * If {@link #currentIteration} is not set in each iteration. It can only start from 0. This progress update doesn't
 * work well for task fail-over(TODO).
 */
public class GuaguaMRRecordReader extends RecordReader<LongWritable, Text> {
    /** Singular key object */
    private static final LongWritable ONLY_KEY = new LongWritable(0);
    /** Single value object */
    private static final Text ONLY_VALUE = new Text("only value");

    /**
     * This parameter is used to calculate progress.
     */
    private final int totalIterations;

    /**
     * {@link #currentIteration} is set to static, the reason is that no interface to update currentIteration especially
     * task is failed.
     */
    private static int currentIteration;

    /**
     * Default constructor, {@link #totalIterations} is set to default 0.
     */
    public GuaguaMRRecordReader() {
        this(0);
    }

    /**
     * Constructor with {@link #totalIterations} setting.
     * 
     * @param totalIterations
     *            total iterations for such guagua job.
     */
    public GuaguaMRRecordReader(int totalIterations) {
        this.totalIterations = totalIterations;
    }

    @Override
    public void close() throws IOException {
        // currently no logic
    }

    /**
     * Each iteration {@code context.nextKeyValue} should be called, and currentIteration is updated, so the progress is
     * updated.
     */
    @Override
    public float getProgress() throws IOException {
        return currentIteration * 1.0f / this.totalIterations;
    }

    /**
     * This is a mock to hide Hadoop raw map iteration on map input key.
     */
    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return ONLY_KEY;
    }

    /**
     * This is a mock to hide Hadoop raw map iteration on map input value.
     */
    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return ONLY_VALUE;
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        // currently nothing to be initialized
    }

    /**
     * Update iteration number. This is called for each iteration once. It is used to update Hadoop job progress more
     * precisely.
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return currentIteration <= this.totalIterations ? true : false;
    }

    /**
     * Should only be called in GuaguaMapper Progress callback.
     */
    public static void setCurrentIteration(int currentIteration) {
        GuaguaMRRecordReader.currentIteration = currentIteration;
    }

}
