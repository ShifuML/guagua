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
package ml.shifu.guagua.io;

import java.io.IOException;

import ml.shifu.guagua.worker.AbstractWorkerComputable;

/**
 * {@link GuaguaRecordReader} is used for consistent interface to iterate data through FileSplit provided. The typical
 * implementation is HDFS implementation in guagua-mapreduce.
 * 
 * <p>
 * To use it, one should set implementation in {@link AbstractWorkerComputable}.
 * 
 * @param <KEY>
 *            key type for each record
 * @param <VALUE>
 *            value type for each record
 */
public interface GuaguaRecordReader<KEY extends Bytable, VALUE extends Bytable> {

    /**
     * Initialize file split for user to create relative reader instance.
     */
    public abstract void initialize(GuaguaFileSplit genericSplit) throws IOException;

    /**
     * Cursor shift to next and set current key value.
     */
    public abstract boolean nextKeyValue() throws IOException;

    /**
     * Tmp we only support LongWritable key for byte offset in each line, follow LineRecordReader in hadoop.
     */
    public abstract KEY getCurrentKey();

    /**
     * Tmp we only support Text value for whole content in each line, follow LineRecordReader in hadoop.
     */
    public abstract VALUE getCurrentValue();

    /**
     * Close resources at last, for example file descriptors. Should be called in finally segment.
     */
    public abstract void close() throws IOException;

}