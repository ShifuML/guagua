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
package ml.shifu.guagua.hadoop.io;

import java.io.IOException;

import ml.shifu.guagua.io.GuaguaFileSplit;
import ml.shifu.guagua.io.GuaguaRecordReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.SequenceFileAsBinaryInputFormat.SequenceFileAsBinaryRecordReader;

/**
 * A reader read HDFS sequence file key by key. The sequence key and value types are both {@link BytesWritable}.
 * 
 * <p>
 * Copy some code from {@link org.apache.hadoop.mapred.SequenceFileAsBinaryInputFormat} but to support
 * {@link GuaguaRecordReader} interface.
 * 
 * <p>
 * If use default constructor, user should also call {@link #initialize(GuaguaFileSplit)} like in below:
 * 
 * <pre>
 * this.setRecordReader(new GuaguaSequenceAsBinaryRecordReader());
 * this.getRecordReader().initialize(fileSplit);
 * </pre>
 * 
 * or directly use other constructors:
 * 
 * <pre>
 * this.setRecordReader(new GuaguaSequenceAsBinaryRecordReader(fileSplit));
 * </pre>
 */
public class GuaguaSequenceAsBinaryRecordReader implements
        GuaguaRecordReader<GuaguaWritableAdapter<BytesWritable>, GuaguaWritableAdapter<BytesWritable>> {

    private SequenceFileAsBinaryRecordReader sequenceReader;

    private Configuration conf;

    private GuaguaWritableAdapter<BytesWritable> key = null;
    private GuaguaWritableAdapter<BytesWritable> value = null;

    public GuaguaSequenceAsBinaryRecordReader() {
        this.conf = new Configuration();
    }

    public GuaguaSequenceAsBinaryRecordReader(GuaguaFileSplit split) throws IOException {
        this(new Configuration(), split);
    }

    public GuaguaSequenceAsBinaryRecordReader(Configuration conf, GuaguaFileSplit split) throws IOException {
        this.conf = conf;
        initialize(split);
    }

    /**
     * Return the progress within the input split
     * 
     * @return 0.0 to 1.0 of the input byte range
     */
    public float getProgress() throws IOException {
        return sequenceReader.getProgress();
    }

    @Override
    public void initialize(GuaguaFileSplit split) throws IOException {
        FileSplit fileSplit = new FileSplit(new Path(split.getPath()), split.getOffset(), split.getLength(),
                (String[]) null);
        this.sequenceReader = new SequenceFileAsBinaryRecordReader(conf, fileSplit);
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        if(key == null) {
            key = new GuaguaWritableAdapter<BytesWritable>(new BytesWritable());
        }
        if(value == null) {
            value = new GuaguaWritableAdapter<BytesWritable>(new BytesWritable());
        }
        return this.sequenceReader.next(key.getWritable(), value.getWritable());
    }

    @Override
    public GuaguaWritableAdapter<BytesWritable> getCurrentKey() {
        return key;
    }

    @Override
    public GuaguaWritableAdapter<BytesWritable> getCurrentValue() {
        return value;
    }

    @Override
    public synchronized void close() throws IOException {
        if(sequenceReader != null) {
            sequenceReader.close();
        }
    }

}
