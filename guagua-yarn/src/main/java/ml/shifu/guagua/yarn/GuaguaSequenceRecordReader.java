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
package ml.shifu.guagua.yarn;

import java.io.IOException;

import ml.shifu.guagua.io.GuaguaFileSplit;
import ml.shifu.guagua.io.GuaguaRecordReader;
import ml.shifu.guagua.util.ReflectionUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.SequenceFileRecordReader;

/**
 * A reader read HDFS sequence file key by key. The sequence key and value types are both {@link BytesWritable}.
 * 
 * <p>
 * Copy some code from {@link org.apache.hadoop.mapred.SequenceFileRecordReader} but to support
 * {@link GuaguaRecordReader} interface.
 * 
 * <p>
 * If use default constructor, user should also call {@link #initialize(GuaguaFileSplit)} like in below:
 * 
 * <pre>
 * this.setRecordReader(new SequenceFileRecordReader(Text.class, Text,class));
 * this.getRecordReader().initialize(fileSplit);
 * </pre>
 * 
 * or directly use other constructors:
 * 
 * <pre>
 * this.setRecordReader(new SequenceFileRecordReader(fileSplit, Text.class, Text,class));
 * </pre>
 */
public class GuaguaSequenceRecordReader<KEY extends Writable, VALUE extends Writable> implements
        GuaguaRecordReader<GuaguaWritableAdapter<KEY>, GuaguaWritableAdapter<VALUE>> {

    private SequenceFileRecordReader<KEY, VALUE> sequenceReader;

    private Configuration conf;

    private GuaguaWritableAdapter<KEY> key = null;
    private GuaguaWritableAdapter<VALUE> value = null;

    private Class<? extends Writable> keyClass;

    private Class<? extends Writable> valueClass;

    public GuaguaSequenceRecordReader(Class<? extends Writable> keyClass, Class<? extends Writable> valueClass)
            throws IOException {
        this(null, keyClass, valueClass);
    }

    public GuaguaSequenceRecordReader(GuaguaFileSplit split, Class<? extends Writable> keyClass,
            Class<? extends Writable> valueClass) throws IOException {
        this(new Configuration(), split, keyClass, valueClass);
    }

    public GuaguaSequenceRecordReader(Configuration conf, GuaguaFileSplit split, Class<? extends Writable> keyClass,
            Class<? extends Writable> valueClass) throws IOException {
        this.conf = conf;
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        if(split != null) {
            initialize(split);
        }
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
        this.sequenceReader = new SequenceFileRecordReader<KEY, VALUE>(conf, fileSplit);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public boolean nextKeyValue() throws IOException {
        if(key == null) {
            key = new GuaguaWritableAdapter(((Writable) ReflectionUtils.newInstance(this.keyClass)));
        }
        if(value == null) {
            value = new GuaguaWritableAdapter(((Writable) ReflectionUtils.newInstance(this.valueClass)));
        }
        return this.sequenceReader.next(key.getWritable(), value.getWritable());
    }

    @Override
    public GuaguaWritableAdapter<KEY> getCurrentKey() {
        return key;
    }

    @Override
    public GuaguaWritableAdapter<VALUE> getCurrentValue() {
        return value;
    }

    @Override
    public synchronized void close() throws IOException {
        if(sequenceReader != null) {
            sequenceReader.close();
        }
    }

}
