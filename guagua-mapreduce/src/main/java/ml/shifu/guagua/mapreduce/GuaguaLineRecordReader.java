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
package ml.shifu.guagua.mapreduce;

import java.io.IOException;

import ml.shifu.guagua.GuaguaConstants;
import ml.shifu.guagua.io.GuaguaFileSplit;
import ml.shifu.guagua.io.GuaguaRecordReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.LineReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copy from LineRecordReader but to support {@link GuaguaRecordReader} interface.
 */
// TODO support more input format
public class GuaguaLineRecordReader implements
        GuaguaRecordReader<GuaguaWritableAdapter<LongWritable>, GuaguaWritableAdapter<Text>> {

    private static final Logger LOG = LoggerFactory.getLogger(GuaguaLineRecordReader.class);

    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long pos;
    private long end;
    private LineReader in;
    private int maxLineLength;
    private GuaguaWritableAdapter<LongWritable> key = null;
    private GuaguaWritableAdapter<Text> value = null;

    public GuaguaLineRecordReader() {
    }

    /*
     * (non-Javadoc)
     * 
     * @see ml.shifu.guagua.mapreduce.RecordReader#initialize(ml.shifu.guagua.io.GuaguaFileSplit)
     */
    @Override
    public void initialize(GuaguaFileSplit genericSplit) throws IOException {
        this.maxLineLength = Integer.MAX_VALUE;
        start = genericSplit.getOffset();
        end = start + genericSplit.getLength();
        final Path file = new Path(genericSplit.getPath());
        compressionCodecs = new CompressionCodecFactory(new Configuration());
        final CompressionCodec codec = compressionCodecs.getCodec(file);

        // open the file and seek to the start of the split
        FileSystem fs = file.getFileSystem(new Configuration());
        FSDataInputStream fileIn = fs.open(file);
        boolean skipFirstLine = false;
        if(codec != null) {
            in = new LineReader(codec.createInputStream(fileIn), GuaguaConstants.DEFAULT_IO_BUFFER_SIZE);
            end = Long.MAX_VALUE;
        } else {
            if(start != 0) {
                skipFirstLine = true;
                --start;
                fileIn.seek(start);
            }
            in = new LineReader(fileIn, GuaguaConstants.DEFAULT_IO_BUFFER_SIZE);
        }
        if(skipFirstLine) { // skip first line and re-establish "start".
            start += in.readLine(new Text(), 0, (int) Math.min((long) Integer.MAX_VALUE, end - start));
        }
        this.pos = start;
    }

    /*
     * (non-Javadoc)
     * 
     * @see ml.shifu.guagua.mapreduce.RecordReader#nextKeyValue()
     */
    @Override
    public boolean nextKeyValue() throws IOException {
        if(key == null) {
            key = new GuaguaWritableAdapter<LongWritable>(new LongWritable());
        }
        key.getWritable().set(pos);
        if(value == null) {
            value = new GuaguaWritableAdapter<Text>(new Text());
        }
        int newSize = 0;
        while(pos < end) {
            newSize = in.readLine(value.getWritable(), maxLineLength,
                    Math.max((int) Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));
            if(newSize == 0) {
                break;
            }
            pos += newSize;
            if(newSize < maxLineLength) {
                break;
            }

            // line too long. try again
            LOG.info("Skipped line of size {} at pos {}", newSize, (pos - newSize));
        }
        if(newSize == 0) {
            key = null;
            value = null;
            return false;
        } else {
            return true;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see ml.shifu.guagua.mapreduce.RecordReader#getCurrentKey()
     */
    @Override
    public GuaguaWritableAdapter<LongWritable> getCurrentKey() {
        return key;
    }

    /*
     * (non-Javadoc)
     * 
     * @see ml.shifu.guagua.mapreduce.RecordReader#getCurrentValue()
     */
    @Override
    public GuaguaWritableAdapter<Text> getCurrentValue() {
        return value;
    }

    /**
     * Get the progress within the split
     */
    public float getProgress() {
        if(start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see ml.shifu.guagua.mapreduce.RecordReader#close()
     */
    @Override
    public synchronized void close() throws IOException {
        if(in != null) {
            in.close();
        }
    }
}
