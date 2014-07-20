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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * {@link InputSplit} implementation in guagua. If mapper with {@link GuaguaInputSplit#isMaster} true means it is
 * master, and the master's FileSplit is {@code null}.
 */
public class GuaguaInputSplit extends InputSplit implements Writable {

    /**
     * Whether the input split is master split.
     */
    private boolean isMaster;

    /**
     * File splits used for that mapper task. For master task, it is almost null. Using array here to make guagua
     * support combining small files into one split.
     */
    private FileSplit[] fileSplits;

    /**
     * Default constructor without any setting
     */
    public GuaguaInputSplit() {
    }

    /**
     * Constructor with {@link #isMaster} and {@link #fileSplits} settings.
     * 
     * @param isMaster
     *            Whether the input split is master split.
     * @param fileSplits
     *            File splits used for mapper task.
     */
    public GuaguaInputSplit(boolean isMaster, FileSplit... fileSplits) {
        this.isMaster = isMaster;
        this.fileSplits = fileSplits;
    }

    /**
     * Constructor with {@link #isMaster} and one FileSplit settings.
     * 
     * @param isMaster
     *            Whether the input split is master split.
     * @param fileSplit
     *            File split used for mapper task.
     */
    public GuaguaInputSplit(boolean isMaster, FileSplit fileSplit) {
        this(isMaster, new FileSplit[] { fileSplit });
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBoolean(this.isMaster());
        if(!this.isMaster()) {
            int length = this.getFileSplits().length;
            out.writeInt(length);
            for(int i = 0; i < length; i++) {
                this.getFileSplits()[i].write(out);
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.setMaster(in.readBoolean());
        if(!isMaster()) {
            int len = in.readInt();
            FileSplit[] splits = new FileSplit[len];
            for(int i = 0; i < len; i++) {
                splits[i] = new FileSplit(null, 0, 0, (String[]) null);
                splits[i].readFields(in);
            }
            this.setFileSplits(splits);
        }
    }

    /**
     * For master split, use <code>Long.MAX_VALUE</code> as its length to make it is the first task for Hadoop job. It
     * is convenient for users to check master in Hadoop UI.
     */
    @Override
    public long getLength() throws IOException, InterruptedException {
        if(isMaster()) {
            return Long.MAX_VALUE;
        }
        long len = 0;
        for(FileSplit split: this.getFileSplits()) {
            len += split.getLength();
        }
        return len;
    }

    /**
     * This is just a mock. TODO, maybe in the future we should use block location.
     */
    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[0];
    }

    public boolean isMaster() {
        return isMaster;
    }

    public void setMaster(boolean isMaster) {
        this.isMaster = isMaster;
    }

    public FileSplit[] getFileSplits() {
        return fileSplits;
    }

    public void setFileSplits(FileSplit[] fileSplits) {
        this.fileSplits = fileSplits;
    }

    @Override
    public String toString() {
        return String.format("GuaguaInputSplit [isMaster=%s, fileSplits=%s]", isMaster,
                Arrays.toString(this.fileSplits));
    }

}
