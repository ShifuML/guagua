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
package ml.shifu.guagua.hadoop.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link InputSplit} implementation in guagua for Hadoop MapReduce job.
 * 
 * <p>
 * If mapper with {@link GuaguaInputSplit#isMaster} true means it is master, for master so far {@link #fileSplits} is
 * {@code null}.
 * 
 * <p>
 * For worker, input {@link #fileSplits} are included, here <code>FileSplit</code> array is used to make guagua support
 * combining <code>FileSplit</code>s in one task.
 */
public class GuaguaInputSplit extends InputSplit implements Writable {

    private static final Logger LOG = LoggerFactory.getLogger(GuaguaInputSplit.class);
    /**
     * Whether the input split is master split.
     */
    private boolean isMaster;

    /**
     * File splits used for the task. For master task, it is almost null. Using array here to make guagua
     * support combining small files into one GuaguaInputSplit.
     */
    private FileSplit[] fileSplits;
    
    private Object[] extensions;

    /**
     * Default constructor without any setting.
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
            if(this.extensions != null) {
                out.writeInt(extensions.length);
                for(int i = 0; i < extensions.length; i++) {
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    ObjectOutput ext = null;
                    try {
                        ext = new ObjectOutputStream(bos);
                        ext.writeObject(extensions[i]);
                        byte[] bytes = bos.toByteArray();
                        out.writeInt(bytes.length);
                        out.write(bytes);
                    } finally {
                        IOUtils.closeQuietly(bos);
                    }
                }
            }else{
                out.writeInt(0);
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
            int extLen = in.readInt();
            if(extLen > 0) {
                Object[] exts = new Object[extLen];
                for(int i = 0; i < extLen; i++) {
                    int objectLen = in.readInt();
                    byte[] bytes = new byte[objectLen];
                    in.readFully(bytes);
                    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                    ObjectInput ext = new ObjectInputStream(bis);
                    try {                      
                        Object extension = ext.readObject();
                        exts[i] = extension;
                    } catch (ClassNotFoundException ce) {
                        LOG.error(ce.getMessage(), ce);
                    } finally {
                        IOUtils.closeQuietly(bis);
                    }
                }
               this.setExtensions(exts);
            }
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
     * Data locality functions, return all hosts for all file splits.
     */
    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        if(this.getFileSplits() == null || this.getFileSplits().length == 0) {
            return new String[0];
        }

        List<String> hosts = new ArrayList<String>();
        for(FileSplit fileSplit: this.getFileSplits()) {
            if(fileSplit != null) {
                hosts.addAll(Arrays.asList(fileSplit.getLocations()));
            }
        }

        return hosts.toArray(new String[0]);
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
    
    public Object[] getExtensions() {
        return extensions;
    }

    public void setExtensions(Object[] extensions) {
        this.extensions = extensions;
    }

    @Override
    public String toString() {
        return String
                .format("GuaguaInputSplit [isMaster=%s, fileSplit=%s]", isMaster, Arrays.toString(this.fileSplits));
    }

}
