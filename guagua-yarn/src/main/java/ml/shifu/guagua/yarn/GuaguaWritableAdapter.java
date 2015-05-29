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
package ml.shifu.guagua.yarn;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import ml.shifu.guagua.GuaguaConstants;
import ml.shifu.guagua.io.Bytable;
import ml.shifu.guagua.io.HaltBytable;

import org.apache.hadoop.io.Writable;

/**
 * {@link GuaguaWritableAdapter} is used to adapt hadoop io to {@link Bytable} interface.
 * 
 * <p>
 * The reason we use our own writable interface is that we'd like to not depend on hadoop io on guagua core service.
 * 
 * <p>
 * By using this adapter we don't need to implement such as IntWritable, LongWritable, Text and other common writable
 * implementations.
 * 
 * <p>
 * Notice: If you use GuaguaWritableAdapter as your result class, you should provide the inner writable class name to
 * {@link GuaguaConstants#GUAGUA_MASTER_RESULT_CLASS} or {@link GuaguaConstants#GUAGUA_WORKER_RESULT_CLASS} setting.
 * That means to set '-mr' or 'wr' to hadoop writable class name.
 * 
 * Here is a sample:
 * 
 * {@code
 * hadoop jar ../lib/guagua-mapreduce-0.0.1-SNAPSHOT.jar \
 *     ml.shifu.guagua.mapreduce.GuaguaMapReduceClient   \
 *     -libjars ../lib/guagua-mapreduce-examples-0.0.1-SNAPSHOT.jar,../lib/guava-r09-jarjar.jar,../lib/encog-core-3.1.0.jar,../lib/guagua-mapreduce-0.0.1-SNAPSHOT.jar,../lib/guagua-core-0.0.1-SNAPSHOT.jar,../lib/zookeeper-3.4.5.jar \
 *     -i &lt;input&gt;  \
 *     -z &lt;zkhost:zkport&gt;  \
 *     -w ml.shifu.guagua.example.sum.SumWorker  \
 *     -m ml.shifu.guagua.example.sum.SumMaster  \
 *     -c 10 \
 *     -n "Guagua Sum Master-Workers Job" \
 *     -mr org.apache.hadoop.io.LongWritable \
 *     -wr org.apache.hadoop.io.LongWritable \
 *     $queue_opts}
 * 
 * <p>
 * If you set your own main class, not use {@link GuaguaYarnClient}, you can set the result class like this.
 * 
 * <pre>
 * {@code  
 *         conf.set(GuaguaConstants.GUAGUA_MASTER_RESULT_CLASS, LongWritable.class.getName());
 *         conf.set(GuaguaConstants.GUAGUA_WORKER_RESULT_CLASS, LongWritable.class.getName());
 * }
 * </pre>
 * 
 * @see GuaguaWritableSerializer
 * 
 * @deprecated use {@link ml.shifu.guagua.hadoop.io.GuaguaWritableAdapter}
 */
@Deprecated
public class GuaguaWritableAdapter<W extends Writable> extends HaltBytable {

    /**
     * Hadoop Writable instance.
     */
    private W writable;

    /**
     * Contructor with Hadoop Writable setting.
     * 
     * @param writable
     *            Hadoop Writable instance
     */
    public GuaguaWritableAdapter(W writable) {
        this.setWritable(writable);
    }

    @Override
    public void doWrite(DataOutput out) throws IOException {
        getWritable().write(out);
    }

    @Override
    public void doReadFields(DataInput in) throws IOException {
        getWritable().readFields(in);
    }

    public W getWritable() {
        return writable;
    }

    public void setWritable(W writable) {
        this.writable = writable;
    }

    @Override
    public String toString() {
        return String.format("GuaguaWritableAdapter [writable=%s, isHalt=%s]", this.getWritable(), super.isHalt());
    }

}
