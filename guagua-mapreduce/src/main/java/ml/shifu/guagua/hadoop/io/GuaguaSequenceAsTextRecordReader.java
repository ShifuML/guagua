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

import java.io.IOException;

import ml.shifu.guagua.io.GuaguaFileSplit;
import ml.shifu.guagua.io.GuaguaRecordReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

/**
 * A reader read HDFS sequence file key by key. The sequence key and value types are both {@link BytesWritable}.
 * 
 * <p>
 * Copy some code from {@link org.apache.hadoop.mapred.GuaguaSequenceAsTextRecordReader} but to support
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
 * this.setRecordReader(new GuaguaSequenceAsTextRecordReader(fileSplit));
 * </pre>
 */
public class GuaguaSequenceAsTextRecordReader extends GuaguaSequenceRecordReader<Text, Text> {

    public GuaguaSequenceAsTextRecordReader() throws IOException {
        super(Text.class, Text.class);
    }

    public GuaguaSequenceAsTextRecordReader(GuaguaFileSplit split) throws IOException {
        super(split, Text.class, Text.class);
    }

    public GuaguaSequenceAsTextRecordReader(Configuration conf, GuaguaFileSplit split) throws IOException {
        super(conf, split, Text.class, Text.class);
    }
}
