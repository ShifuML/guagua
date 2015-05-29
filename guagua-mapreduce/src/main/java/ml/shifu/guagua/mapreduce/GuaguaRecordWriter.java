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
package ml.shifu.guagua.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * We don't use {@link GuaguaRecordWriter} but hadoop MapReduce needs it.
 */
public class GuaguaRecordWriter extends RecordWriter<Text, Text> {

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        // Do nothing
    }

    @Override
    public void write(Text key, Text value) throws IOException, InterruptedException {
        throw new IOException(String.format("write: Cannot write with %s.  Should never be called", getClass()
                .getName()));
    }
}
