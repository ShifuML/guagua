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
package ml.shifu.guagua.mapreduce.example.sum;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import junit.framework.Assert;
import ml.shifu.guagua.GuaguaConstants;
import ml.shifu.guagua.mapreduce.GuaguaMRUnitDriver;
import ml.shifu.guagua.mapreduce.GuaguaWritableAdapter;
import ml.shifu.guagua.unit.GuaguaUnitDriver;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.LongWritable;
import org.junit.After;
import org.junit.Test;

/**
 * {@link SumTest} is an example to show how to use in-memory guagua for unit test, no zookeeper server needed for
 * in-memory case.
 */
public class SumTest {

    private static final String SUM_OUTPUT = "sum-output";

    @Test
    public void testSumApp() throws IOException {
        Properties props = new Properties();
        props.setProperty(GuaguaConstants.MASTER_COMPUTABLE_CLASS, SumMaster.class.getName());
        props.setProperty(GuaguaConstants.WORKER_COMPUTABLE_CLASS, SumWorker.class.getName());
        props.setProperty(GuaguaConstants.GUAGUA_ITERATION_COUNT, "10");
        props.setProperty(GuaguaConstants.GUAGUA_MASTER_RESULT_CLASS, LongWritable.class.getName());
        props.setProperty(GuaguaConstants.GUAGUA_WORKER_RESULT_CLASS, LongWritable.class.getName());

        props.setProperty(GuaguaConstants.GUAGUA_MASTER_INTERCEPTERS, SumOutput.class.getName());

        props.setProperty(GuaguaConstants.GUAGUA_INPUT_DIR, getClass().getResource("/sum").toString());

        props.setProperty("guagua.sum.output", SUM_OUTPUT);

        GuaguaUnitDriver<GuaguaWritableAdapter<LongWritable>, GuaguaWritableAdapter<LongWritable>> driver = 
                new GuaguaMRUnitDriver<GuaguaWritableAdapter<LongWritable>, GuaguaWritableAdapter<LongWritable>>(
                props);

        driver.run();

        Assert.assertEquals(15345 + "", FileUtils.readLines(new File(SUM_OUTPUT)).get(0));
    }
    
    @After
    public void tearDown(){
        FileUtils.deleteQuietly(new File(System.getProperty("user.dir") + File.separator + SUM_OUTPUT));
        FileUtils.deleteQuietly(new File(System.getProperty("user.dir") + File.separator + "." + SUM_OUTPUT + ".crc"));
    }

}
