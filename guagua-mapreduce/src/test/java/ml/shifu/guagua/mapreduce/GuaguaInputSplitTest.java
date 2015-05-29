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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import junit.framework.Assert;

import ml.shifu.guagua.GuaguaRuntimeException;
import ml.shifu.guagua.hadoop.io.GuaguaInputSplit;
import ml.shifu.guagua.util.ReflectionUtils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class GuaguaInputSplitTest {

    private GuaguaInputSplit guaguaInputSplit;

    @Before
    public void setUp() {
        guaguaInputSplit = new GuaguaInputSplit();
    }

    @Test
    public void testMasterInputSplit() {
        guaguaInputSplit = new GuaguaInputSplit(true, (FileSplit) null);
        try {
            Assert.assertEquals(Long.MAX_VALUE, guaguaInputSplit.getLength());
            guaguaInputSplit.getLocations();
            guaguaInputSplit.toString();

            GuaguaInputSplit guaguaInputSplit2 = (GuaguaInputSplit) (bytesToObject(
                    objectToBytes((Writable) guaguaInputSplit), GuaguaInputSplit.class.getName()));

            Assert.assertEquals(guaguaInputSplit2.isMaster(), guaguaInputSplit.isMaster());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testWorkerInputSplit() {
        int length = 10;
        guaguaInputSplit = new GuaguaInputSplit(false, new FileSplit[] { new FileSplit(new Path("a.txt"), 0, length,
                new String[0]) });
        try {
            Assert.assertEquals(length, guaguaInputSplit.getLength());
            guaguaInputSplit.getLocations();
            guaguaInputSplit.toString();
            GuaguaInputSplit guaguaInputSplit2 = (GuaguaInputSplit) (bytesToObject(
                    objectToBytes((Writable) guaguaInputSplit), GuaguaInputSplit.class.getName()));
            Assert.assertEquals(guaguaInputSplit2.isMaster(), guaguaInputSplit.isMaster());
            Assert.assertEquals(guaguaInputSplit2.getFileSplits().length, guaguaInputSplit.getFileSplits().length);
            for(int i = 0; i < guaguaInputSplit2.getFileSplits().length; i++) {
                FileSplit fs1 = guaguaInputSplit.getFileSplits()[i];
                FileSplit fs2 = guaguaInputSplit2.getFileSplits()[i];
                Assert.assertEquals(fs1.getStart(), fs2.getStart());
                Assert.assertEquals(fs1.getLength(), fs2.getLength());
                Assert.assertEquals(fs1.getPath(), fs2.getPath());
                Assert.assertEquals(fs1.getLocations().length, fs2.getLocations().length);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @After
    public void tearDown() {
        guaguaInputSplit = null;
    }

    public byte[] objectToBytes(Writable result) {
        ByteArrayOutputStream out = null;
        DataOutputStream dataOut = null;
        try {
            out = new ByteArrayOutputStream();
            dataOut = new DataOutputStream(out);
            result.write(dataOut);
        } catch (IOException e) {
            throw new GuaguaRuntimeException(e);
        } finally {
            if(dataOut != null) {
                try {
                    dataOut.close();
                } catch (IOException e) {
                    throw new GuaguaRuntimeException(e);
                }
            }
        }
        return out.toByteArray();
    }

    public Writable bytesToObject(byte[] data, String className) {
        if(data == null || className == null) {
            throw new NullPointerException(String.format(
                    "data and className should not be null. data:%s, className:%s",
                    data == null ? null : Arrays.toString(data), className));
        }
        Writable result = (Writable) ReflectionUtils.newInstance(className);
        DataInputStream dataIn = null;
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(data);
            dataIn = new DataInputStream(in);
            result.readFields(dataIn);
        } catch (Exception e) {
            throw new GuaguaRuntimeException(e);
        } finally {
            if(dataIn != null) {
                try {
                    dataIn.close();
                } catch (IOException e) {
                    throw new GuaguaRuntimeException(e);
                }
            }
        }
        return result;
    }

}
