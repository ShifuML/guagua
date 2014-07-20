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

import junit.framework.Assert;

import ml.shifu.guagua.mapreduce.GuaguaWritableAdapter;
import ml.shifu.guagua.mapreduce.GuaguaWritableSerializer;

import org.apache.hadoop.io.IntWritable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class GuaguaWritableTest {

    private GuaguaWritableAdapter<IntWritable> adapter;
    private GuaguaWritableSerializer<GuaguaWritableAdapter<IntWritable>> serializer;

    @Before
    public void setUp() {
        this.adapter = new GuaguaWritableAdapter<IntWritable>(new IntWritable(1));
        this.serializer = new GuaguaWritableSerializer<GuaguaWritableAdapter<IntWritable>>();
    }

    @Test
    public void testReadWrite() throws Exception {
        this.adapter.toString();
        this.adapter.hashCode();
        Assert.assertEquals(this.adapter,
                this.serializer.bytesToObject(this.serializer.objectToBytes(this.adapter), IntWritable.class.getName()));
    }

    @Test
    public void testCommonFunction() throws Exception {
        this.adapter.toString();
        this.adapter.hashCode();
        Assert.assertTrue(this.adapter.equals(this.adapter));
        Assert.assertFalse(this.adapter.equals(null));
        Assert.assertFalse(this.adapter.equals(new Object()));

        GuaguaWritableAdapter<IntWritable> adapter2 = new GuaguaWritableAdapter<IntWritable>(new IntWritable(2));
        Assert.assertFalse(this.adapter.equals(adapter2));

        this.adapter.setHalt(false);
        adapter2.setHalt(true);
        this.adapter.hashCode();
        adapter2.hashCode();
        adapter2.setWritable(new IntWritable(1));
        Assert.assertFalse(this.adapter.equals(adapter2));

        this.adapter.setHalt(false);
        adapter2.setHalt(false);
        this.adapter.hashCode();
        this.adapter.setWritable(new IntWritable(1));
        adapter2.setWritable(new IntWritable(2));
        Assert.assertFalse(this.adapter.equals(adapter2));
        
        this.adapter.setHalt(false);
        adapter2.setHalt(false);
        this.adapter.setWritable(null);
        this.adapter.hashCode();
        adapter2.setWritable(new IntWritable(2));
        Assert.assertFalse(this.adapter.equals(adapter2));
    }
    
    @Test(expected = NullPointerException.class)
    public void testSerializerNullInputs1() {
        this.serializer.bytesToObject(null, IntWritable.class.getName());
    }
    
    @Test(expected = NullPointerException.class)
    public void testSerializerNullInputs2() {
        this.serializer.bytesToObject(null, null);
    }
    
    @Test(expected = NullPointerException.class)
    public void testSerializerNullInputs3() {
        this.serializer.bytesToObject(new byte[]{1}, null);
    }

    @After
    public void tearDown() {
        this.adapter = null;
        this.serializer = null;
    }

}
