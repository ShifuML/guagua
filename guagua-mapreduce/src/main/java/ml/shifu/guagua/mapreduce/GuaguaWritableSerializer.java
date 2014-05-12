/**
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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;

import ml.shifu.guagua.GuaguaRuntimeException;
import ml.shifu.guagua.io.Bytable;
import ml.shifu.guagua.io.BytableSerializer;
import ml.shifu.guagua.util.ReflectionUtils;

import org.apache.hadoop.io.Writable;

/**
 * {@link GuaguaWritableSerializer} is to provide the functionality to support use hadoop writable interface.
 * 
 * By using this, one should set result class name to Writable, not {@link Bytable} like this:
 * 
 * <pre>
 * conf.set(GuaguaConstants.GUAGUA_RESULT_CLASS, LongWritable.class.getName());
 * </pre>
 */
public class GuaguaWritableSerializer<RESULT extends Bytable> extends BytableSerializer<RESULT> {

    /**
     * De-serialize from bytes to object. One should provide the instance before de-serializing the object.
     * 
     * @throws NullPointerException
     *             if className or data is null.
     * @throws GuaguaRuntimeException
     *             if any IO exception or other reflection exception.
     */
    @SuppressWarnings("unchecked")
    @Override
    public RESULT bytesToObject(byte[] data, String className) {
        if(data == null || className == null) {
            throw new NullPointerException(String.format(
                    "data and className should not be null. data:%s, className %s", Arrays.toString(data), className));
        }
        Writable writable = (Writable) ReflectionUtils.newInstance(className);
        GuaguaWritableAdapter<Writable> result = new GuaguaWritableAdapter<Writable>(writable);
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
        return (RESULT) result;
    }

}
