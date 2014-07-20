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
package ml.shifu.guagua.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import ml.shifu.guagua.GuaguaRuntimeException;
import ml.shifu.guagua.util.ReflectionUtils;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;

/**
 * {@link Bzip2BytableSerializer} is using bzip2 compression algorithm for serialization.
 * 
 * <p>
 * Byte array by using {@link Bzip2BytableSerializer} is bzip2 format.
 * 
 * @see Serializer
 * @see BytableSerializer
 */
public class Bzip2BytableSerializer<RESULT extends Bytable> implements Serializer<RESULT> {

    /**
     * Serialize from object to bytes.
     * 
     * @throws NullPointerException
     *             if result is null.
     * @throws GuaguaRuntimeException
     *             if any io exception.
     */
    @Override
    public byte[] objectToBytes(RESULT result) {
        ByteArrayOutputStream out = null;
        DataOutputStream dataOut = null;
        try {
            out = new ByteArrayOutputStream();
            OutputStream gzipOutput = new BZip2CompressorOutputStream(out);
            dataOut = new DataOutputStream(gzipOutput);
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

    /**
     * De-serialize from bytes to object. One should provide the class name before de-serializing the object.
     * 
     * @throws NullPointerException
     *             if className or data is null.
     * @throws GuaguaRuntimeException
     *             if any io exception or other reflection exception.
     */
    @Override
    public RESULT bytesToObject(byte[] data, String className) {
        if(data == null || className == null) {
            throw new NullPointerException(String.format(
                    "data and className should not be null. data:%s, className:%s", Arrays.toString(data), className));
        }
        @SuppressWarnings("unchecked")
        RESULT result = (RESULT) ReflectionUtils.newInstance(className);
        DataInputStream dataIn = null;
        try {
            InputStream in = new ByteArrayInputStream(data);
            InputStream gzipInput = new BZip2CompressorInputStream(in);
            dataIn = new DataInputStream(gzipInput);
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
