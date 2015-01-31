/*
 * Copyright [2012-2015] eBay Software Foundation
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
package ml.shifu.guagua.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import ml.shifu.guagua.GuaguaRuntimeException;

/**
 * TODO
 * 
 * @author Zhang David (pengzhang@paypal.com)
 */
public class JavaSerializer<T> implements Serializer<T> {

    /*
     * (non-Javadoc)
     * 
     * @see ml.shifu.guagua.util.Serializer#serialize(java.lang.Object)
     */
    @Override
    public byte[] serialize(T t) {
        ObjectOutputStream output = null;
        try {
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            output = new ObjectOutputStream(bout);
            output.writeObject(t);
            return bout.toByteArray();
        } catch (IOException e) {
            throw new GuaguaRuntimeException(e);
        } finally {
            if(output != null) {
                try {
                    output.close();
                } catch (IOException ignore) {
                }
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see ml.shifu.guagua.util.Serializer#deserialize(byte[], java.lang.Class)
     */
    @SuppressWarnings("unchecked")
    @Override
    public T deserialize(byte[] bytes, Class<?> clazz) {
        ObjectInputStream input = null;
        try {
            ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
            input = new ObjectInputStream(bin);
            return (T) input.readObject();
        } catch (IOException e) {
            throw new GuaguaRuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new GuaguaRuntimeException(e);
        } finally {
            if(input != null) {
                try {
                    input.close();
                } catch (IOException ignore) {
                }
            }
        }
    }

}
