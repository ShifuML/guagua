/*
 * Copyright [2012-2015] PayPal Software Foundation
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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Iterator;

import ml.shifu.guagua.GuaguaRuntimeException;

/**
 * A list wrapper to implement store data into disk.
 * 
 * <p>
 * No size limit for this list but user should make sure valid fileName when constructing.
 * 
 * <p>
 * Only two stages support in such kind of list. The first one is WRITE, the next is read. So far random WRITE and READ
 * are not supported in this list.
 * 
 * <p>
 * WARNING: {@link #close()} should be called at last if use such List.
 * 
 * @author Zhang David (pengzhang@paypal.com)
 */
public class DiskList<T extends Serializable> implements AppendList<T> {

    /**
     * Serializer instance to serialize object to bytes or verse visa.
     */
    private ObjectSerializer<T> serializer = new JavaObjectSerializer<T>();

    private OutputStream outputStream;

    private InputStream inputStream;

    /**
     * Internal state for WRITE or READ
     */
    private State state = State.WRITE;

    /**
     * File used to store elements
     */
    private File file;

    /**
     * Number of elements in this list
     */
    private long count;

    /**
     * Constructor with file name in current working dir.
     */
    public DiskList(String fileName) {
        try {
            this.file = new File(fileName);
            this.outputStream = new FileOutputStream(file);
            this.inputStream = new FileInputStream(file);
        } catch (FileNotFoundException e) {
            throw new GuaguaRuntimeException(e);
        }
    }

    /**
     * Switch state from WRITE to READ
     */
    @Override
    public void switchState() {
        this.state = State.READ;
    }

    /**
     * Re-open stream for iterators.
     */
    public void reOpen() {
        close();
        try {
            this.inputStream = new FileInputStream(file);
        } catch (FileNotFoundException e) {
            throw new GuaguaRuntimeException(e);
        }
    }

    @Override
    public boolean append(T t) {
        if(this.state != State.WRITE) {
            throw new IllegalStateException();
        }
        this.count += 1;
        byte[] bytes = getSerializer().serialize(t);
        try {
            this.outputStream.write(bytes.length);
            this.outputStream.write(bytes);
        } catch (IOException e) {
            throw new GuaguaRuntimeException(e);
        }
        return true;
    }

    /**
     * This method should be called at the end of {@link DiskList} usage to release file descriptors.
     */
    public void close() {
        try {
            this.outputStream.close();
            this.inputStream.close();
        } catch (IOException ignore) {
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Iterable#iterator()
     */
    @Override
    public Iterator<T> iterator() {
        if(this.state != State.READ) {
            throw new IllegalStateException();
        }
        return new Iterator<T>() {

            @Override
            public boolean hasNext() {
                try {
                    return DiskList.this.inputStream.available() > 0;
                } catch (IOException e) {
                    return false;
                }
            }

            @Override
            public T next() {
                try {
                    int length = DiskList.this.inputStream.read();
                    byte[] bytes = new byte[length];
                    int size = DiskList.this.inputStream.read(bytes);
                    if(size < 0) {
                        throw new GuaguaRuntimeException("IO exception on reading disk file.");
                    }
                    return DiskList.this.getSerializer().deserialize(bytes, null);
                } catch (IOException e) {
                    throw new GuaguaRuntimeException(e);
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * @return the serializer
     */
    public ObjectSerializer<T> getSerializer() {
        return serializer;
    }

    /**
     * @param serializer
     *            the serializer to set
     */
    public void setSerializer(ObjectSerializer<T> serializer) {
        this.serializer = serializer;
    }

    @Override
    public long size() {
        return this.count;
    }

    /*
     * (non-Javadoc)
     * 
     * @see ml.shifu.guagua.util.AppendList#clear()
     */
    @Override
    public void clear() {
        FileUtils.deleteQuietly(file);
    }

}
