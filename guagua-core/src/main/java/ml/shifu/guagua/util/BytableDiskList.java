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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;

import ml.shifu.guagua.GuaguaRuntimeException;
import ml.shifu.guagua.io.Bytable;
import ml.shifu.guagua.io.BytableSerializer;

/**
 * A list wrapper to implement store {@link Bytable} data into disk.
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

public class BytableDiskList<T extends Bytable> implements AppendList<T> {
    /**
     * Serializer instance to serialize object to bytes or verse visa.
     */
    private BytableSerializer<T> serializer = new BytableSerializer<T>();

    private DataOutputStream outputStream;

    private DataInputStream inputStream;

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
     * Class name used for serialization.
     */
    private String className;

    /**
     * Constructor with file name in current working dir and instance class name.
     */
    public BytableDiskList(String fileName, String className) {
        this(fileName, className, new BytableSerializer<T>());
    }

    /**
     * Constructor with file name in current working dir, class name and serializer instance.
     */
    public BytableDiskList(String fileName, String className, BytableSerializer<T> serializer) {
        try {
            this.file = new File(fileName);
            this.outputStream = new DataOutputStream(new FileOutputStream(file));
            this.inputStream = new DataInputStream(new FileInputStream(file));
        } catch (FileNotFoundException e) {
            throw new GuaguaRuntimeException(e);
        }
        this.className = className;
        this.serializer = serializer;
    }

    /**
     * Switch state from WRITE to READ
     */
    @Override
    public void switchState() {
        this.state = State.READ;
        try {
            this.outputStream.flush();
        } catch (IOException e) {
            throw new GuaguaRuntimeException(e);
        }
    }

    /**
     * Re-open stream for iterators.
     */
    public void reOpen() {
        close();
        try {
            this.inputStream = new DataInputStream(new FileInputStream(file));
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
        byte[] bytes = getSerializer().objectToBytes(t);
        try {
            this.outputStream.writeInt(bytes.length);
            this.outputStream.write(bytes);
        } catch (IOException e) {
            throw new GuaguaRuntimeException(e);
        }
        return true;
    }

    /**
     * This method should be called at the end of {@link BytableDiskList} usage to release file descriptors.
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
                    return BytableDiskList.this.inputStream.available() > 0;
                } catch (IOException e) {
                    return false;
                }
            }

            @Override
            public T next() {
                try {
                    int length = BytableDiskList.this.inputStream.readInt();
                    byte[] bytes = new byte[length];
                    int size = BytableDiskList.this.inputStream.read(bytes);
                    if(size < 0) {
                        throw new GuaguaRuntimeException("IO exception on reading disk file.");
                    }
                    return BytableDiskList.this.getSerializer().bytesToObject(bytes, className);
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
    public BytableSerializer<T> getSerializer() {
        return serializer;
    }

    /**
     * @param serializer
     *            the serializer to set
     */
    public void setSerializer(BytableSerializer<T> serializer) {
        this.serializer = serializer;
    }

    @Override
    public int size() {
        return (int)this.count;
    }

    /**
     * Delete file to store elements. After this method call, user cannot use such list. But clear here is used to clear
     * resources used.
     */
    @Override
    public void clear() {
        FileUtils.deleteQuietly(file);
    }

}
