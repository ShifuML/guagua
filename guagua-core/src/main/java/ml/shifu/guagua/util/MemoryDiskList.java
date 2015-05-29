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

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * A list to store data firstly into memory then into disk if over threshold.
 * 
 * <p>
 * Only two stages support in such kind of list. The first one is WRITE, the next is read. So far random WRITE and READ
 * are not supported in this list.
 * 
 * <p>
 * WARNING: {@link #close()} should be called at last to release file descriptor.
 * 
 * <p>
 * User should provide fileName in constructor and only one file is used to store data.
 * 
 * @author Zhang David (pengzhang@paypal.com)
 */
public class MemoryDiskList<T extends Serializable> implements AppendList<T> {

    /**
     * Limited size setting for memory
     */
    private long maxByteSize = Long.MAX_VALUE;

    /**
     * Current size of delegated list.
     */
    private long byteSize = 0L;

    /**
     * A delegation memory list, TODO if LinkedList, current size should be changed.
     */
    private List<T> delegationList;

    /**
     * Backup {@link DiskList} to store element into disk.
     */
    private DiskList<T> diskList;

    /**
     * Number of elements in this list
     */
    private long count;

    /**
     * Internal current state
     */
    private State state = State.WRITE;

    /**
     * Constructor with max bytes size of memory and {@link #delegationList}. Disk file name is random filename in
     * current working dir.
     */
    public MemoryDiskList(long maxSize, List<T> delegationList) {
        super();
        this.maxByteSize = maxSize;
        this.delegationList = delegationList;
        this.diskList = new DiskList<T>(System.currentTimeMillis() + "");
    }

    /**
     * Constructor with only memory {@link #delegationList}. By default no limit of bytes size in memory.
     */
    public MemoryDiskList(List<T> delegationList) {
        super();
        this.delegationList = delegationList;
        this.diskList = new DiskList<T>(System.currentTimeMillis() + "");
    }

    /**
     * Constructor with max bytes size of memory and {@link #delegationList} and disk file name in current working dir.
     */
    public MemoryDiskList(long maxSize, List<T> delegationList, String fileName) {
        super();
        this.maxByteSize = maxSize;
        this.delegationList = delegationList;
        this.diskList = new DiskList<T>(fileName);
    }

    /**
     * Constructor with max bytes size of memory and disk file name in current working dir. By default
     * {@link LinkedList} is used for memory list.
     */
    public MemoryDiskList(long maxSize, String fileName) {
        super();
        this.maxByteSize = maxSize;
        this.delegationList = new LinkedList<T>();
        this.diskList = new DiskList<T>(fileName);
    }

    /**
     * Constructor with only memory bytes size limit.
     */
    public MemoryDiskList(long maxSize) {
        super();
        this.maxByteSize = maxSize;
        this.delegationList = new LinkedList<T>();
        this.diskList = new DiskList<T>(System.currentTimeMillis() + "");
    }

    /**
     * Default constructor. By default no memory limit, {@link LinkedList} and random file name.
     */
    public MemoryDiskList() {
        super();
        this.delegationList = new LinkedList<T>();
        this.diskList = new DiskList<T>(System.currentTimeMillis() + "");
    }

    @Override
    public boolean append(T t) {
        if(this.state != State.WRITE) {
            throw new IllegalStateException();
        }
        this.count += 1;
        long current = SizeEstimator.estimate(t);
        if(byteSize + current > maxByteSize) {
            this.byteSize += current;
            return this.diskList.append(t);
        } else {
            this.byteSize += current;
            return this.delegationList.add(t);
        }
    }

    /**
     * Close disk stream. Should be called at the end of usage of {@link MemoryDiskList}.
     */
    public void close() {
        if(this.diskList != null) {
            this.diskList.close();
        }
    }

    /**
     * Reopen stream for iteration.
     */
    public void reOpen() {
        this.close();
        if(this.diskList != null) {
            this.diskList.reOpen();
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

            private Iterator<T> iter1 = MemoryDiskList.this.delegationList.iterator();

            private Iterator<T> iter2 = MemoryDiskList.this.diskList.iterator();

            boolean isDisk = false;

            @Override
            public boolean hasNext() {
                if(iter1.hasNext()) {
                    return true;
                }
                isDisk = iter2.hasNext();
                if(!isDisk) {
                    MemoryDiskList.this.close();
                }
                return isDisk;
            }

            @Override
            public T next() {
                if(!isDisk) {
                    return iter1.next();
                } else {
                    return iter2.next();
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public void switchState() {
        this.state = State.READ;
        this.diskList.switchState();
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
        this.delegationList.clear();
        this.diskList.clear();
    }

}
