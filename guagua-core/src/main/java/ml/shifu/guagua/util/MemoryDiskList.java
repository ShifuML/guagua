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

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * A list to store data firstly into memory then into disk if over threshold.
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

    private DiskList<T> diskList;

    private long count;

    private State state = State.WRITE;

    public MemoryDiskList(long maxSize, List<T> delegationList) {
        super();
        this.maxByteSize = maxSize;
        this.delegationList = delegationList;
        this.diskList = new DiskList<T>(System.currentTimeMillis() + "");
    }

    public MemoryDiskList(List<T> delegationList) {
        super();
        this.delegationList = delegationList;
        this.diskList = new DiskList<T>(System.currentTimeMillis() + "");
    }

    public MemoryDiskList(long maxSize, List<T> delegationList, String fileName) {
        super();
        this.maxByteSize = maxSize;
        this.delegationList = delegationList;
        this.diskList = new DiskList<T>(fileName);
    }

    public MemoryDiskList(long maxSize, String fileName) {
        super();
        this.maxByteSize = maxSize;
        this.delegationList = new LinkedList<T>();
        this.diskList = new DiskList<T>(fileName);
    }

    public MemoryDiskList(long maxSize) {
        super();
        this.maxByteSize = maxSize;
        this.delegationList = new LinkedList<T>();
        this.diskList = new DiskList<T>(System.currentTimeMillis() + "");
    }

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

    public void close() {
        this.diskList.close();
    }

    public void reOpen() {
        this.close();
        this.diskList.reOpen();
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

    public static class Data implements Serializable {

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            return "Data [l=" + l + ", d=" + d + "]";
        }

        /**
         * @param l
         * @param d
         */
        public Data(long l, double d) {
            super();
            this.l = l;
            this.d = d;
        }

        /**
         * @return the l
         */
        public long getL() {
            return l;
        }

        /**
         * @param l
         *            the l to set
         */
        public void setL(long l) {
            this.l = l;
        }

        /**
         * @return the d
         */
        public double getD() {
            return d;
        }

        /**
         * @param d
         *            the d to set
         */
        public void setD(double d) {
            this.d = d;
        }

        private static final long serialVersionUID = -663953997290154586L;

        private long l;

        private double d;

    }

    public static void main(String[] args) {

        MemoryDiskList<Data> dataList = new MemoryDiskList<Data>(60L, "c.txt");
        dataList.append(new Data(1, 1));
        dataList.append(new Data(2, 2));
        dataList.append(new Data(3, 3));
        dataList.append(new Data(4, 4));
        dataList.switchState();
        for(Data d: dataList) {
            System.out.println(d);
        }

        dataList.reOpen();

        for(Data d: dataList) {
            System.out.println(d);
        }

        dataList.close();
    }

}
