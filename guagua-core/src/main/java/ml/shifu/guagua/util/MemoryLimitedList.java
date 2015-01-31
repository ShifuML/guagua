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

import java.util.Iterator;
import java.util.List;

import ml.shifu.guagua.GuaguaRuntimeException;

/**
 * A simple wrapper list with limited byte size.
 * 
 * <p>
 * If current size is over limited size, a GuaguaRuntimeException is added when {@link #add(Object)}.
 * 
 * @author Zhang David (pengzhang@paypal.com)
 */
public class MemoryLimitedList<T> implements AppendList<T> {

    /**
     * Limited size setting
     */
    private long maxByteSize = Long.MAX_VALUE;

    /**
     * Current size of delegated list.
     */
    private long byteSize = 0L;

    /**
     * A delegation list, TODO if LinkedList, current size should be changed.
     */
    private List<T> delegationList;

    private State state = State.WRITE;

    private long count;

    public MemoryLimitedList(long maxSize, List<T> delegationList) {
        super();
        this.maxByteSize = maxSize;
        this.delegationList = delegationList;
    }

    public MemoryLimitedList(List<T> delegationList) {
        super();
        this.delegationList = delegationList;
    }

    @Override
    public boolean append(T t) {
        if(this.state != State.WRITE) {
            throw new IllegalStateException();
        }
        this.count += 1;
        long current = SizeEstimator.estimate(t);
        if(byteSize + current > maxByteSize) {
            throw new GuaguaRuntimeException("List over size limit.");
        } else {
            this.byteSize += current;
            return this.delegationList.add(t);
        }
    }

    @Override
    public Iterator<T> iterator() {
        if(this.state != State.READ) {
            throw new IllegalStateException();
        }

        return new Iterator<T>() {

            private Iterator<T> iter = MemoryLimitedList.this.delegationList.iterator();

            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public T next() {
                return iter.next();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /*
     * (non-Javadoc)
     * 
     * @see ml.shifu.guagua.util.AppendList#switchState()
     */
    @Override
    public void switchState() {
        this.state = State.READ;
    }

    /*
     * (non-Javadoc)
     * 
     * @see ml.shifu.guagua.util.AppendList#size()
     */
    @Override
    public long size() {
        return this.count;
    }

}
