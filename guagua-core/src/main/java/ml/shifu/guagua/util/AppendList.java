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

/**
 * A appendable list interface definition.
 * 
 * <p>
 * Such list doesn't support randomly read and write. All data should be append into this list and then can be read one
 * by one.
 * 
 * <p>
 * Such code as an example:
 * 
 * <pre>
 * AppendList&lt;Data&gt; dataList = new MemoryDiskList&lt;Data&gt;(60L, &quot;c.txt&quot;);
 * dataList.append(new Data(1, 1));
 * dataList.append(new Data(2, 2));
 * dataList.append(new Data(3, 3));
 * dataList.append(new Data(4, 4));
 * 
 * dataList.switchState();
 * 
 * for(Data d: dataList) {
 *     System.out.println(d);
 * }
 * </pre>
 * 
 * @author Zhang David (pengzhang@paypal.com)
 */
public interface AppendList<T> extends Iterable<T> {

    /**
     * Append object to this list
     * 
     * @param t
     *            the object to append
     * @return whether appending object successful
     * @throws IllegalStateException
     *             if not in WRITE state.
     */
    boolean append(T t);

    /**
     * After appending, switch state from WRITE to READ to allow iterating.
     */
    void switchState();
    
    /**
     * Return size of this list.
     */
    long size();

    /**
     * Inner state definition.
     */
    public static enum State {
        WRITE, READ;
    }

}
