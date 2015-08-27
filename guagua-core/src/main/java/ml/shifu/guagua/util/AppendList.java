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

/**
 * Appendable list interface definition.
 * 
 * <p>
 * Two states are in this list. State {@link State#WRITE} when instance created, then element can be append to tail.
 * After all elements are appended, switch state to {@link State#READ}. When state is {@link State#READ}, data cannot be
 * appended, only can be iterative.
 * 
 * <p>
 * Only support appending element to tail and iterative reading.
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
    int size();

    /**
     * Clear all elements.
     */
    void clear();

    /**
     * Inner state definition.
     */
    public static enum State {
        WRITE, READ;
    }

}
