/*
 * Copyright [2013-2014] PayPal Software Foundation
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * {@link Bytable} defines master and worker results.
 * 
 * <p>
 * {@link Bytable} is coped from Hadoop io Writable. Hadoop Writable was used firstly but {@link Bytable} is used to
 * make guagua-core independent (not depend on any platform).
 * 
 * <p>
 * To use Hadoop original Writable, GuaguaWritableAdapter in guagua-mapreduce module is an adapter to help you re-use
 * Hadoop existing Writable in guagua application.
 * 
 * @see GuaguaWritableAdapter in guagua-mapreduce
 */
public interface Bytable {

    /**
     * Serialize the fields of this object to <code>out</code>.
     * 
     * @param out
     *            <code>DataOuput</code> to serialize this object into.
     * @throws IOException
     *             In case of any io exception.
     */
    void write(DataOutput out) throws IOException;

    /**
     * De-serialize the fields of this object from <code>in</code>.
     * 
     * <p>
     * For efficiency, implementations should attempt to re-use storage in the existing object where possible.
     * </p>
     * 
     * @param in
     *            <code>DataInput</code> to de-seriablize this object from.
     * @throws IOException
     *             In case of any io exception.
     */
    void readFields(DataInput in) throws IOException;
}
