/**
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Abstract {@link Bytable} implementation to read and store halt status.
 * 
 * <p>
 * One can add halt status for master or worker result. Guagua will determine whether the application should be
 * terminated.
 * 
 * <p>
 * By default master has the only right to stop the application if halt status in master result.
 * 
 * <p>
 * One switch in GuaguaConstants#GUAGUA_WORKER_HALT_ENABLE, if this switch is on, application will be terminated if all
 * workers are halted no matter what is the master halt status.
 * 
 */
public abstract class HaltBytable implements Bytable {

    /**
     * Whether the master or worker is halt.
     */
    private boolean isHalt;

    /**
     * Set status to halt if {@code isHalt} is true.
     */
    public void setHalt(boolean isHalt) {
        this.isHalt = isHalt;
    }

    /**
     * Return halt status.
     */
    public boolean isHalt() {
        return this.isHalt;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBoolean(this.isHalt);
        doWrite(out);
    }

    /**
     * Write fields to out stream.
     */
    public abstract void doWrite(DataOutput out) throws IOException;

    @Override
    public void readFields(DataInput in) throws IOException {
        this.isHalt = in.readBoolean();
        doReadFields(in);
    }

    /**
     * Read fields from in stream.
     */
    public abstract void doReadFields(DataInput in) throws IOException;

}
