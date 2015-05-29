/*
 * Copyright [2013-2015] PayPal Software Foundation
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
import java.nio.charset.Charset;
import java.util.Arrays;

/**
 * A {@link Bytable} wrapper to wrap some useful information to netty server and client communication.
 */
public class BytableWrapper implements Bytable {

    /**
     * Bytes to store real Bytable object
     */
    private byte[] bytes;

    private int currentIteration;

    private String containerId;

    private boolean isStopMessage;

    public BytableWrapper() {
    }

    /**
     * @return the currentIteration
     */
    public int getCurrentIteration() {
        return currentIteration;
    }

    /**
     * @param currentIteration
     *            the currentIteration to set
     */
    public void setCurrentIteration(int currentIteration) {
        this.currentIteration = currentIteration;
    }

    /**
     * @return the containerId
     */
    public String getContainerId() {
        return containerId;
    }

    /**
     * @param containerId
     *            the containerId to set
     */
    public void setContainerId(String containerId) {
        this.containerId = containerId;
    }

    /**
     * @return the isStopMessage
     */
    public boolean isStopMessage() {
        return isStopMessage;
    }

    /**
     * @param isStopMessage
     *            the isStopMessage to set
     */
    public void setStopMessage(boolean isStopMessage) {
        this.isStopMessage = isStopMessage;
    }

    /**
     * @return the bytes
     */
    public byte[] getBytes() {
        return bytes;
    }

    /**
     * @param bytes
     *            the bytes to set
     */
    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
    }

    /*
     * (non-Javadoc)
     * 
     * @see ml.shifu.guagua.io.Bytable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.currentIteration);
        if(this.containerId == null) {
            out.writeInt(0);
        } else {
            writeBytes(out, this.containerId.getBytes(Charset.forName("UTF-8")));
        }
        out.writeBoolean(this.isStopMessage);
        if(this.bytes != null) {
            out.writeInt(bytes.length);
            for(int i = 0; i < bytes.length; i++) {
                out.writeByte(bytes[i]);
            }
        } else {
            out.writeInt(0);
        }
    }

    private void writeBytes(DataOutput out, byte[] bytes) throws IOException {
        out.writeInt(bytes.length);
        for(int i = 0; i < bytes.length; i++) {
            out.writeByte(bytes[i]);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see ml.shifu.guagua.io.Bytable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.currentIteration = in.readInt();
        int containerIdlen = in.readInt();
        if(containerIdlen != 0) {
            byte[] containerIdbytes = new byte[containerIdlen];
            for(int i = 0; i < containerIdbytes.length; i++) {
                containerIdbytes[i] = in.readByte();
            }
            this.containerId = new String(containerIdbytes, Charset.forName("UTF-8"));
        } else {
            this.containerId = null;
        }
        this.isStopMessage = in.readBoolean();
        int bytesSize = in.readInt();
        if(bytesSize != 0) {
            this.bytes = new byte[bytesSize];
            for(int i = 0; i < this.bytes.length; i++) {
                this.bytes[i] = in.readByte();
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "BytableWrapper [bytes=" + Arrays.toString(bytes) + ", currentIteration=" + currentIteration
                + ", containerId=" + containerId + ", isStopMessage=" + isStopMessage + "]";
    }

}
