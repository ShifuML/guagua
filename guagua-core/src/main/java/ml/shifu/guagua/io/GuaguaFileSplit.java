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

/**
 * {@link GuaguaFileSplit} is used for WorkerComputable to read data.
 * 
 * <p>
 * {@link GuaguaFileSplit} is using to wrap FileSplit in hadoop to make guagua read split data easy. Which is also to
 * make guagua independent from hadoop packages.
 */
public class GuaguaFileSplit {

    /**
     * File path: should be uri like path: 'hdfs://aaaa'.
     */
    private String path;

    /**
     * Offset in {@link #path} to start read data from.
     */
    private long offset;

    /**
     * Size of data read in this split.
     */
    private long length;

    /**
     * Default constructor.
     */
    public GuaguaFileSplit() {
    }

    /**
     * Constructor which is used to build full {@link GuaguaFileSplit}.
     */
    public GuaguaFileSplit(String path, long offset, long length) {
        this.path = path;
        this.offset = offset;
        this.length = length;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }

    @Override
    public String toString() {
        return String.format("FileSplit [path=%s, offset=%s, length=%s]", path, offset, length);
    }

}
