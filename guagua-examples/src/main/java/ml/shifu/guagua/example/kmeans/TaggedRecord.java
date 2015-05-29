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
package ml.shifu.guagua.example.kmeans;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Data records with tag. The tag is the index of the k categories range from 0 to k-1.
 */
public class TaggedRecord implements Serializable {

    private static final long serialVersionUID = -7663764463842894467L;

    public static final int INVALID_TAG = -1;

    public TaggedRecord() {
        this(null, INVALID_TAG);
    }

    public TaggedRecord(Double[] record) {
        this(record, INVALID_TAG);
    }

    public TaggedRecord(Double[] record, int tag) {
        this.record = record;
        this.tag = tag;
    }

    /**
     * Data record
     */
    private Double[] record;

    /**
     * Data in which category
     */
    private int tag = INVALID_TAG;

    public Double[] getRecord() {
        return record;
    }

    public void setRecord(Double[] record) {
        this.record = record;
    }

    public int getTag() {
        return tag;
    }

    public void setTag(int tag) {
        this.tag = tag;
    }

    public String toString(String separator) {
        StringBuilder sb = new StringBuilder(200);
        for(int i = 0; i < this.record.length; i++) {
            sb.append(this.record[i]).append(separator);
        }
        sb.append(this.tag);
        return sb.toString();
    }

    @Override
    public String toString() {
        return this.toString("|");
    }

    public static void main(String[] args) {
        System.out.println(Arrays.toString("aaa\u0010bbb".split("\u0010")));
    }

}
