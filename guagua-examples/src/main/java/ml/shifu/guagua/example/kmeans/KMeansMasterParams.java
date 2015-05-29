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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import ml.shifu.guagua.io.HaltBytable;

/**
 * {@link KMeansMasterParams} is the master results for KMeans distributed guagua application.
 * 
 * <p>
 * Master result for each iteration is the new k center points after accumulating from all workers' result.
 */
public class KMeansMasterParams extends HaltBytable {

    /**
     * K category pre-defined
     */
    private int k;

    /**
     * How many dimensions used for each record.
     */
    private int c;

    /**
     * For the first iteration, this is the k initial centroids selected. For other iterations, this is the new
     * centriods after worker iteration.
     */
    private List<double[]> pointList;

    @Override
    public void doWrite(DataOutput out) throws IOException {
        validate();
        out.writeInt(this.k);
        out.writeInt(this.c);
        for(int i = 0; i < this.k; i++) {
            for(int j = 0; j < this.c; j++) {
                out.writeDouble(this.pointList.get(i)[j]);
            }
        }
    }

    private void validate() {
        validateK();
        validateC();

        if(this.k != this.pointList.size()) {
            throw new IllegalArgumentException("In-consistent sum list.");
        }
    }

    @Override
    public void doReadFields(DataInput in) throws IOException {
        this.k = in.readInt();
        validateK();
        this.c = in.readInt();
        validateC();
        this.pointList = new LinkedList<double[]>();
        for(int i = 0; i < this.k; i++) {
            double[] units = new double[this.c];
            for(int j = 0; j < this.c; j++) {
                units[j] = in.readDouble();
            }
            this.pointList.add(units);
        }
    }

    private void validateK() {
        if(this.k <= 0) {
            throw new IllegalArgumentException("'k' should be a positive number.");
        }
    }

    private void validateC() {
        if(this.c <= 0) {
            throw new IllegalArgumentException("'c' (cloumn number) should be a positive number.");
        }
    }

    public int getK() {
        return k;
    }

    public void setK(int k) {
        this.k = k;
    }

    public int getC() {
        return c;
    }

    public void setC(int c) {
        this.c = c;
    }

    public List<double[]> getPointList() {
        return pointList;
    }

    public void setPointList(List<double[]> pointList) {
        this.pointList = pointList;
    }
}
