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
package ml.shifu.guagua.mapreduce.example.kmeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import ml.shifu.guagua.io.HaltBytable;

/**
 * {@link KMeansWorkerParams} is the worker results for KMeans distributed guagua application.
 * 
 * Sum values for each k categories are stored and counts are also stored for master to compute global average k center
 * new points.
 */
public class KMeansWorkerParams extends HaltBytable {

    /**
     * K category pre-defined
     */
    private int k;

    /**
     * Dimensions used for each record.
     */
    private int c;

    /**
     * If first iteration,
     */
    private boolean isFirstIteration;

    /**
     * For the first iteration, this is the k initial centroids selected in worker. For other iterations, this is sum
     * value of the new centriods in worker iteration.
     */
    private List<double[]> pointList;

    /**
     * Counts all records for each category.
     */
    private List<Integer> countList;

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
        out.writeBoolean(this.isFirstIteration);
        if(!this.isFirstIteration) {
            if(this.countList != null) {
                for(int i = 0; i < this.k; i++) {
                    out.writeInt(this.countList.get(i));
                }
            }
        }
    }

    private void validate() {
        validateK();
        validateC();
        if(this.k != this.pointList.size()) {
            throw new IllegalArgumentException("In-consistent sum list.");
        }
        if(!this.isFirstIteration) {
            if(this.k != this.countList.size()) {
                throw new IllegalArgumentException("In-consistent count list.");
            }
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
        boolean isFirstIteration = in.readBoolean();
        if(!isFirstIteration) {
            this.countList = new LinkedList<Integer>();
            for(int i = 0; i < this.k; i++) {
                this.countList.add(in.readInt());
            }
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

    public void setPointList(List<double[]> sumList) {
        this.pointList = sumList;
    }

    public List<Integer> getCountList() {
        return countList;
    }

    public void setCountList(List<Integer> countList) {
        this.countList = countList;
    }

    public boolean isFirstIteration() {
        return isFirstIteration;
    }

    public void setFirstIteration(boolean isFirstIteration) {
        this.isFirstIteration = isFirstIteration;
    }

    @Override
    public String toString() {
        return "KMeansWorkerParams [k=" + k + ", c=" + c + ", sumList=" + pointList + ", countList=" + countList + "]";
    }

}
