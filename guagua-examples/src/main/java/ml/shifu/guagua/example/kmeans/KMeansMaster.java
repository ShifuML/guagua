/*
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
package ml.shifu.guagua.example.kmeans;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import ml.shifu.guagua.GuaguaRuntimeException;
import ml.shifu.guagua.master.MasterComputable;
import ml.shifu.guagua.master.MasterContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link KMeansMaster} computes new k center points for next iteration.
 * 
 * <p>
 * By accumulating all the k center points sum values from all workers, then average to get new k center points.
 */
public class KMeansMaster implements MasterComputable<KMeansMasterParams, KMeansWorkerParams> {

    private static final Logger LOG = LoggerFactory.getLogger(KMeansMaster.class);

    /**
     * Master computation by accumulating all the k center points sum values from all workers, then average to get new k
     * center points.
     * 
     * @throws NullPointerException
     *             if worker result or worker results is null.
     */
    @Override
    public KMeansMasterParams compute(MasterContext<KMeansMasterParams, KMeansWorkerParams> context) {
        if(context.getWorkerResults() == null) {
            throw new NullPointerException("No worker results received in Master.");
        }

        if(context.getCurrentIteration() == 1) {
            return doFirstIteration(context);
        } else {
            return doOtherIterations(context);
        }
    }

    private KMeansMasterParams doFirstIteration(MasterContext<KMeansMasterParams, KMeansWorkerParams> context) {
        List<double[]> allInitialCentriods = new ArrayList<double[]>();
        boolean initilized = false;
        int k = 0, c = 0;
        for(KMeansWorkerParams workerResult: context.getWorkerResults()) {
            allInitialCentriods.addAll(workerResult.getPointList());
            if(!initilized) {
                k = workerResult.getK();
                c = workerResult.getC();
            }
        }

        if(allInitialCentriods.size() < k) {
            throw new GuaguaRuntimeException(
                    "Error: data size is smaller than k, please check your input and k settings.");
        }

        Collections.sort(allInitialCentriods, new Comparator<double[]>() {
            @Override
            public int compare(double[] o1, double[] o2) {
                double dist = distance(o1) - distance(o2);
                return Double.valueOf(dist).compareTo(Double.valueOf(0d));
            }
        });

        List<double[]> initialCentriods = new ArrayList<double[]>(k);
        int step = allInitialCentriods.size() / k;
        for(int i = 0; i < k; i++) {
            initialCentriods.add(allInitialCentriods.get(i * step));
        }

        KMeansMasterParams masterResult = new KMeansMasterParams();
        masterResult.setK(k);
        masterResult.setC(c);
        masterResult.setPointList(initialCentriods);
        return masterResult;
    }

    private double distance(double[] record) {
        double sumSquare = 0d;
        for(int i = 0; i < record.length; i++) {
            sumSquare += (record[i] * record[i]);
        }

        return Math.sqrt(sumSquare);
    }

    private KMeansMasterParams doOtherIterations(MasterContext<KMeansMasterParams, KMeansWorkerParams> context) {
        // Accumulate all values for all categories
        List<double[]> sumAllList = new LinkedList<double[]>();
        // here use long to avoid over flow
        List<Long> countAllList = new LinkedList<Long>();

        boolean initilized = false;
        int k = 0, c = 0;
        for(KMeansWorkerParams workerResult: context.getWorkerResults()) {
            LOG.debug("Worker result: %s", workerResult);
            if(!initilized) {
                k = workerResult.getK();
                c = workerResult.getC();
            }
            for(int i = 0; i < k; i++) {
                if(!initilized) {
                    sumAllList.add(new double[c]);
                    countAllList.add(0L);
                }

                long currCount = countAllList.get(i);
                countAllList.set(i, currCount + workerResult.getCountList().get(i));

                double[] sumAll = sumAllList.get(i);
                for(int j = 0; j < c; j++) {
                    sumAll[j] += workerResult.getPointList().get(i)[j];
                }
            }
            initilized = true;
        }

        LOG.debug("sumList: %s", (sumAllList));
        LOG.debug("countList: %s", countAllList);

        // Get new center points
        List<double[]> meanList = new LinkedList<double[]>();
        for(int i = 0; i < k; i++) {
            double[] means = new double[c];
            for(int j = 0; j < c; j++) {
                means[j] = sumAllList.get(i)[j] / countAllList.get(i);
            }
            meanList.add(means);
        }

        LOG.debug("meanList: %s", (meanList));

        // Construct new master result with new center points
        KMeansMasterParams masterResult = new KMeansMasterParams();
        masterResult.setK(k);
        masterResult.setC(c);
        masterResult.setPointList(meanList);
        return masterResult;
    }

}
