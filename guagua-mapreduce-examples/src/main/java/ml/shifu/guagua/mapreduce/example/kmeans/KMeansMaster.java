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
package ml.shifu.guagua.mapreduce.example.kmeans;

import java.util.LinkedList;
import java.util.List;

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
                    sumAll[j] += workerResult.getSumList().get(i)[j];
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
        masterResult.setMeanList(meanList);
        return masterResult;
    }

}
