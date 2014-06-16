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

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import ml.shifu.guagua.io.GuaguaFileSplit;
import ml.shifu.guagua.mapreduce.GuaguaLineRecordReader;
import ml.shifu.guagua.mapreduce.GuaguaWritableAdapter;
import ml.shifu.guagua.worker.AbstractWorkerComputable;
import ml.shifu.guagua.worker.WorkerContext;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;

/**
 * {@link KMeansWorker} re-computes each record tagged with new category.
 * 
 * <p>
 * To calculate new k centers in master, {@link KMeansWorker} also help to accumulate worker info for new k centers by
 * using sum list and count list.
 */
public class KMeansWorker
        extends
        AbstractWorkerComputable<KMeansMasterParams, KMeansWorkerParams, GuaguaWritableAdapter<LongWritable>, GuaguaWritableAdapter<Text>> {

    private static final Logger LOG = LoggerFactory.getLogger(KMeansWorker.class);

    /**
     * Data list of current worker cached in memory.
     */
    private List<TaggedRecord> dataList;

    /**
     * K categories pre-defined
     */
    private int k;

    /**
     * Columns (dimensions) for each record
     */
    private int c;

    /**
     * Separator to split data for each record
     */
    private String separator;

    /**
     * Reading input line by line
     */
    @Override
    public void initRecordReader(GuaguaFileSplit fileSplit) throws IOException {
        this.setRecordReader(new GuaguaLineRecordReader());
        this.getRecordReader().initialize(fileSplit);
    }

    @Override
    public void init(WorkerContext<KMeansMasterParams, KMeansWorkerParams> workerContext) {
        this.k = Integer.parseInt(workerContext.getProps().getProperty(KMeansContants.KMEANS_K_NUMBER));
        this.c = Integer.parseInt(workerContext.getProps().getProperty(KMeansContants.KMEANS_COLUMN_NUMBER));
        this.separator = workerContext.getProps().getProperty(KMeansContants.KMEANS_DATA_SEPERATOR);
        this.dataList = new LinkedList<TaggedRecord>();
        // just set into worker context for data output interceptor usage.
        workerContext.setAttachment(this.dataList);
    }

    /**
     * Using the new k centers to tag each record with index denoting the record belongs to which category.
     */
    @Override
    public KMeansWorkerParams doCompute(WorkerContext<KMeansMasterParams, KMeansWorkerParams> workerContext) {
        if(workerContext.getCurrentIteration() == 1) {
            return doFirstIteration(workerContext);
        } else {
            return doOtherIterations(workerContext);
        }
    }

    private KMeansWorkerParams doFirstIteration(WorkerContext<KMeansMasterParams, KMeansWorkerParams> workerContext) {
        KMeansWorkerParams workerResult = new KMeansWorkerParams();
        workerResult.setK(this.k);
        workerResult.setC(this.c);
        workerResult.setFirstIteration(true);
        int dataSize = this.dataList.size();
        List<double[]> pointList = new ArrayList<double[]>(dataSize);
        if(this.k >= dataSize) {
            for(TaggedRecord record: this.dataList) {
                pointList.add(toDouble(record));
            }
        } else {
            int m = dataSize / this.k;
            for(int i = 0; i < this.k; i++) {
                pointList.add(toDouble(this.dataList.get(m * i)));
            }
        }
        workerResult.setPointList(pointList);
        return workerResult;
    }

    private double[] toDouble(TaggedRecord record) {
        Double[] data = record.getRecord();
        double[] newData = new double[data.length];
        int i = 0;
        for(Double d: data) {
            newData[i] = d == null ? 0d : d;
        }
        return newData;
    }

    private KMeansWorkerParams doOtherIterations(WorkerContext<KMeansMasterParams, KMeansWorkerParams> workerContext) {
        // new centers used in this iteration.
        List<double[]> centers = workerContext.getLastMasterResult().getPointList();
        LOG.debug("Initial centers:%s", (centers));

        // sum list and count list as worker result sent to master for global accumulation.
        List<double[]> sumList = new LinkedList<double[]>();
        List<Integer> countList = new LinkedList<Integer>();

        // Initializing sum list and count list.
        for(int i = 0; i < this.k; i++) {
            sumList.add(new double[this.c]);
            countList.add(0);
        }

        for(TaggedRecord record: this.dataList) {
            int index = findClosedCenter(record.getRecord(), centers);
            record.setTag(index);
            countList.set(index, countList.get(index) + 1);
            double[] sum = sumList.get(index);
            for(int i = 0; i < this.c; i++) {
                sum[i] += record.getRecord()[i] == null ? 0d : record.getRecord()[i].doubleValue();
            }
        }

        LOG.debug("sumList:%s", (sumList));
        LOG.debug("countList:%s", countList);

        KMeansWorkerParams workerResult = new KMeansWorkerParams();
        workerResult.setK(this.k);
        workerResult.setC(this.c);
        workerResult.setFirstIteration(false);
        workerResult.setPointList(sumList);
        workerResult.setCountList(countList);
        return workerResult;
    }

    /**
     * Finding closed center from all the k centers. Return the index of finding center.
     */
    private int findClosedCenter(Double[] record, List<double[]> centers) {
        int index = 0;
        double minDist = distance(record, centers.get(0));
        for(int i = 1; i < centers.size(); i++) {
            double distance = distance(record, centers.get(i));
            if(distance < minDist) {
                index = i;
            }
        }
        return index;
    }

    /**
     * Calculate cosine distance for two points.
     */
    // TODO cache sqW2, no need re-computing
    private double distance(Double[] record, double[] center) {
        double denominator = 0;
        for(int i = 0; i < center.length; i++) {
            denominator += record[i] == null ? 0d : (record[i] * center[i]);
        }

        double sqW1 = 0, sqW2 = 0;
        for(int i = 0; i < center.length; i++) {
            sqW1 += record[i] == null ? 0d : (record[i] * record[i]);
            sqW2 += (center[i] * center[i]);
        }

        return denominator / (Math.sqrt(sqW1) * Math.sqrt(sqW2));
    }

    /**
     * Loading data into memory. any invalid data will be set to null.
     */
    @Override
    public void load(GuaguaWritableAdapter<LongWritable> currentKey, GuaguaWritableAdapter<Text> currentValue,
            WorkerContext<KMeansMasterParams, KMeansWorkerParams> workerContext) {
        String line = currentValue.getWritable().toString();
        Double[] record = new Double[this.c];
        int i = 0;
        for(String input: Splitter.on(this.separator).split(line)) {
            try {
                record[i++] = Double.parseDouble(input);
            } catch (NumberFormatException e) {
                // use null to replace in-valid number
                record[i++] = null;
            }
        }
        this.dataList.add(new TaggedRecord(record));
    }

}
