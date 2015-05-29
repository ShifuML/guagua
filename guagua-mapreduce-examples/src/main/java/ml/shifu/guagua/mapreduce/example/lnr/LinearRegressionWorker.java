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
package ml.shifu.guagua.mapreduce.example.lnr;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import ml.shifu.guagua.hadoop.io.GuaguaLineRecordReader;
import ml.shifu.guagua.hadoop.io.GuaguaWritableAdapter;
import ml.shifu.guagua.io.GuaguaFileSplit;
import ml.shifu.guagua.util.NumberFormatUtils;
import ml.shifu.guagua.worker.AbstractWorkerComputable;
import ml.shifu.guagua.worker.WorkerContext;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;

/**
 * {@link LinearRegressionWorker} defines logic to accumulate local linear regression gradients.
 * 
 * <p>
 * At first iteration, wait for master to use the consistent initiating model.
 * 
 * <p>
 * At other iterations, workers include:
 * <ul>
 * <li>1. Update local model by using global model from last step..</li>
 * <li>2. Accumulate gradients by using local worker input data.</li>
 * <li>3. Send new local gradients to master by returning parameters.</li>
 * </ul>
 * 
 * <p>
 * <em>WARNING</em>: Input data should be normalized before, or you will get a very bad model.
 */
public class LinearRegressionWorker
        extends
        AbstractWorkerComputable<LinearRegressionParams, LinearRegressionParams, GuaguaWritableAdapter<LongWritable>, GuaguaWritableAdapter<Text>> {

    private static final Logger LOG = LoggerFactory.getLogger(LinearRegressionWorker.class);

    /**
     * Input column number
     */
    private int inputNum;

    /**
     * Output column number
     */
    private int outputNum;

    /**
     * In-memory data which located in memory at the first iteration.
     */
    private List<Data> dataList;

    /**
     * Local linear regression model.
     */
    private double[] weights;

    /**
     * A splitter to split data with specified delimiter.
     */
    private Splitter splitter = Splitter.on(",");

    @Override
    public void initRecordReader(GuaguaFileSplit fileSplit) throws IOException {
        this.setRecordReader(new GuaguaLineRecordReader(fileSplit));
    }

    @Override
    public void init(WorkerContext<LinearRegressionParams, LinearRegressionParams> context) {
        this.inputNum = NumberFormatUtils.getInt(LinearRegressionContants.LR_INPUT_NUM,
                LinearRegressionContants.LR_INPUT_DEFAULT_NUM);
        this.outputNum = 1;
        this.dataList = new LinkedList<Data>();
    }

    @Override
    public LinearRegressionParams doCompute(WorkerContext<LinearRegressionParams, LinearRegressionParams> context) {
        if(context.isFirstIteration()) {
            return new LinearRegressionParams();
        } else {
            this.weights = context.getLastMasterResult().getParameters();
            double[] gradients = new double[this.inputNum + 1];
            double finalError = 0.0d;
            int size = 0;
            for(Data data: dataList) {
                double error = dot(data.inputs, this.weights) - data.outputs[0];
                finalError += error * error / 2;
                for(int i = 0; i < gradients.length; i++) {
                    gradients[i] += error * data.inputs[i];
                }
                size++;
            }
            LOG.info("Iteration {} with error {}", context.getCurrentIteration(), finalError / size);
            return new LinearRegressionParams(gradients, finalError / size);
        }
    }

    /**
     * Compute dot value of two vectors.
     */
    private double dot(double[] inputs, double[] weights) {
        double value = 0.0d;
        for(int i = 0; i < weights.length; i++) {
            value += weights[i] * inputs[i];
        }
        return value;
    }

    @Override
    public void load(GuaguaWritableAdapter<LongWritable> currentKey, GuaguaWritableAdapter<Text> currentValue,
            WorkerContext<LinearRegressionParams, LinearRegressionParams> context) {
        String line = currentValue.getWritable().toString();
        double[] inputData = new double[inputNum + 1];
        double[] outputData = new double[outputNum];
        int count = 0, inputIndex = 0, outputIndex = 0;
        inputData[inputIndex++] = 1.0d;
        for(String unit: splitter.split(line)) {
            if(count < inputNum) {
                inputData[inputIndex++] = Double.valueOf(unit);
            } else if(count >= inputNum && count < (inputNum + outputNum)) {
                outputData[outputIndex++] = Double.valueOf(unit);
            } else {
                break;
            }
            count++;
        }
        this.dataList.add(new Data(inputData, outputData));
    }

    private static class Data {
        public Data(double[] inputs, double[] outputs) {
            this.inputs = inputs;
            this.outputs = outputs;
        }

        private final double[] inputs;
        private final double[] outputs;
    }

}
