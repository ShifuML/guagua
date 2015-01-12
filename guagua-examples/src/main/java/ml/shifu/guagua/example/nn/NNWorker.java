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
package ml.shifu.guagua.example.nn;

import java.io.IOException;

import ml.shifu.guagua.GuaguaRuntimeException;
import ml.shifu.guagua.example.nn.meta.NNParams;
import ml.shifu.guagua.hadoop.io.GuaguaLineRecordReader;
import ml.shifu.guagua.hadoop.io.GuaguaWritableAdapter;
import ml.shifu.guagua.io.GuaguaFileSplit;
import ml.shifu.guagua.util.NumberFormatUtils;
import ml.shifu.guagua.worker.AbstractWorkerComputable;
import ml.shifu.guagua.worker.WorkerContext;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.encog.engine.network.activation.ActivationSigmoid;
import org.encog.ml.data.MLDataPair;
import org.encog.ml.data.MLDataSet;
import org.encog.ml.data.basic.BasicMLData;
import org.encog.ml.data.basic.BasicMLDataPair;
import org.encog.ml.data.basic.BasicMLDataSet;
import org.encog.neural.error.LinearErrorFunction;
import org.encog.neural.flat.FlatNetwork;
import org.encog.neural.networks.BasicNetwork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;

/**
 * {@link NNWorker} is used to compute NN model according to splits assigned. The result will be sent to master for
 * accumulation.
 * 
 * <p>
 * Gradients in each worker will be sent to master to update weights of model in worker, which follows Encog's
 * multi-core implementation.
 */
public class NNWorker extends
        AbstractWorkerComputable<NNParams, NNParams, GuaguaWritableAdapter<LongWritable>, GuaguaWritableAdapter<Text>> {

    private static final Logger LOG = LoggerFactory.getLogger(NNWorker.class);

    /**
     * Training data set
     */
    private MLDataSet trainingData = null;

    /**
     * Testing data set
     */
    private MLDataSet testingData = null;

    /**
     * NN algorithm runner instance.
     */
    private Gradient gradient;

    /**
     * input record size, inc one by one.
     */
    private long count;

    private int inputs;

    private int hiddens;

    private int outputs;

    /**
     * Create memory data set object
     */
    private void initMemoryDataSet() {
        this.trainingData = new BasicMLDataSet();
        this.testingData = new BasicMLDataSet();
    }

    @Override
    public void init(WorkerContext<NNParams, NNParams> workerContext) {
        inputs = NumberFormatUtils.getInt(workerContext.getProps().getProperty(NNConstants.GUAGUA_NN_INPUT_NODES),
                NNConstants.GUAGUA_NN_DEFAULT_INPUT_NODES);
        hiddens = NumberFormatUtils.getInt(workerContext.getProps().getProperty(NNConstants.GUAGUA_NN_HIDDEN_NODES),
                NNConstants.GUAGUA_NN_DEFAULT_HIDDEN_NODES);
        outputs = NumberFormatUtils.getInt(workerContext.getProps().getProperty(NNConstants.GUAGUA_NN_OUTPUT_NODES),
                NNConstants.GUAGUA_NN_DEFAULT_OUTPUT_NODES);

        LOG.info("NNWorker is loading data into memory.");
        initMemoryDataSet();
    }

    @Override
    public NNParams doCompute(WorkerContext<NNParams, NNParams> workerContext) {
        // For first iteration, we don't do anything, just wait for master to update weights in next iteration. This
        // make sure all workers in the 1st iteration to get the same weights.
        if(workerContext.getCurrentIteration() == 1) {
            return buildEmptyNNParams(workerContext);
        }

        if(workerContext.getLastMasterResult() == null) {
            // This may not happen since master will set initialization weights firstly.
            LOG.warn("Master result of last iteration is null.");
            return null;
        }
        LOG.debug("Set current model with params {}", workerContext.getLastMasterResult());

        // initialize gradients if null
        if(gradient == null) {
            initGradient(this.trainingData, workerContext.getLastMasterResult().getWeights());
        }

        // using the weights from master to train model in current iteration
        this.gradient.setWeights(workerContext.getLastMasterResult().getWeights());

        this.gradient.run();

        // get train errors and test errors
        double trainError = this.gradient.getError();
        double testError = this.testingData.getRecordCount() > 0 ? (this.gradient.getNetwork()
                .calculateError(this.testingData)) : 0;
        LOG.info("NNWorker compute iteration {} (train error {} validation error {})",
                new Object[] { workerContext.getCurrentIteration(), trainError, testError });

        NNParams params = new NNParams();
        params.setTestError(testError);
        params.setTrainError(trainError);
        params.setGradients(this.gradient.getGradients());
        // prevent null point;
        params.setWeights(new double[0]);
        params.setTrainSize(this.trainingData.getRecordCount());
        return params;
    }

    private void initGradient(MLDataSet training, double[] weights) {
        BasicNetwork network = NNUtils.generateNetwork(this.inputs, this.hiddens, this.outputs);
        // use the weights from master
        network.getFlat().setWeights(weights);

        FlatNetwork flat = network.getFlat();
        // copy Propagation from encog
        double[] flatSpot = new double[flat.getActivationFunctions().length];
        for(int i = 0; i < flat.getActivationFunctions().length; i++) {
            flatSpot[i] = flat.getActivationFunctions()[i] instanceof ActivationSigmoid ? 0.1 : 0.0;
        }

        this.gradient = new Gradient(flat, training.openAdditional(), flatSpot, new LinearErrorFunction());
    }

    private NNParams buildEmptyNNParams(WorkerContext<NNParams, NNParams> workerContext) {
        NNParams params = new NNParams();
        params.setWeights(new double[0]);
        params.setGradients(new double[0]);
        params.setTestError(0.0d);
        params.setTrainError(0.0d);
        return params;
    }

    @Override
    protected void postLoad(WorkerContext<NNParams, NNParams> workerContext) {
        LOG.info("- # Records of the whole data set: {}.", this.count);
        LOG.info("- # Records of the training data set: {}.", this.trainingData.getRecordCount());
        LOG.info("- # Records of the testing data set: {}.", this.testingData.getRecordCount());
    }

    @Override
    public void load(GuaguaWritableAdapter<LongWritable> currentKey, GuaguaWritableAdapter<Text> currentValue,
            WorkerContext<NNParams, NNParams> workerContext) {
        ++this.count;
        if((this.count) % 100000 == 0) {
            LOG.info("Read {} records.", this.count);
        }

        // use guava to iterate only once
        double[] ideal = new double[1];
        int inputNodes = NumberFormatUtils.getInt(
                workerContext.getProps().getProperty(NNConstants.GUAGUA_NN_INPUT_NODES),
                NNConstants.GUAGUA_NN_DEFAULT_INPUT_NODES);
        double[] inputs = new double[inputNodes];

        int i = 0;
        for(String input: Splitter.on(NNConstants.NN_DEFAULT_COLUMN_SEPARATOR).split(
                currentValue.getWritable().toString())) {
            if(i == 0) {
                ideal[i++] = NumberFormatUtils.getDouble(input, 0.0d);
            } else {
                int inputsIndex = (i++) - 1;
                if(inputsIndex >= inputNodes) {
                    break;
                }
                inputs[inputsIndex] = NumberFormatUtils.getDouble(input, 0.0d);
            }
        }

        if(i < (inputNodes + 1)) {
            throw new GuaguaRuntimeException(String.format(
                    "Not enough data columns, input nodes setting:%s, data column:%s", inputNodes, i));
        }

        int scale = NumberFormatUtils.getInt(workerContext.getProps().getProperty(NNConstants.NN_RECORD_SCALE), 1);
        for(int j = 0; j < scale; j++) {
            double[] tmpInputs = j == 0 ? inputs : new double[inputs.length];
            double[] tmpIdeal = j == 0 ? ideal : new double[ideal.length];
            System.arraycopy(inputs, 0, tmpInputs, 0, inputs.length);
            MLDataPair pair = new BasicMLDataPair(new BasicMLData(tmpInputs), new BasicMLData(tmpIdeal));
            double r = Math.random();
            if(r >= 0.5d) {
                this.trainingData.add(pair);
            } else {
                this.testingData.add(pair);
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see ml.shifu.guagua.worker.AbstractWorkerComputable#initRecordReader(ml.shifu.guagua.io.GuaguaFileSplit)
     */
    @Override
    public void initRecordReader(GuaguaFileSplit fileSplit) throws IOException {
        this.setRecordReader(new GuaguaLineRecordReader());
        this.getRecordReader().initialize(fileSplit);
    }

    public MLDataSet getTrainingData() {
        return trainingData;
    }

    public void setTrainingData(MLDataSet trainingData) {
        this.trainingData = trainingData;
    }

    public MLDataSet getTestingData() {
        return testingData;
    }

    public void setTestingData(MLDataSet testingData) {
        this.testingData = testingData;
    }

}
