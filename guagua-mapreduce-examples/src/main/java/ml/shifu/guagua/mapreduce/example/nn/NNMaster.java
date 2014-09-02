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
package ml.shifu.guagua.mapreduce.example.nn;

import java.util.concurrent.atomic.AtomicBoolean;

import ml.shifu.guagua.mapreduce.example.nn.meta.NNParams;
import ml.shifu.guagua.master.MasterComputable;
import ml.shifu.guagua.master.MasterContext;
import ml.shifu.guagua.util.NumberFormatUtils;

import org.encog.neural.networks.BasicNetwork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link NNMaster} is used to accumulate all workers NN parameters.
 * 
 * <p>
 * All gradients are accumulated from workers to calculate model weights, and then new weights are sent to workers. Then
 * workers use new weights to set their models and train for another iteration.
 * 
 * <p>
 * This logic follows Encog multi-core implementation.
 * 
 * <p>
 * To make sure workers and master use the same initialization weights, first iteration of this guagua application is
 * used to compute initial weights which are then sent to works for their initial weights.
 */
public class NNMaster implements MasterComputable<NNParams, NNParams> {

    private static final Logger LOG = LoggerFactory.getLogger(NNMaster.class);

    /**
     * Global master NN parameters instance which is used to update model weights by using accumulated gradients.
     */
    private NNParams globalNNParams = new NNParams();

    /**
     * Whether some configurations are initialized
     */
    private AtomicBoolean isInitialized = new AtomicBoolean(false);

    /**
     * To calculate weights according to last weights and accumulated gradients.
     */
    private Weight weightCalculator = null;

    private double learningRate;

    @Override
    public NNParams compute(MasterContext<NNParams, NNParams> context) {
        // For first step, we not only initialize whole context but also return weights to master to make sure all
        // workers and master are using the same weights.
        if(this.isInitialized.compareAndSet(false, true)) {
            // first iteration is used to set initial weights
            NNParams params = initWeights(context);
            // should be set here to make sure master and workers use the same weights
            this.globalNNParams.setWeights(params.getWeights());

            return params;
        }

        if(context.getWorkerResults() == null) {
            throw new IllegalArgumentException("workers' results are null.");
        }

        double totalTestError = 0;
        double totalTrainError = 0;
        int size = 0;

        // before accumulate, reset gradients and train size
        this.globalNNParams.reset();

        for(NNParams nn: context.getWorkerResults()) {
            totalTestError += nn.getTestError();
            totalTrainError += nn.getTrainError();
            this.globalNNParams.accumulateGradients(nn.getGradients());
            this.globalNNParams.accumulateTrainSize(nn.getTrainSize());
            size++;
        }

        // worker result size is 0. throw exception because shouldn't happen
        if(size == 0) {
            throw new IllegalArgumentException("workers' results are empty.");
        }

        // initialize weightCalCulater.
        if(this.weightCalculator == null) {
            // get the learning rate
            this.weightCalculator = new Weight(this.globalNNParams.getGradients().length,
                    this.globalNNParams.getTrainSize(), this.learningRate, NNConstants.QUICK_PROPAGATION);
        }

        // use last weights and current gradients to calculate
        double[] weights = this.weightCalculator.calculateWeights(this.globalNNParams.getWeights(),
                this.globalNNParams.getGradients());

        this.globalNNParams.setWeights(weights);

        double currentTestError = totalTestError / size;
        double currentTrainError = totalTrainError / size;

        LOG.info("NNMaster compute iteration {} ( avg train error {}, avg validation error {} )", new Object[] {
                context.getCurrentIteration(), currentTrainError, currentTestError });

        NNParams params = new NNParams();
        params.setTrainError(currentTrainError);
        params.setTestError(currentTestError);
        // prevent null point
        params.setGradients(new double[0]);
        params.setWeights(weights);
        LOG.debug("master result {} in iteration {}", params, context.getCurrentIteration());
        return params;
    }

    private NNParams initWeights(MasterContext<NNParams, NNParams> context) {
        int inputs = NumberFormatUtils.getInt(context.getProps().getProperty(NNConstants.GUAGUA_NN_INPUT_NODES),
                NNConstants.GUAGUA_NN_DEFAULT_INPUT_NODES);
        int hiddens = NumberFormatUtils.getInt(context.getProps().getProperty(NNConstants.GUAGUA_NN_HIDDEN_NODES),
                NNConstants.GUAGUA_NN_DEFAULT_HIDDEN_NODES);
        int outputs = NumberFormatUtils.getInt(context.getProps().getProperty(NNConstants.GUAGUA_NN_OUTPUT_NODES),
                NNConstants.GUAGUA_NN_DEFAULT_OUTPUT_NODES);
        this.learningRate = NumberFormatUtils.getDouble(context.getProps().getProperty(
                NNConstants.GUAGUA_NN_LEARNING_RATE, NNConstants.GUAGUA_NN_DEFAULT_LEARNING_RATE));

        BasicNetwork network = NNUtils.generateNetwork(inputs, hiddens, outputs);

        NNParams params = new NNParams();
        params.setTrainError(0);
        params.setTestError(0);
        // prevent null point
        params.setGradients(new double[0]);
        params.setWeights(network.getFlat().getWeights());
        return params;
    }

}