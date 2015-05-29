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
package ml.shifu.guagua.mapreduce.example.nn;

import org.encog.Encog;
import org.encog.engine.network.activation.ActivationLinear;
import org.encog.engine.network.activation.ActivationSigmoid;
import org.encog.mathutil.randomize.NguyenWidrowRandomizer;
import org.encog.neural.networks.BasicNetwork;
import org.encog.neural.networks.layers.BasicLayer;

/**
 * Helper class for NN distributed training.
 */
public final class NNUtils {

    private NNUtils() {
    }

    /**
     * Generate basic NN network object
     */
    public static BasicNetwork generateNetwork(int in, int hidden, int out) {
        final BasicNetwork network = new BasicNetwork();
        network.addLayer(new BasicLayer(new ActivationLinear(), true, in));
        network.addLayer(new BasicLayer(new ActivationSigmoid(), true, hidden));
        network.addLayer(new BasicLayer(new ActivationSigmoid(), false, out));

        network.getStructure().finalizeStructure();
        network.reset();
        return network;
    }

    /**
     * Determine the sign of the value.
     * 
     * @param value
     *            The value to check.
     * @return -1 if less than zero, 1 if greater, or 0 if zero.
     */
    public static int sign(final double value) {
        if(Math.abs(value) < Encog.DEFAULT_DOUBLE_EQUAL) {
            return 0;
        } else if(value > 0) {
            return 1;
        } else {
            return -1;
        }
    }

    public static void randomize(int seed, double[] weights) {
        NguyenWidrowRandomizer randomizer = new NguyenWidrowRandomizer(-1, 1);
        randomizer.randomize(weights);
    }

}
