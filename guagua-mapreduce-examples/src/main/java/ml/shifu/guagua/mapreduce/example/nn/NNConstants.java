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
package ml.shifu.guagua.mapreduce.example.nn;

/**
 * Constants in guagua mapreduce.
 */
public final class NNConstants {
    // avoid new
    private NNConstants() {
    }

    public static final String GUAGUA_NN_LEARNING_RATE = "guagua.nn.learning.rate";
    public static final String GUAGUA_NN_THREAD_COUNT = "guagua.nn.thread.count";
    public static final String GUAGUA_NN_ALGORITHM = "guagua.nn.algorithm";
    public static final String GUAGUA_NN_OUTPUT_NODES = "guagua.nn.output.nodes";
    public static final String GUAGUA_NN_HIDDEN_NODES = "guagua.nn.hidden.nodes";
    public static final String GUAGUA_NN_INPUT_NODES = "guagua.nn.input.nodes";

    public static final String GUAGUA_NN_DEFAULT_LEARNING_RATE = "0.1";
    public static final int GUAGUA_NN_DEFAULT_THREAD_COUNT = 1;
    public static final String GUAGUA_NN_DEFAULT_ALGORITHM = "Q";
    public static final int GUAGUA_NN_DEFAULT_OUTPUT_NODES = 1;
    public static final int GUAGUA_NN_DEFAULT_HIDDEN_NODES = 2;
    public static final int GUAGUA_NN_DEFAULT_INPUT_NODES = 100;
    public static final String GUAGUA_NN_OUTPUT = "guagua.nn.output";
    public static final String NN_RECORD_SCALE = "nn.record.scale";
    public static final String NN_TEST_SCALE = "nn.test.scale";
    public static final String NN_DEFAULT_COLUMN_SEPARATOR = "|";

    public static final String RESILIENTPROPAGATION = "R";
    public static final String SCALEDCONJUGATEGRADIENT = "S";
    public static final String MANHATTAN_PROPAGATION = "M";
    public static final String QUICK_PROPAGATION = "Q";
    public static final String BACK_PROPAGATION = "B";

    /**
     * The POSITIVE ETA value. This is specified by the resilient propagation
     * algorithm. This is the percentage by which the deltas are increased by if
     * the partial derivative is greater than zero.
     */
    public static final double POSITIVE_ETA = 1.2;

    /**
     * The NEGATIVE ETA value. This is specified by the resilient propagation
     * algorithm. This is the percentage by which the deltas are increased by if
     * the partial derivative is less than zero.
     */
    public static final double NEGATIVE_ETA = 0.5;

    /**
     * The minimum delta value for a weight matrix value.
     */
    public static final double DELTA_MIN = 1e-6;

    /**
     * The starting update for a delta.
     */
    public static final double DEFAULT_INITIAL_UPDATE = 0.1;
}