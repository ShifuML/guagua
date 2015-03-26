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

import ml.shifu.guagua.example.nn.meta.NNParams;
import ml.shifu.guagua.master.BasicMasterInterceptor;
import ml.shifu.guagua.master.MasterContext;
import ml.shifu.guagua.util.NumberFormatUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.encog.neural.networks.BasicNetwork;
import org.encog.persist.EncogDirectoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link NNOutput} is used to write the final model output to file system.
 */
public class NNOutput extends BasicMasterInterceptor<NNParams, NNParams> {

    private static final Logger LOG = LoggerFactory.getLogger(NNOutput.class);

    @Override
    public void postApplication(MasterContext<NNParams, NNParams> context) {
        LOG.info("NNOutput starts to write model to files.");
        int inputs = NumberFormatUtils.getInt(context.getProps().getProperty(NNConstants.GUAGUA_NN_INPUT_NODES),
                NNConstants.GUAGUA_NN_DEFAULT_INPUT_NODES);
        int hiddens = NumberFormatUtils.getInt(context.getProps().getProperty(NNConstants.GUAGUA_NN_HIDDEN_NODES),
                NNConstants.GUAGUA_NN_DEFAULT_HIDDEN_NODES);
        int outputs = NumberFormatUtils.getInt(context.getProps().getProperty(NNConstants.GUAGUA_NN_OUTPUT_NODES),
                NNConstants.GUAGUA_NN_DEFAULT_OUTPUT_NODES);
        BasicNetwork network = NNUtils.generateNetwork(inputs, hiddens, outputs);

        Path out = new Path(context.getProps().getProperty(NNConstants.GUAGUA_NN_OUTPUT));
        FSDataOutputStream fos = null;
        try {
            fos = FileSystem.get(new Configuration()).create(out);
            LOG.info("Writing results to {}", out.toString());
            network.getFlat().setWeights(context.getMasterResult().getWeights());
            EncogDirectoryPersistence.saveObject(fos, network);
        } catch (IOException e) {
            LOG.error("Error in writing output.", e);
        } finally {
            IOUtils.closeStream(fos);
        }
    }

}
