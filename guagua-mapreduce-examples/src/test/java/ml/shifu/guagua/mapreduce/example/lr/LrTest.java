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
package ml.shifu.guagua.mapreduce.example.lr;

import java.io.IOException;
import java.util.Properties;

import ml.shifu.guagua.GuaguaConstants;
import ml.shifu.guagua.hadoop.GuaguaMRUnitDriver;
import ml.shifu.guagua.unit.GuaguaUnitDriver;

import org.junit.Test;

/**
 * {@link LrTest} is used to test out logistic regression implementation.
 */
public class LrTest {

    @Test
    public void testLrApp() throws IOException {
        Properties props = new Properties();
        props.setProperty(GuaguaConstants.MASTER_COMPUTABLE_CLASS, LogisticRegressionMaster.class.getName());
        props.setProperty(GuaguaConstants.WORKER_COMPUTABLE_CLASS, LogisticRegressionWorker.class.getName());
        props.setProperty(GuaguaConstants.GUAGUA_ITERATION_COUNT, "20");
        props.setProperty(GuaguaConstants.GUAGUA_MASTER_RESULT_CLASS, LogisticRegressionParams.class.getName());
        props.setProperty(GuaguaConstants.GUAGUA_WORKER_RESULT_CLASS, LogisticRegressionParams.class.getName());

        props.setProperty(GuaguaConstants.GUAGUA_INPUT_DIR, getClass().getResource("/lr").toString());

        GuaguaUnitDriver<LogisticRegressionParams, LogisticRegressionParams> driver = new GuaguaMRUnitDriver<LogisticRegressionParams, LogisticRegressionParams>(
                props);

        driver.run();
    }

}
