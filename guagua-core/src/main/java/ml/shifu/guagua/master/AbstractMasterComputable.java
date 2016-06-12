/*
 * Copyright [2013-2015] PayPal Software Foundation
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
package ml.shifu.guagua.master;

import java.util.concurrent.atomic.AtomicBoolean;

import ml.shifu.guagua.io.Bytable;

/**
 * {@link AbstractMasterComputable} is a abstract {@link MasterComputable} implementation to add
 * {@link #init(MasterContext)} for master. Real computation logic is derived into {@link #doCompute(MasterContext)}.
 * 
 * @author Zhang David (pengzhang@paypal.com)
 */
public abstract class AbstractMasterComputable<MASTER_RESULT extends Bytable, WORKER_RESULT extends Bytable> implements
        MasterComputable<MASTER_RESULT, WORKER_RESULT> {

    protected AtomicBoolean isInitialized = new AtomicBoolean(false);

    @Override
    public MASTER_RESULT compute(MasterContext<MASTER_RESULT, WORKER_RESULT> context) {
        if(isInitialized.compareAndSet(false, true)) {
            init(context);
        }
        return doCompute(context);
    }

    /**
     * Initialization logic. This is used to initialize some useful fields like config parameters. And another important
     * feature can be done here: If state in {@link MasterComputable}, for fail-over, the state should also be recoverd.
     * To do that, like this in this method:
     * 
     * <pre>
     * if (!context.isFirstIteration) { // not first iteration means we need recover state
     *     lastMasterResult = context.getMasterResult();
     *     if ( lasterMasterResult != null ) {
     *         this.weights = lastMasterResult.getWeights()
     *     } else {
     *         this.weights = initWeights();
     *     }
     * }
     * </pre>
     */
    public abstract void init(MasterContext<MASTER_RESULT, WORKER_RESULT> context);

    /**
     * Real computable logic excludes init part.
     */
    public abstract MASTER_RESULT doCompute(MasterContext<MASTER_RESULT, WORKER_RESULT> context);

}
