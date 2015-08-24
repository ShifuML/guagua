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

import ml.shifu.guagua.io.Bytable;
import ml.shifu.guagua.master.MasterContext.MasterCompletionCallBack;

/**
 * Common {@link MasterComputable} abstraction to define {@link #setup(MasterContext)} and
 * {@link #cleanup(MasterContext)} hooks.
 * 
 * <p>
 * For classes who extends {@link HookMasterComputable}, user can provide its own {@link #setup(MasterContext)} and
 * {@link #cleanup(MasterContext)}. Guagua will make sure both will be called only once at the beginning and the end of
 * whole process.
 * 
 * @author Zhang David (pengzhang@paypal.com)
 */
public abstract class HookMasterComputable<MASTER_RESULT extends Bytable, WORKER_RESULT extends Bytable> implements
        MasterComputable<MASTER_RESULT, WORKER_RESULT> {

    /**
     * Setup method for master computation. This method will be called once before all iterations.
     * 
     * @param context
     *            the worker context instance.
     */
    protected void setup(MasterContext<MASTER_RESULT, WORKER_RESULT> context) {
    }

    /*
     * (non-Javadoc)
     * 
     * @see ml.shifu.guagua.master.MasterComputable#compute(ml.shifu.guagua.master.MasterContext)
     */
    @Override
    public MASTER_RESULT compute(MasterContext<MASTER_RESULT, WORKER_RESULT> context) {
        if(context.isFirstIteration()) {
            this.setup(context);
            context.addCompletionCallBack(new MasterCompletionCallBack<MASTER_RESULT, WORKER_RESULT>() {
                @Override
                public void callback(MasterContext<MASTER_RESULT, WORKER_RESULT> context) {
                    cleanup(context);
                }
            });
        }
        return doCompute(context);
    }

    /**
     * Real computation logic excluding some setup and cleanup hooks.
     * 
     * @param context
     *            the worker context instance.
     * @return the master result in that iteration.
     */
    public abstract MASTER_RESULT doCompute(MasterContext<MASTER_RESULT, WORKER_RESULT> context);

    /**
     * Cleanup method for master computation. This method will be called once after all iterations.
     * 
     * @param context
     *            the worker context instance.
     * @see GuaguaMasterService#run(ml.shifu.guagua.util.Progressable) for cleanup hook
     */
    protected void cleanup(MasterContext<MASTER_RESULT, WORKER_RESULT> context) {
    }

}
