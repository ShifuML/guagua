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
package ml.shifu.guagua.worker;

import ml.shifu.guagua.io.Bytable;
import ml.shifu.guagua.master.GuaguaMasterService;
import ml.shifu.guagua.master.MasterComputable;
import ml.shifu.guagua.worker.WorkerContext.WorkerCompletionCallBack;

/**
 * Common {@link MasterComputable} abstraction to define {@link #setup(WorkerContext)} and
 * {@link #cleanup(WorkerContext)} hooks.
 * 
 * <p>
 * For classes who extends {@link HookWorkerComputable}, user can provide its own {@link #setup(WorkerContext)} and
 * {@link #cleanup(WorkerContext)}. Guagua will make sure both will be called only once at the beginning and the end of
 * whole process.
 * 
 * @author Zhang David (pengzhang@paypal.com)
 */
public abstract class HookWorkerComputable<MASTER_RESULT extends Bytable, WORKER_RESULT extends Bytable> implements
        WorkerComputable<MASTER_RESULT, WORKER_RESULT> {

    /**
     * Setup method for master computation. This method will be called once before all iterations.
     * 
     * @param context
     *            the worker context instance.
     */
    protected void setup(WorkerContext<MASTER_RESULT, WORKER_RESULT> context) {
    }

    /*
     * (non-Javadoc)
     * 
     * @see ml.shifu.guagua.worker.WorkerComputable#compute(ml.shifu.guagua.worker.WorkerContext)
     */
    @Override
    public WORKER_RESULT compute(WorkerContext<MASTER_RESULT, WORKER_RESULT> context) {
        if(context.isFirstIteration()) {
            this.setup(context);
            context.addCompletionCallBack(new WorkerCompletionCallBack<MASTER_RESULT, WORKER_RESULT>() {
                @Override
                public void callback(WorkerContext<MASTER_RESULT, WORKER_RESULT> context) {
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
    public abstract WORKER_RESULT doCompute(WorkerContext<MASTER_RESULT, WORKER_RESULT> context);

    /**
     * Cleanup method for master computation. This method will be called once after all iterations.
     * 
     * @param context
     *            the worker context instance.
     * @see GuaguaMasterService#run(ml.shifu.guagua.util.Progressable) for cleanup hook
     */
    protected void cleanup(WorkerContext<MASTER_RESULT, WORKER_RESULT> context) {
    }

}
