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
package ml.shifu.guagua.worker;

import ml.shifu.guagua.io.Bytable;

/**
 * <p>
 * {@link WorkerInterceptor} is a entry point for all services in worker implementation.
 * 
 * <p>
 * You can add your coordinator mechanism by using one {@link WorkerInterceptor}; you can also save each iteration
 * result by override {@code WorkerIntercepter#postApplication(MasterContext)}.
 * 
 * <p>
 * For a list of intercepters, the order to call preXXX methods and postXXX methods is different. For example, a and b
 * two intercepters. The order is
 * a.preApplication->b.preApplication->a.preIteration->b.preIteration->computation->b.postIteration
 * ->a.postIteration->b.postApplication->a.postApplication. This is like call stack to make sure each intercepter to
 * intercept the whole other intercepters and computations.
 * 
 * @param <MASTER_RESULT>
 *            master result for computation in each iteration.
 * @param <WORKER_RESULT>
 *            worker result for computation in each iteration.
 */
public interface WorkerInterceptor<MASTER_RESULT extends Bytable, WORKER_RESULT extends Bytable> {

    /**
     * The hook point for each application or each mapreduce job which is before all iterations started.
     */
    void preApplication(WorkerContext<MASTER_RESULT, WORKER_RESULT> context);

    /**
     * The hook point for each iteration which is before {@link WorkerComputable#compute(WorkerContext)}.
     */
    void preIteration(WorkerContext<MASTER_RESULT, WORKER_RESULT> context);

    /**
     * The hook point for each iteration which is after {@link WorkerComputable#compute(WorkerContext)}.
     */
    void postIteration(WorkerContext<MASTER_RESULT, WORKER_RESULT> context);

    /**
     * The hook point for each application or each mapreduce job which is after all iterations completed.
     */
    void postApplication(WorkerContext<MASTER_RESULT, WORKER_RESULT> context);

}