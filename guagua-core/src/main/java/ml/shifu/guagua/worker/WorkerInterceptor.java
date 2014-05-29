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
 * {@link WorkerInterceptor} defines hook points for each guagua application. Four hooks are defined like hooks before
 * and after application and hooks after and before each iteration.
 * 
 * <p>
 * Almost all services in guagua like coordination, fail-over, profiler are implemented as intercepters. These are
 * system intercepters can be configured by using command line '-D' parameter.
 * 
 * <p>
 * After system intercepters, user defined intercepters are also supported for user to define his/her own intercepters.
 * Check <code>SumOutput</code> in examples project to see how interceper is used to save global result at the end of
 * one guagua application.
 * 
 * <p>
 * For a list of intercepters, the order to call preXXX methods and postXXX methods is different. For example, a and b
 * two intercepters. The order is
 * a.preApplication-&gt;b.preApplication-&gt;a.preIteration-&gt;b.preIteration-&gt;computation-&gt;b.postIteration
 * -&gt;a.postIteration-&gt;b.postApplication-&gt;a.postApplication. This is like call stack to make sure each
 * intercepter to intercept the whole other intercepters and computations.
 * 
 * <p>
 * {@link BasicWorkerInterceptor} is a empty implementation for user to choose the hooks to override.
 * 
 * @param <MASTER_RESULT>
 *            master result for computation in each iteration.
 * @param <WORKER_RESULT>
 *            worker result for computation in each iteration.
 * @see BasicWorkerInterceptor
 */
public interface WorkerInterceptor<MASTER_RESULT extends Bytable, WORKER_RESULT extends Bytable> {

    /**
     * The hook point before any computation logic.
     * 
     * @param context
     *            the worker context instance which includes master result and other useful parameters.
     */
    void preApplication(WorkerContext<MASTER_RESULT, WORKER_RESULT> context);

    /**
     * The hook point before computation of each iteration.
     * 
     * @param context
     *            the worker context instance which includes master result and other useful parameters.
     */
    void preIteration(WorkerContext<MASTER_RESULT, WORKER_RESULT> context);

    /**
     * The hook point after computation of each iteration.
     * 
     * @param context
     *            the worker context instance which includes master result and other useful parameters.
     */
    void postIteration(WorkerContext<MASTER_RESULT, WORKER_RESULT> context);

    /**
     * The hook point after any computation logic.
     * 
     * @param context
     *            the worker context instance which includes master result and other useful parameters.
     */
    void postApplication(WorkerContext<MASTER_RESULT, WORKER_RESULT> context);

}