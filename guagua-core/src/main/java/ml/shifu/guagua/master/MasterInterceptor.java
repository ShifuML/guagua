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
package ml.shifu.guagua.master;

import ml.shifu.guagua.io.Bytable;

/**
 * {@link MasterInterceptor} defines hook points for each guagua application. Four hooks are defined like hooks before
 * and after application and hooks after and before each iteration.
 * 
 * <p>
 * Almost all services in guagua like coordination, fail-over, profiler are implemented as interceptors. These are
 * system interceptors can be configured by using command line '-D' parameter.
 * 
 * <p>
 * After system interceptors, user defined interceptors are also supported for user to define his/her own interceptors.
 * Check <code>SumOutput</code> in examples project to see how interceper is used to save global result at the end of
 * one guagua application.
 * 
 * <p>
 * For a list of interceptors, the order to call preXXX methods and postXXX methods is different. For example, a and b
 * two interceptors. The order is
 * a.preApplication-&gt;b.preApplication-&gt;a.preIteration-&gt;b.preIteration-&gt;computation-&gt;b.postIteration
 * -&gt;a.postIteration-&gt;b.postApplication-&gt;a.postApplication. This is like call stack to make sure each
 * interceptor to intercept the whole other interceptors and master computation.
 * 
 * <p>
 * {@link BasicMasterInterceptor} is a empty implementation for user to choose the hooks to override.
 * 
 * @param <MASTER_RESULT>
 *            master computation result in each iteration.
 * @param <WORKER_RESULT>
 *            worker computation result in each iteration.
 * @see BasicMasterInterceptor
 */
public interface MasterInterceptor<MASTER_RESULT extends Bytable, WORKER_RESULT extends Bytable> {

    /**
     * The hook point before any computation logic.
     * 
     * @param context
     *            the master context instance which includes worker results and other useful parameters.
     */
    void preApplication(MasterContext<MASTER_RESULT, WORKER_RESULT> context);

    /**
     * The hook point before computation of each iteration.
     * 
     * @param context
     *            the master context instance which includes worker results and other useful parameters.
     */
    void preIteration(MasterContext<MASTER_RESULT, WORKER_RESULT> context);

    /**
     * The hook point after computation of each iteration.
     * 
     * @param context
     *            the master context instance which includes worker results and other useful parameters.
     */
    void postIteration(MasterContext<MASTER_RESULT, WORKER_RESULT> context);

    /**
     * The hook point after any computation logic.
     * 
     * @param context
     *            the master context instance which includes worker results and other useful parameters.
     */
    void postApplication(MasterContext<MASTER_RESULT, WORKER_RESULT> context);
}
