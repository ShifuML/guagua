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
package ml.shifu.guagua.master;

import ml.shifu.guagua.io.Bytable;

/**
 * {@link MasterComputable} defines master computation for any guagua application. All guagua application should define
 * its own master logic and to configure it by using guagua client configuration.
 * 
 * <p>
 * Any context parameters like current iteration, worker results can be got from {@link MasterContext}.
 * 
 * <p>
 * To get global properties like Hadoop and YARN configuration properties, {@link MasterContext#getProps()} is a wrapper
 * for Hadoop Configuration. Anything configured by using '-D' in command line can be got from these properties.
 * 
 * <p>
 * {@link MasterContext#getWorkerResults()} is the worker results from all the workers in current iteration. It can be
 * used for master computation logic.
 * 
 * <p>
 * Iteration starts from 1, ends with {@link MasterContext#getTotalIteration()}. Total iteration number can be set in
 * command line through '-c' parameter.
 * 
 * <p>
 * {@link MasterContext#getAppId()} denotes the job ID for a map-reduce job in Hadoop, or YARN application ID for a YARN
 * application. {@link MasterContext#getContainerId()} denotes current master container. It is task partition index in
 * Hadoop map-reduce job or split index in YARN guagua application. Container ID is unique for each master task, if
 * current master task is restarted after failure, it should use the same container id to make sure guagua know who it
 * is.
 * 
 * @param <MASTER_RESULT>
 *            master result for computation in each iteration.
 * @param <WORKER_RESULT>
 *            worker result for computation in each iteration.
 */
public interface MasterComputable<MASTER_RESULT extends Bytable, WORKER_RESULT extends Bytable> {

    /**
     * Master computation for each iteration.
     * 
     * @param context
     *            the master context instance which includes worker results and other useful parameters.
     * @return
     *         the master result of each iteration.
     */
    MASTER_RESULT compute(MasterContext<MASTER_RESULT, WORKER_RESULT> context);

}
