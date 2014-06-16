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

import java.io.IOException;

import ml.shifu.guagua.io.Bytable;

/**
 * {@link WorkerComputable} defines worker computation for any guagua application. All guagua application should define
 * its own worker logic and to configure it by using guagua client configuration.
 * 
 * <p>
 * Any context parameters like current iteration, master result in last iteration can be got from {@link WorkerContext}.
 * 
 * <p>
 * To get global properties like Hadoop and YARN configuration properties, {@link WorkerContext#getProps()} is a wrapper
 * for Hadoop Configuration. Anything configurated by using '-D' in command line or by internal Hadoop/YARN filling can
 * be got from these properties.
 * 
 * <p>
 * {@link WorkerContext#getLastMasterResult()} is the master result from last iteration. For first iteration (current
 * iteration equals 1), it is null.
 * 
 * <p>
 * Iteration starts from 1, ends with {@link WorkerContext#getTotalIteration()}.Total iteration number can be set in
 * command line through '-c' parameter. It is the same for master and worker total iteration number setting.
 * 
 * <p>
 * {@link WorkerContext#getAppId()} denotes the job ID for a map-reduce job in Hadoop, or YARN application ID for a YARN
 * application. {@link WorkerContext#getContainerId()} denotes current worker container. It is task partition index in
 * Hadoop map-reduce job or split index in YARN guagua application. Container ID is unique for each worker task, if
 * current worker task is restarted after failure, it should use the same container id with the failed worker task to
 * make sure guagua knows who it is.
 * 
 * <p>
 * In worker computation logic, sometimes data loading is needed for only the first iteration. An abstract
 * {@link AbstractWorkerComputable} class which wraps data loading in first iteration (iteration 1) to help load data
 * and do worker computation logic easier.
 * 
 * @param <MASTER_RESULT>
 *            master result for computation in each iteration.
 * @param <WORKER_RESULT>
 *            worker result for computation in each iteration.
 * @see AbstractWorkerComputable
 */
public interface WorkerComputable<MASTER_RESULT extends Bytable, WORKER_RESULT extends Bytable> {

    /**
     * Worker computation for each iteration.
     * 
     * @param context
     *            the worker context instance which includes worker info, master result of last iteration or other
     *            useful into for each iteration.
     * @return
     *         the worker result of each iteration.
     * @throws IOException
     *             any io exception in computation, for example, IOException in reading data.
     */
    WORKER_RESULT compute(WorkerContext<MASTER_RESULT, WORKER_RESULT> context) throws IOException;

}
