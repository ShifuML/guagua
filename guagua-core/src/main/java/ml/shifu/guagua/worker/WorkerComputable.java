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
 * Worker computation interface for an application.
 * 
 * <p>
 * The absolute flexibility for customers to use with only {@link #compute(WorkerContext)} function.
 * 
 * <p>
 * If you'd like to load data then computation, you can implement such logic in {@link #compute(WorkerContext)} to leave
 * some template functions. We do it in guagua-mapreduce project.
 * 
 * @param <MASTER_RESULT>
 *            master result for computation in each iteration.
 * @param <WORKER_RESULT>
 *            worker result for computation in each iteration.
 */
public interface WorkerComputable<MASTER_RESULT extends Bytable, WORKER_RESULT extends Bytable> {

    /**
     * Computation for each iteration.
     * 
     * @param context
     *            worker context instance which includes worker info for each iteration.
     * @throws IOException
     *             If any io exception in computation, for example, IOException in reading data.
     */
    WORKER_RESULT compute(WorkerContext<MASTER_RESULT, WORKER_RESULT> context) throws IOException;

}
