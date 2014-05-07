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
import ml.shifu.guagua.util.MemoryUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link MemoryStatsWorkerInterceptor} is used to print memory usage for each worker checkpoint.
 * 
 * @param <MASTER_RESULT>
 *            master result for computation in each iteration.
 * @param <WORKER_RESULT>
 *            worker result for computation in each iteration.
 */
public class MemoryStatsWorkerInterceptor<MASTER_RESULT extends Bytable, WORKER_RESULT extends Bytable> extends
        BasicWorkerInterceptor<MASTER_RESULT, WORKER_RESULT> {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryStatsWorkerInterceptor.class);

    @Override
    public void preIteration(WorkerContext<MASTER_RESULT, WORKER_RESULT> context) {
        LOG.info("pre iteration:{} with memory info {}.", context.getCurrentIteration(),
                MemoryUtils.getRuntimeMemoryStats());
    }

    @Override
    public void postIteration(WorkerContext<MASTER_RESULT, WORKER_RESULT> context) {
        LOG.info("post iteration:{} with memory info {}.", context.getCurrentIteration(),
                MemoryUtils.getRuntimeMemoryStats());
    }

    @Override
    public void preApplication(WorkerContext<MASTER_RESULT, WORKER_RESULT> context) {
        LOG.info("pre application with memory info {}.", MemoryUtils.getRuntimeMemoryStats());
    }

    @Override
    public void postApplication(WorkerContext<MASTER_RESULT, WORKER_RESULT> context) {
        LOG.info("post application with memory info {}.", MemoryUtils.getRuntimeMemoryStats());
    }
}
