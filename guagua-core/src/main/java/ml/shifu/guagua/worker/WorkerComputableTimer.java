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
package ml.shifu.guagua.worker;

import java.util.concurrent.TimeUnit;

import ml.shifu.guagua.io.Bytable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link WorkerComputableTimer} is used to print execution time for master computation function. Waiting time is not
 * included in this timer.
 * 
 * <p>
 * {@link WorkerComputableTimer} should be set as user interceptor, not system interceptor.
 * 
 * @param <MASTER_RESULT>
 *            master result for computation in each iteration.
 * @param <WORKER_RESULT>
 *            worker result for computation in each iteration.
 */
public class WorkerComputableTimer<MASTER_RESULT extends Bytable, WORKER_RESULT extends Bytable> extends
        BasicWorkerInterceptor<MASTER_RESULT, WORKER_RESULT> {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerComputableTimer.class);

    /**
     * Application starting time.
     */
    private long appStartTime;

    /**
     * Iteration starting time.
     */
    private long iterStartTime;

    @Override
    public void preApplication(WorkerContext<MASTER_RESULT, WORKER_RESULT> context) {
        this.appStartTime = System.nanoTime();
        LOG.info("Application {} container {} computation starts internal worker computation.", context.getAppId(),
                context.getContainerId());
    }

    @Override
    public void preIteration(WorkerContext<MASTER_RESULT, WORKER_RESULT> context) {
        this.iterStartTime = System.nanoTime();
        LOG.info("Application {} container {} iteration {} computation starts internal worker computatio.",
                context.getAppId(), context.getContainerId(), context.getCurrentIteration());
    }

    @Override
    public void postIteration(WorkerContext<MASTER_RESULT, WORKER_RESULT> context) {
        LOG.info("Application {} container {} iteration {} computation ends with {}ms execution time.",
                context.getAppId(), context.getContainerId(), context.getCurrentIteration(),
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - this.iterStartTime));
    }

    @Override
    public void postApplication(WorkerContext<MASTER_RESULT, WORKER_RESULT> context) {
        LOG.info("Application {} container {} computation ends with {}ms execution time.", context.getAppId(),
                context.getContainerId(), TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - this.appStartTime));
    }

}
