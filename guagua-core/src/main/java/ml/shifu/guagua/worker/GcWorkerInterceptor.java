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
package ml.shifu.guagua.worker;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.TimeUnit;

import ml.shifu.guagua.io.Bytable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link GcWorkerInterceptor} is used to log gc time of preXXXX of interceptors, worker computation and postXXXX of
 * interceptors. {@link GcWorkerInterceptor} should be set as system interceptor.
 * 
 * <p>
 * {@link GcWorkerInterceptor} is set as one worker interceptor by default.
 * 
 * @param <MASTER_RESULT>
 *            master computation result in each iteration.
 * @param <WORKER_RESULT>
 *            worker computation result in each iteration.
 */
public class GcWorkerInterceptor<MASTER_RESULT extends Bytable, WORKER_RESULT extends Bytable> implements
        WorkerInterceptor<MASTER_RESULT, WORKER_RESULT> {

    private static final Logger LOG = LoggerFactory.getLogger(GcWorkerInterceptor.class);

    /**
     * Application starting time.
     */
    private long appGCStartTime;

    /**
     * Iteration starting time.
     */
    private long iterGCStartTime;

    @Override
    public void preApplication(WorkerContext<MASTER_RESULT, WORKER_RESULT> context) {
        this.appGCStartTime = computeGCTime();
    }

    @Override
    public void preIteration(WorkerContext<MASTER_RESULT, WORKER_RESULT> context) {
        this.iterGCStartTime = System.nanoTime();
        LOG.info("Application {} container {} iteration {} starts master computation.", context.getAppId(),
                context.getContainerId(), context.getCurrentIteration());
    }

    @Override
    public void postIteration(WorkerContext<MASTER_RESULT, WORKER_RESULT> context) {
        LOG.info("Application {} container {} iteration {} ends with {}ms gc time.", context.getAppId(),
                context.getContainerId(), context.getCurrentIteration(),
                TimeUnit.NANOSECONDS.toMillis(computeGCTime() - this.iterGCStartTime));
    }

    @Override
    public void postApplication(WorkerContext<MASTER_RESULT, WORKER_RESULT> context) {
        LOG.info("Application {} container {} ends with {}ms gc time.", context.getAppId(), context.getContainerId(),
                TimeUnit.NANOSECONDS.toMillis(computeGCTime() - this.appGCStartTime));
    }

    private long computeGCTime() {
        long sum = 0;
        List<GarbageCollectorMXBean> gcMBeans = ManagementFactory.getGarbageCollectorMXBeans();
        for(GarbageCollectorMXBean garbageCollectorMXBean: gcMBeans) {
            sum += garbageCollectorMXBean.getCollectionTime();
        }
        return sum;
    }

}
