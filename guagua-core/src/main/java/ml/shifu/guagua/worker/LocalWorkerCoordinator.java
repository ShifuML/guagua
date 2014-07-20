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

import ml.shifu.guagua.InMemoryCoordinator;
import ml.shifu.guagua.io.Bytable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link LocalWorkerCoordinator} is local coordinator implementation in one jvm instance.
 * 
 * <p>
 * {@link LocalWorkerCoordinator} is a proxy and {@link #coordinator} has the real logic to coordinate master and
 * workers. {@link #coordinator} should be set by using the same instance with InternalWorkerCoordinator.
 * 
 * @param <MASTER_RESULT>
 *            master result for computation in each iteration.
 * @param <WORKER_RESULT>
 *            worker result for computation in each iteration.
 */
public class LocalWorkerCoordinator<MASTER_RESULT extends Bytable, WORKER_RESULT extends Bytable> extends
        BasicWorkerInterceptor<MASTER_RESULT, WORKER_RESULT> {

    private static final Logger LOG = LoggerFactory.getLogger(LocalWorkerCoordinator.class);

    /**
     * Real in memory coordinator implementation.
     */
    private InMemoryCoordinator<MASTER_RESULT, WORKER_RESULT> coordinator;

    @Override
    public void preApplication(WorkerContext<MASTER_RESULT, WORKER_RESULT> context) {
        this.coordinator.signalMaster(context.getCurrentIteration(), Integer.parseInt(context.getContainerId()) - 1,
                null);
        LOG.info("Worker {} is initilized.", context.getContainerId());
        this.coordinator.awaitMaster(context.getCurrentIteration());
    }

    @Override
    public void preIteration(WorkerContext<MASTER_RESULT, WORKER_RESULT> context) {
        context.setLastMasterResult(this.coordinator.getMasterResult());
        LOG.info("Worker {} is started in iteration {}.", context.getContainerId(), context.getCurrentIteration());
    }

    @Override
    public void postIteration(WorkerContext<MASTER_RESULT, WORKER_RESULT> context) {
        this.coordinator.signalMaster(context.getCurrentIteration(), Integer.parseInt(context.getContainerId()) - 1,
                context.getWorkerResult());
        this.coordinator.awaitMaster(context.getCurrentIteration());
        LOG.info("Worker {} is done in iteration {}.", context.getContainerId(), context.getCurrentIteration());
    }

    public void setCoordinator(InMemoryCoordinator<MASTER_RESULT, WORKER_RESULT> coordinator) {
        this.coordinator = coordinator;
    }

}
