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
package ml.shifu.guagua.master;

import ml.shifu.guagua.InMemoryCoordinator;
import ml.shifu.guagua.io.Bytable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link LocalMasterCoordinator} is local coordinator implementation in one jvm instance.
 * 
 * <p>
 * {@link #coordinator} should be set by using the same instance with InternalWorkerCoordinator.
 * 
 * @param <MASTER_RESULT>
 *            master result for computation in each iteration.
 * @param <WORKER_RESULT>
 *            worker result for computation in each iteration.
 */
public class LocalMasterCoordinator<MASTER_RESULT extends Bytable, WORKER_RESULT extends Bytable> extends
        BasicMasterInterceptor<MASTER_RESULT, WORKER_RESULT> {

    private static final Logger LOG = LoggerFactory.getLogger(LocalMasterCoordinator.class);

    private InMemoryCoordinator<MASTER_RESULT, WORKER_RESULT> coordinator;

    @Override
    public void preApplication(MasterContext<MASTER_RESULT, WORKER_RESULT> context) {
            this.coordinator.awaitWorkers(context.getCurrentIteration());
        LOG.info("All workers are initilized.");
        this.coordinator.signalWorkers(context.getCurrentIteration(), null);
    }

    @Override
    public void preIteration(MasterContext<MASTER_RESULT, WORKER_RESULT> context) {
            this.coordinator.awaitWorkers(context.getCurrentIteration());
        context.setWorkerResults(this.coordinator.getWorkerResults());
        LOG.info("All workers are synced in iteration {}.", context.getCurrentIteration());
    }

    @Override
    public void postIteration(MasterContext<MASTER_RESULT, WORKER_RESULT> context) {
        this.coordinator.signalWorkers(context.getCurrentIteration(), context.getMasterResult());
    }

    /**
     * @param coordinator
     *            the coordinator to set
     */
    public void setCoordinator(InMemoryCoordinator<MASTER_RESULT, WORKER_RESULT> coordinator) {
        this.coordinator = coordinator;
    }

}
