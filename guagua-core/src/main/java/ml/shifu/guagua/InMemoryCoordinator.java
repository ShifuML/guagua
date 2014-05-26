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
package ml.shifu.guagua;

import java.util.ArrayList;
import java.util.List;

import ml.shifu.guagua.io.Bytable;

/**
 * {@link InMemoryCoordinator} is helper for in-memory master and worker coordination.
 * 
 * <p>
 * Master result can be set from master by {@link #signalWorkers(int, Bytable)}. Workers can get the result by
 * {@link #getMasterResult()}.
 * 
 * <p>
 * Worker results can be set from worker by {@link #signalWorkers(int, Bytable)}. Master can get the result by
 * {@link #getWorkerResults()}.
 * 
 * @param <MASTER_RESULT>
 *            master computation result in each iteration.
 * @param <WORKER_RESULT>
 *            worker computation result in each iteration.
 */
public class InMemoryCoordinator<MASTER_RESULT extends Bytable, WORKER_RESULT extends Bytable> {

    private static final int DEFAULT_SLEEP_TIME = 300;

    /**
     * Worker number which is used to construct count-down array.
     */
    private final int workers;

    /**
     * Master result which is set from master and transfer to worker.
     */
    private MASTER_RESULT masterResult;

    /**
     * Worker result list which are set from worker and transfer to master.
     */
    private List<WORKER_RESULT> workerResults;

    /**
     * Worker count-down list for all iterations(plus initialization iteration 0)
     */
    private List<Integer> workerCounts;

    /**
     * Master count-down list for all iterations(plus initialization iteration 0)
     */
    private List<Integer> masterCounts;

    public InMemoryCoordinator(int workers, int iteration) {
        this.workers = workers;
        // iteration + (step 0(initialization))
        int maxIteration = iteration + 1;
        this.workerCounts = new ArrayList<Integer>(maxIteration);
        this.masterCounts = new ArrayList<Integer>(maxIteration);
        for(int i = 0; i < maxIteration; i++) {
            // workers is set by parameter
            this.workerCounts.add(this.workers);
            // only one master
            this.masterCounts.add(1);
        }

        this.workerResults = new ArrayList<WORKER_RESULT>(this.workers);
        for(int i = 0; i < this.workers; i++) {
            this.workerResults.add(null);
        }
    }

    /**
     * Master waits for workers on each iteration condition
     */
    public void awaitWorkers(int iteration) {
        int currentCount = 0;
        while(true) {
            synchronized(this.workerCounts) {
                currentCount = this.workerCounts.get(iteration);
            }
            if(currentCount == 0) {
                break;
            }
            try {
                Thread.sleep(DEFAULT_SLEEP_TIME);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Signal master with result setting.
     */
    public void signalMaster(int iteration, int containerIndex, WORKER_RESULT workerResult) {
        synchronized(this.workerCounts) {
            this.workerCounts.set(iteration, this.workerCounts.get(iteration) - 1);
            if(workerResult != null) {
                this.workerResults.set(containerIndex, workerResult);
            }
        }
    }

    /**
     * Workers wait for master on each iteration condition
     */
    public void awaitMaster(int iteration) {
        int currentCount = 0;
        while(true) {
            synchronized(this.masterCounts) {
                currentCount = this.masterCounts.get(iteration);
            }
            if(currentCount == 0) {
                break;
            }
            try {
                Thread.sleep(DEFAULT_SLEEP_TIME);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Master signals workers with result setting.
     */
    public void signalWorkers(int iteration, MASTER_RESULT masterResult) {
        synchronized(this.masterCounts) {
            this.masterCounts.set(iteration, this.masterCounts.get(iteration) - 1);
            this.masterResult = masterResult;
        }
    }

    public MASTER_RESULT getMasterResult() {
        return this.masterResult;
    }

    public List<WORKER_RESULT> getWorkerResults() {
        return new ArrayList<WORKER_RESULT>(this.workerResults);
    }

}
