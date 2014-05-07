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

import java.util.Properties;

import ml.shifu.guagua.io.Bytable;

/**
 * {@link MasterContext} is a context to contain all objects used in master computation.
 * 
 * @param <MASTER_RESULT>
 *            master result for computation in each iteration.
 * @param <WORKER_RESULT>
 *            worker result for computation in each iteration.
 */
// TODO make it readonly, the same as WorkerContext 
public class MasterContext<MASTER_RESULT extends Bytable, WORKER_RESULT extends Bytable> {

    /**
     * Worker results for current iteration, should be set in coordination preIteration function.
     */
    private Iterable<WORKER_RESULT> workerResults;

    /**
     * Master result for current iteration, should be sent in coordination postIteration function.
     */
    private MASTER_RESULT masterResult;

    /**
     * Current iteration, start from 1. 0 is used for initialization.
     */
    private int currentIteration;

    /**
     * Total iteration number, set by client.
     */
    private final int totalIteration;

    /**
     * How many workers, set by client, for MapReduce, it is set by getSplits
     */
    private final int workers;

    /**
     * This props is used to store any key-value pairs for configurations. It will be set from hadoop configuration. The
     * reason we don't want to use Configuration is that we don't want to depend on hadoop for guagua-core.
     */
    private final Properties props;

    /**
     * App id for whole application. For example: Job id in MapReduce(hadoop 1.0) or application id in Yarn.
     */
    private final String appId;

    /**
     * Class name for master result in each iteration. We must have this field because of reflection need it to create a
     * new instance.
     */
    private final String masterResultClassName;

    /**
     * Class name for worker result in each iteration. We must have this field because of reflection need it to create a
     * new instance.
     */
    private final String workerResultClassName;

    /**
     * Container id in yarn, task attempt id in mapreduce.
     */
    private final String containerId;

    /**
     * The ratio of minimal workers which are done to determine done of that iteration.
     */
    private final double minWorkersRatio;

    /**
     * After this time elapsed, we can use {@link #minWorkersRatio} to determine done of that iteration.
     */
    private final long minWorkersTimeOut;

    public MasterContext(int totalIteration, int workers, Properties props, String appId, String containerId,
            String masterResultClassName, String workerResultClassName, double minWorkersRatio, long minWorkersTimeOut) {
        this.totalIteration = totalIteration;
        this.workers = workers;
        this.props = props;
        this.appId = appId;
        this.containerId = containerId;
        this.masterResultClassName = masterResultClassName;
        this.workerResultClassName = workerResultClassName;
        this.minWorkersRatio = minWorkersRatio;
        this.minWorkersTimeOut = minWorkersTimeOut;
    }

    public String getContainerId() {
        return containerId;
    }

    public Iterable<WORKER_RESULT> getWorkerResults() {
        return workerResults;
    }

    public void setWorkerResults(Iterable<WORKER_RESULT> workerResults) {
        this.workerResults = workerResults;
    }

    public int getTotalIteration() {
        return totalIteration;
    }

    public int getCurrentIteration() {
        return currentIteration;
    }

    public void setCurrentIteration(int currentIteration) {
        this.currentIteration = currentIteration;
    }

    public int getWorkers() {
        return workers;
    }

    public Properties getProps() {
        return props;
    }

    public String getAppId() {
        return appId;
    }

    public MASTER_RESULT getMasterResult() {
        return masterResult;
    }

    public void setMasterResult(MASTER_RESULT masterResult) {
        this.masterResult = masterResult;
    }

    public String getMasterResultClassName() {
        return masterResultClassName;
    }

    public String getWorkerResultClassName() {
        return workerResultClassName;
    }

    public double getMinWorkersRatio() {
        return minWorkersRatio;
    }

    public long getMinWorkersTimeOut() {
        return minWorkersTimeOut;
    }

    @Override
    public String toString() {
        return String
                .format("MasterContext [workerResults=%s, masterResult=%s, currentIteration=%s, totalIteration=%s, workers=%s, appId=%s, masterResultClassName=%s, workerResultClassName=%s, containerId=%s, minWorkersRatio=%s, minWorkersTimeOut=%s]",
                        workerResults, masterResult, currentIteration, totalIteration, workers, appId,
                        masterResultClassName, workerResultClassName, containerId, minWorkersRatio, minWorkersTimeOut);
    }
}
