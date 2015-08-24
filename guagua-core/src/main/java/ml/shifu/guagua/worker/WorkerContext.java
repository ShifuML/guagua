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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import ml.shifu.guagua.GuaguaConstants;
import ml.shifu.guagua.io.Bytable;
import ml.shifu.guagua.io.GuaguaFileSplit;
import ml.shifu.guagua.io.HaltBytable;

/**
 * {@link WorkerContext} is a context to contain all useful info which can be used in worker computation.
 * 
 * <p>
 * The info includes:
 * <ul>
 * <li>Application ID: Job ID for Hadoop mapreduce Job, application ID for YARN application.</li>
 * <li>Container ID: Task index for Hadoop mapreduce task, Task index for YARN Container.</li>
 * <li>Total iteration number.</li>
 * <li>Current iteration number.</li>
 * <li>Worker result for current iteration after computation which is used to be sent to master.</li>
 * <li>Master result from last iteration.</li>
 * </ul>
 * 
 * @param <MASTER_RESULT>
 *            master result for computation in each iteration.
 * @param <WORKER_RESULT>
 *            worker result for computation in each iteration.
 */
public class WorkerContext<MASTER_RESULT extends Bytable, WORKER_RESULT extends Bytable> {

    /**
     * Current iteration, start from 1. 0 is used for initialization.
     */
    private int currentIteration;

    /**
     * Total iteration number, set by client.
     */
    private final int totalIteration;

    /**
     * master result in last iteration, this should be set in coordination mechanism.
     */
    private MASTER_RESULT lastMasterResult;

    /**
     * After iteration completed, worker result is set to this field. Intercepters in postIteration can read this field.
     */
    private WORKER_RESULT workerResult;

    /**
     * App id for whole application. For example: Job id in MapReduce(hadoop 1.0) or application id in Yarn.
     */
    private final String appId;

    /**
     * This props is used to store any key-value pairs for configurations. It will be set from hadoop configuration. The
     * reason we don't want to use Configuration is that we don't want to depend on hadoop for guagua-core.
     */
    private final Properties props;

    /**
     * Container id in yarn, task attempt id in mapreduce.
     */
    private final String containerId;

    /**
     * Using list to make worker can read more than one FileSplit.
     */
    private final List<GuaguaFileSplit> fileSplits;

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
     * This attachment is for {@link WorkerComputable} and {@link WorkerInterceptor} to transfer object. It can be set
     * by user for running time usage.
     * 
     * @since 0.4.1
     */
    private Object attachment;

    /**
     * Call back list
     * 
     * @since 0.8.1
     */
    private List<WorkerCompletionCallBack<MASTER_RESULT, WORKER_RESULT>> callBackList = new ArrayList<WorkerCompletionCallBack<MASTER_RESULT, WORKER_RESULT>>();

    public WorkerContext(int totalIteration, String appId, Properties props, String containerId,
            List<GuaguaFileSplit> fileSplits, String masterResultClassName, String workerResultClassName) {
        this.totalIteration = totalIteration;
        this.appId = appId;
        this.props = props;
        this.containerId = containerId;
        this.fileSplits = fileSplits;
        this.masterResultClassName = masterResultClassName;
        this.workerResultClassName = workerResultClassName;
    }

    public int getCurrentIteration() {
        return currentIteration;
    }

    public void setCurrentIteration(int currentIteration) {
        this.currentIteration = currentIteration;
    }

    public int getTotalIteration() {
        return totalIteration;
    }

    public MASTER_RESULT getLastMasterResult() {
        return lastMasterResult;
    }

    public void setLastMasterResult(MASTER_RESULT lastMasterResult) {
        this.lastMasterResult = lastMasterResult;
    }

    void setWorkerResult(WORKER_RESULT workerResult) {
        this.workerResult = workerResult;
    }

    public String getAppId() {
        return appId;
    }

    public String getContainerId() {
        return containerId;
    }

    public List<GuaguaFileSplit> getFileSplits() {
        return fileSplits;
    }

    public Properties getProps() {
        return props;
    }

    public WORKER_RESULT getWorkerResult() {
        return workerResult;
    }

    public String getWorkerResultClassName() {
        return workerResultClassName;
    }

    public String getMasterResultClassName() {
        return masterResultClassName;
    }

    public Object getAttachment() {
        return attachment;
    }

    public void setAttachment(Object attachment) {
        this.attachment = attachment;
    }

    /**
     * Whether is in first iteration, default first iteration is 1.
     */
    public boolean isFirstIteration() {
        return getCurrentIteration() == GuaguaConstants.GUAGUA_FIRST_ITERATION;
    }

    /**
     * Whether is in initiate iteration, default initiate iteration is 1.
     */
    public boolean isInitIteration() {
        return getCurrentIteration() == GuaguaConstants.GUAGUA_INIT_STEP;
    }

    /**
     * Add callback function. Be careful not add it in each iteration.
     */
    public void addCompletionCallBack(WorkerCompletionCallBack<MASTER_RESULT, WORKER_RESULT> callback) {
        this.callBackList.add(callback);
    }

    /**
     * @return the callBackList
     */
    public List<WorkerCompletionCallBack<MASTER_RESULT, WORKER_RESULT>> getCallBackList() {
        return callBackList;
    }

    @Override
    public String toString() {
        return String
                .format("WorkerContext [currentIteration=%s, totalIteration=%s, lastMasterResult=%s, workerResult=%s, appId=%s, containerId=%s, fileSplits=%s, masterResultClassName=%s, workerResultClassName=%s]",
                        currentIteration, totalIteration, lastMasterResult, workerResult, appId, containerId,
                        fileSplits, masterResultClassName, workerResultClassName);
    }

    /**
     * Define callback for completion of worker computation.
     * 
     * @author Zhang David (pengzhang@paypal.com)
     */
    public static interface WorkerCompletionCallBack<MASTER_RESULT extends Bytable, WORKER_RESULT extends Bytable> {

        /**
         * Callback function which will be called at the end of last iteration of worker computation. The last iteration
         * is not the last maximal iteration, it may be some iteration at which computation is halt though
         * {@link HaltBytable#isHalt}
         */
        void callback(WorkerContext<MASTER_RESULT, WORKER_RESULT> context);
    }
}
