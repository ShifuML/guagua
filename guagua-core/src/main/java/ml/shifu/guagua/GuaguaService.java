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

import java.util.List;
import java.util.Properties;

import ml.shifu.guagua.io.GuaguaFileSplit;
import ml.shifu.guagua.util.Progressable;

/**
 * {@link GuaguaService} is a common service interface of both master and worker service implementations.
 * 
 * TODO Define GuaguaServiceListener: 'onInit(Properties), onStart(), onStop()' to make three hooks for
 * GuaguaMasterService and GuaguaWorkerService to scale. GuaguaServiceListener is a list can be configurated by using
 * one parameter. And, we should also add setServiceListeners to set listeners on GuaguaService, which will be invoked
 * at the end of init, start and stop. Example: Master as a RPC server, workers as RPC client, start and stop them here,
 * which can be used to update progress or counter.
 */
public interface GuaguaService {

    /**
     * Service initialization. For example, parameters setting.
     * <p>
     * The caller should make sure {@link #init(Properties)} is called before {@link #start()},
     * {@link #run(Progressable)}, {@link #stop()} functions.
     * 
     * @param props
     *            which contains different properties for master and workers to use.
     */
    public abstract void init(Properties props);

    /**
     * Start point for the service. For example, setting up connection with zookeeper server.
     */
    public abstract void start();

    /**
     * Real logic implementation, for example, master and worker iteration logic.
     * 
     * <p>
     * If {@code progress} is not null, it will be invoked by once per iteration.
     */
    public abstract void run(Progressable progress);

    /**
     * Stop service, resources cleaning should be added in this function.
     * 
     * <em>Warning</em>: this function should be included in finally segment to make sure resources are cleaning well.
     */
    public abstract void stop();

    /**
     * Assign splits to each worker to make them load that data.
     */
    public abstract void setSplits(List<GuaguaFileSplit> splits);

    /**
     * App id for whole application. For example: Job id in MapReduce(hadoop 1.0) or application id in Yarn.
     */
    public abstract void setAppId(String appId);

    /**
     * Set the unique container id for master or worker. For example: Task id in MapReduce or container id in Yarn.
     */
    public abstract void setContainerId(String containerId);

}