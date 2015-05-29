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
package ml.shifu.guagua;

import java.util.List;
import java.util.Properties;

import ml.shifu.guagua.io.GuaguaFileSplit;
import ml.shifu.guagua.util.Progressable;

/**
 * {@link GuaguaService} is a common service interface of both master and worker service implementations.
 * 
 * <p>
 * {@link GuaguaService} defines the process to launch a master or worker service in container. {@link GuaguaService}
 * should be called like code in below:
 * 
 * <pre>
 * {@code 
 *      try {
 *          guaguaService.init(props); 
 *          guaguaService.start(); 
 *          guaguaService.run(null); 
 *      } finally { 
 *          guaguaService.stop(); 
 *      } 
 * }
 * </pre>
 * 
 * <p>
 * {@link #init(Properties)} is used to transfer all properties needed in one guagua application. For Hadoop MapReduce
 * or YARN job, it includes all the properties copied from Hadoop Configuration instance. And these properties will be
 * set to MasterContext and WorkerContext for user to use them.
 */
// TODO Define GuaguaServiceListener: 'onInit(Properties), onStart(), onStop()' to make three hooks for
// GuaguaMasterService and GuaguaWorkerService to scale. GuaguaServiceListener is a list can be configurated by using
// one parameter. And, we should also add setServiceListeners to set listeners on GuaguaService, which will be invoked
// at the end of init, start and stop. Example: Master as a RPC server, workers as RPC client, start and stop them here,
// which can be used to update progress or counter.
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
    void init(Properties props);

    /**
     * Start point for the service. For example, setting up connection with zookeeper server.
     */
    void start();

    /**
     * Real logic implementation, for example, master and worker iteration logic.
     * 
     * <p>
     * If {@code progress} is not null, it will be invoked by once per iteration.
     */
    void run(Progressable progress);

    /**
     * Stop service, resources cleaning should be added in this function.
     * 
     * <em>Warning</em>: this function should be included in finally segment to make sure resources are cleaning well.
     */
    void stop();

    /**
     * Assign splits to each worker to make them load that data.
     */
    void setSplits(List<GuaguaFileSplit> splits);

    /**
     * App id for whole application. For example: Job id in MapReduce(hadoop 1.0) or application id in Yarn.
     */
    void setAppId(String appId);

    /**
     * Set the unique container id for master or worker. For example: Task id in MapReduce or container id in Yarn.
     */
    void setContainerId(String containerId);

}