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

import ml.shifu.guagua.master.GcMasterInterceptor;
import ml.shifu.guagua.master.MasterTimer;
import ml.shifu.guagua.master.MemoryStatsMasterInterceptor;
import ml.shifu.guagua.master.NettyMasterCoordinator;
import ml.shifu.guagua.worker.GcWorkerInterceptor;
import ml.shifu.guagua.worker.MemoryStatsWorkerInterceptor;
import ml.shifu.guagua.worker.NettyWorkerCoordinator;
import ml.shifu.guagua.worker.WorkerTimer;

public final class GuaguaConstants {

    // avoid new
    private GuaguaConstants() {
    }

    public static final String GUAGUA_WORKER_NUMBER = "guagua.worker.number";

    public static final String GUAGUA_ITERATION_COUNT = "guagua.iteration.count";

    public static final int GUAGUA_DEFAULT_ITERATION_COUNT = 50;

    public static final String WORKER_COMPUTABLE_CLASS = "guagua.worker.computable.class";

    public static final String MASTER_COMPUTABLE_CLASS = "guagua.master.computable.class";

    public static final String ZOOKEEPER_SEPARATOR = "/";

    public static final String GUAGUA_ZK_ROOT_NODE = "_guagua";

    public static final String GUAGUA_ZK_MASTER_NODE = "master";

    public static final String GUAGUA_ZK_WORKERS_NODE = "workers";

    public static final String GUAGUA_INIT_DONE_NODE = "0";

    public static final String GUAGUA_MASTER_RESULT_CLASS = "guagua.master.result.class";

    public static final String GUAGUA_WORKER_RESULT_CLASS = "guagua.worker.result.class";

    public static final String GUAGUA_MASTER_INTERCEPTERS = "guagua.master.intercepters";

    public static final String GUAGUA_MASTER_SYSTEM_INTERCEPTERS = "guagua.master.system.intercepters";

    public static final String GUAGUA_WORKER_INTERCEPTERS = "guagua.worker.intercepters";

    public static final String GUAGUA_WORKER_SYSTEM_INTERCEPTERS = "guagua.worker.system.intercepters";

    public static final String GUAGUA_ZK_SERVERS = "guagua.zk.servers";

    public static final String GUAGUA_ZK_SESSION_TIMEOUT = "guagua.zk.session.timeout";

    public static final String GUAGUA_ZK_MAX_ATTEMPTS = "guagua.zk.max.attempt";

    public static final String GUAGUA_ZK_RETRY_WAIT_MILLS = "guagua.zk.retry.wait.mills";

    public static final String GUAGUA = "guagua";

    public static final String GUAGUA_INTERCEPTOR_SEPARATOR = ",";

    public static final String GUAGUA_INPUT_DIR = "guagua.input.dir";

    public static final int GUAGUA_ZK_DEFAULT_RETRY_WAIT_MILLS = 1000;

    public static final int GUAGUA_ZK_DEFAULT_MAX_ATTEMPTS = 5;

    public static final int GUAGUA_ZK_SESSON_DEFAULT_TIMEOUT = 5 * 60 * 1000;

    // using class get name to make sure if class or package changed, such default values will also be changed.
    public static final String GUAGUA_MASTER_DEFAULT_SYSTEM_INTERCEPTERS = MasterTimer.class.getName() + ","
            + GcMasterInterceptor.class.getName() + "," + MemoryStatsMasterInterceptor.class.getName() + ","
            + NettyMasterCoordinator.class.getName();

    public static final String GUAGUA_WORKER_DEFAULT_SYSTEM_INTERCEPTERS = WorkerTimer.class.getName() + ","
            + GcWorkerInterceptor.class.getName() + "," + MemoryStatsWorkerInterceptor.class.getName() + ","
            + NettyWorkerCoordinator.class.getName();

    public static final String GUAGUA_ZK_CLEANUP_ENABLE = "guagua.zk.cleanup.enable";

    public static final String GUAGUA_ZK_DEFAULT_CLEANUP_VALUE = "true";

    public static final String GUAGUA_MASTER_IO_SERIALIZER = "guagua.master.io.serializer";

    public static final String GUAGUA_WORKER_IO_SERIALIZER = "guagua.worker.io.serializer";

    public static final String GUAGUA_IO_DEFAULT_SERIALIZER = "ml.shifu.guagua.io.BytableSerializer";

    public static final String GUAGUA_COORDINATOR_FIXED_SLEEP_ENABLE = "guagua.coordinator.fixed.sleep.enable";

    public static final String GUAGUA_COORDINATOR_SLEEP_UNIT = "guagua.coordinator.sleep.unit";

    public static final String GUAGUA_COORDINATOR_FIXED_SLEEP = "true";

    public static final int DEFAULT_IO_BUFFER_SIZE = 64 * 1024;

    public static final String GUAGUA_WORKER_HALT_ENABLE = "guagua.worker.halt.enable";

    /**
     * @since 0.5.0, change from true to false.
     */
    public static final String GUAGUA_WORKER_DEFAULT_HALT_ENABLE = "false";

    public static final String GUAGUA_SPLIT_MAX_COMBINED_SPLIT_SIZE = "guagua.split.maxCombinedSplitSize";

    public static final String GUAGUA_SPLIT_COMBINABLE = "guagua.split.combinable";

    public static final String GUAGUA_MASTER_NUMBER = "guagua.master.number";

    public static final int DEFAULT_MASTER_NUMBER = 1;

    public static final int GUAGUA_INIT_STEP = 0;

    public static final String GUAGUA_MASTER_ELECTION = "master_election";

    public static final double GUAGUA_DEFAULT_MIN_WORKERS_RATIO = 1.0d;

    public static final String GUAGUA_MIN_WORKERS_TIMEOUT = "guagua.min.workers.timeout";

    public static final String GUAGUA_MIN_WORKERS_RATIO = "guagua.min.workers.ratio";

    /**
     * Zookeeper data limit is 1M, use 1023K for safety.
     */
    public static final int GUAGUA_ZK_DATA_LIMIT = (1024 - 1) * 1024;

    public static final String GUAGUA_ZK_SPLIT_NODE = "split";

    public static final String GUAGUA_ZK_HEARTBEAT_ENABLED = "guagua.zk.heartbeat.enabled";

    public static final String GUAGUA_COMPUTATION_TIME_THRESHOLD = "guagua.computation.time.threshold";

    public static final long GUAGUA_DEFAULT_COMPUTATION_TIME_THRESHOLD = 60 * 1000L;

    public static final long GUAGUA_DEFAULT_MIN_WORKERS_TIMEOUT = GUAGUA_DEFAULT_COMPUTATION_TIME_THRESHOLD;

    public static final String GUAGUA_SITE_FILE = "guagua-site.xml";

    public static final int GUAGUA_FIRST_ITERATION = 1;

    public static final String GUAGUA_STRAGGLER_ITERATORS = "guagua.straggler.iterators";

    public static final int GUAGUA_NETTY_SERVER_DEFAULT_THREAD_COUNT = 8;

    public static final String GUAGUA_NETTY_SEVER_PORT = "guagua.netty.sever.port";

    public static final int GUAGUA_NETTY_SEVER_DEFAULT_PORT = 44323;

    public static final String GUAGUA_MASTER_WORKERESULTS_DEFAULT_MEMORY_FRACTION = "0.7";

    public static final String GUAGUA_MASTER_WORKERESULTS_MEMORY_FRACTION = "guagua.master.workeresults.memoryFraction";

    public static final String GUAGUA_ZK_CLUSTER_SERVER_FOLDER = "guagua.zk.cluster.server.folder";

    public static final String GUAGUA_CLUSTER_ZOOKEEPER_SERVER_FILE = "zookeeper_server";

    public static final String GUAGUA_ZK_EMBEDBED_IS_IN_CLIENT = "guagua.zk.embedbed.isInClient";

    public static final String GUAGUA_CHILD_DEFAULT_ZKSERVER_OPTS = "-Xms512m -Xmx1024m -server -XX:+UseParNewGC "
            + "-XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70";

    public static final String GUAGUA_CHILD_ZKSERVER_OPTS = "guagua.child.zkserver.opts";

    public static final int GUAGUA_DEFAULT_CLEANUP_INTERVAL = 2;

    public static final String GUAGUA_CLEANUP_INTERVAL = "guagua.cleanup.interval";
    
    public static final String GUAGUA_MASTER_RESULT_MERGE_THRESHOLD = "guagua.master.result.merge.threshold";

    public static final String GUAGUA_MASTER_RESULT_NONSPILL = "guagua.master.result.nonspill";
    
    public static final String GUAGUA_UNREGISTER_MASTER_TIMEROUT = "guagua.master.unregister.wait.timeout";
    
    public static final String GUAGUA_ZK_EMBEDED = "guagua.zk.embeded";


}
