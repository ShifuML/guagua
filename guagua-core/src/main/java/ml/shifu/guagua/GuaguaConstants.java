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
package ml.shifu.guagua;

public final class GuaguaConstants {

    // avoid new
    private GuaguaConstants() {
    }

    public static final String GUAGUA_WORKER_NUMBER = "guagua.worker.number";

    public static final String GUAGUA_ITERATION_COUNT = "guagua.iteration.count";

    public static final int GUAGUA_DEFAULT_ITERATION_COUNT = 10;

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

    public static final String GUAGUA_MASTER_DEFAULT_SYSTEM_INTERCEPTERS = "ml.shifu.guagua.master.MasterTimer,ml.shifu.guagua.master.MemoryStatsMasterInterceptor,ml.shifu.guagua.master.SyncMasterCoordinator";

    public static final String GUAGUA_WORKER_DEFAULT_SYSTEM_INTERCEPTERS = "ml.shifu.guagua.worker.WorkerTimer,ml.shifu.guagua.worker.MemoryStatsWorkerInterceptor,ml.shifu.guagua.worker.SyncWorkerCoordinator";

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

    public static final int GUAGUA_DEFAULT_MIN_WORKERS_TIMEOUT = 10 * 60 * 1000;

    public static final String GUAGUA_MIN_WORKERS_TIMEOUT = "guagua.min.workers.timeout";

    public static final String GUAGUA_MIN_WORKERS_RATIO = "guagua.min.workers.ratio";

    /**
     * Zookeeper data limit is 1M, use 1023K for safety.
     */
    public static final int GUAGUA_ZK_DATA_LIMIT = (1024 - 1) * 1024;

    public static final String GUAGUA_ZK_SPLIT_NODE = "split";

    public static final String GUAGUA_ZK_HEARTBEAT_ENABLED = "guagua.zk.heartbeat.enabled";

    public static final String GUAGUA_COMPUTATION_TIME_THRESHOLD = "guagua.computation.time.threshold";

    public static final long GUAGUA_DEFAULT_COMPUTATION_TIME_THRESHOLD = 60 * 1000l;

    public static final String GUAGUA_SITE_FILE = "guagua-site.xml";

    public static final int GUAGUA_FIRST_ITERATION = 1;

}
