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
package ml.shifu.guagua.yarn;

/**
 * Constants in guagua yarn.
 */
public final class GuaguaYarnConstants {

    // avoid new
    private GuaguaYarnConstants() {
    }

    public static final int DEFAULT_IO_BUFFER_SIZE = 64 * 1024;

    public static final String PIG_SCHEMA = "pig_schema";

    public static final String PIG_HEADER = "pig_header";

    public static final String HADOOP_SUCCESS = "_SUCCESS";

    public static final String BZ2 = "bz2";

    public static final int GUAGUA_YARN_DEFAULT_PRIORITY = 0;

    public static final String GUAGUA_YARN_MASTER_PRIORITY = "guagua.yarn.master.priority";

    public static final String GUAGUA_YARN_DEFAULT_QUEUE_NAME = "default";

    public static final String GUAGUA_YARN_QUEUE_NAME = "guagua.yarn.queue.name";

    public static final int GUAGAU_APP_MASTER_DEFAULT_ATTMPTS = 2;

    public static final String GUAGUA_YARN_APP_NAME = "guagua.yarn.app.name";

    public static final String GUAGUA_YARN_INPUT_DIR = "guagua.yarn.input.dir";

    public static final String GUAGUA_YARN_APP_LIB_JAR = "guagua.yarn.app.lib.jar";

    public static final String GUAGUA_YARN_APP_JAR = "guagua.yarn.app.jar";

    public static final String CURRENT_DIR = "./";

    public static final String GUAGUA_YARN_MASTER = "guagua.yarn.master.main";

    public static final String GUAGUA_YARN_MASTER_MEMORY = "guagua.yarn.master.memory";

    public static final int GUAGUA_YARN_DEFAULT_MASTER_MEMORY = 1024;

    public static final String GUAGUA_YARN_MASTER_ARGS = "guagua.yarn.master.args";

    public static final String GUAGUA_YARN_CONTAINER_ARGS = "guagua.yarn.container.args";

    public static final String GUAGUA_LOG4J_PROPERTIES = "log4j.properties";
    public static final String GUAGUA_APP_LIBS_SEPERATOR = ",";

    public static final String GUAGUA_CONF_FILE = "guagua-conf.xml";
    public static final String GUAGUA_HDFS_DIR = "guagua";

    public static final int GUAGUA_CHILD_DEFAULT_MEMORY = 1024;

    public static final String GUAGUA_CHILD_MEMORY = "guagua.child.memory";

    public static final String GUAGUA_YARN_INPUT_SPLIT_PREFIX = "guagua.yarn.input.split.";

    public static final double SPLIT_SLOP = 1.1; // 10% slop

    public static final int GUAGUA_YARN_DEFAULT_MAX_CONTAINER_ATTEMPTS = 4;

    public static final String GUAGUA_YARN_MAX_CONTAINER_ATTEMPTS = "guagua.yarn.max.container.attempts";

    public static final int GUAGUA_YARN_TASK_DEFAULT_VCORES = 1;

    public static final String GUAGUA_YARN_TASK_VCORES = "guagua.yarn.task.vcores";

    public static final int GUAGUA_YARN_MASTER_DEFAULT_VCORES = 1;

    public static final String GUAGUA_YARN_MASTER_VCORES = "guagua.yarn.master.vcores";

    public static final String GUAGUA_YARN_DEFAULT_CONTAINER_JAVA_OPTS = "-server -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70";

    public static final int DEFAULT_STATUS_RPC_PORT = 12345;

    public static final String GUAGUA_YARN_STATUS_RPC_PORT = "guagua.yarn.status.rpc.port";

    public static final long DEFAULT_TIME_OUT = 600 * 1000;

    public static final int DEFAULT_STATUS_RPC_SERVER_THREAD_COUNT = 4;

    public static final String GUAGUA_TASK_TIMEOUT = "guagua.task.timeout";

}
