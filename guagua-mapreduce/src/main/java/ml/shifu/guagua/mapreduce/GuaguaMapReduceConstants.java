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
package ml.shifu.guagua.mapreduce;

/**
 * Constants in guagua mapreduce.
 */
public class GuaguaMapReduceConstants {

    public static final String MAPRED_TASK_PARTITION = "mapred.task.partition";

    public static final String MAPRED_JOB_ID = "mapred.job.id";

    public static final String MAPRED_TASK_ID = "mapred.task.id";

    public static final String IO_SORT_MB = "io.sort.mb";

    public static final String MAPRED_TASK_TIMEOUT = "mapred.task.timeout";

    public static final String MAPRED_MAP_MAX_ATTEMPTS = "mapred.map.max.attempts";

    public static final String MAPRED_REDUCE_TASKS_SPECULATIVE_EXECUTION = "mapred.reduce.tasks.speculative.execution";

    public static final String MAPRED_MAP_TASKS_SPECULATIVE_EXECUTION = "mapred.map.tasks.speculative.execution";

    public static final String PIG_SCHEMA = "pig_schema";

    public static final String PIG_HEADER = "pig_header";

    public static final String HADOOP_SUCCESS = "_SUCCESS";

    public static final String BZ2 = "bz2";

    public static final String MAPRED_MAX_SPLIT_SIZE = "mapred.max.split.size";

    public static final String MAPRED_MIN_SPLIT_SIZE = "mapred.min.split.size";

    public static final double SPLIT_SLOP = 1.1; // 10% slop

    public static final String NUM_INPUT_FILES = "mapreduce.input.num.files";

    public static final String MAPRED_INPUT_DIR = "mapred.input.dir";

    public static final String MAPRED_JOB_REDUCE_MEMORY_MB = "mapred.job.reduce.memory.mb";

    public static final String MAPREDUCE_JOB_COUNTERS_LIMIT = "mapreduce.job.counters.limit";

    public static final String MAPRED_CHILD_JAVA_OPTS = "mapred.child.java.opts";

    public static final String MAPRED_DEFAULT_CHILD_JAVA_OPTS = "-server -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70";

    public static final String GUAGUA_PROGRESS_COUNTER_GROUP_NAME = "guagua";

    public static final String GUAGUA_PROGRESS_COUNTER_NAME = "progress";

}
