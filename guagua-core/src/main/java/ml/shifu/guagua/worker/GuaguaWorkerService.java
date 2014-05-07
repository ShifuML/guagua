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
package ml.shifu.guagua.worker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import ml.shifu.guagua.BasicCoordinator;
import ml.shifu.guagua.GuaguaConstants;
import ml.shifu.guagua.GuaguaRuntimeException;
import ml.shifu.guagua.GuaguaService;
import ml.shifu.guagua.MemoryCoordinator;
import ml.shifu.guagua.io.Bytable;
import ml.shifu.guagua.io.GuaguaFileSplit;
import ml.shifu.guagua.io.HaltBytable;
import ml.shifu.guagua.io.Serializer;
import ml.shifu.guagua.util.NumberFormatUtils;
import ml.shifu.guagua.util.Progressable;
import ml.shifu.guagua.util.ReflectionUtils;
import ml.shifu.guagua.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link GuaguaWorkerService} is the basic implementation as a worker for whole application process.
 * 
 * @param <MASTER_RESULT>
 *            master computation result in each iteration.
 * @param <WORKER_RESULT>
 *            worker computation result in each iteration.
 */
public class GuaguaWorkerService<MASTER_RESULT extends Bytable, WORKER_RESULT extends Bytable> implements GuaguaService {

    private static final Logger LOG = LoggerFactory.getLogger(GuaguaWorkerService.class);

    /**
     * This props is used to store any key-value pairs for configurations. It will be set from hadoop configuration. The
     * reason we don't want to use Configuration is that we don't want to depend on hadoop for guagua-core.
     */
    private Properties props;

    /**
     * Worker computable class, should be created from class name.
     */
    private WorkerComputable<MASTER_RESULT, WORKER_RESULT> workerComputable;

    /**
     * Total iteration number.
     */
    private int totalIteration;

    /**
     * All intercepters which includes system intercepters like {@link SyncWorkerCoordinator} and customized
     * intercepters.
     */
    private List<WorkerInterceptor<MASTER_RESULT, WORKER_RESULT>> workerInterceptors;

    /**
     * File splits for current worker.
     */
    private List<GuaguaFileSplit> splits;

    /**
     * App id for whole application. For example: Job id in MapReduce(hadoop 1.0) or application id in Yarn.
     */
    private String appId;

    /**
     * The unique container id for master or worker. For example: Task id in MapReduce or container id in Yarn.
     */
    private String containerId;

    /**
     * Class name for master result in each iteration. We must have this field because of reflection need it to create a
     * new instance.
     */
    private String masterResultClassName;

    /**
     * Class name for worker result in each iteration. We must have this field because of reflection need it to create a
     * new instance.
     */
    private String workerResultClassName;

    /**
     * Cache for three lines in start, run and stop to buildContext, to make sure they use the same context object.
     */
    private WorkerContext<MASTER_RESULT, WORKER_RESULT> context;

    /**
     * Which is used in in-memory coordination like unit-test.
     */
    private MemoryCoordinator<MASTER_RESULT, WORKER_RESULT> coordinator;

    /**
     * If 3 times over threshold, kill itself.
     */
    private int overThresholdCount = 0;

    private static final int COUNT_THRESHOLD = 3;

    /**
     * Start services from intercepters, all services started logic should be wrapperd in
     * {@link WorkerInterceptor#preApplication(WorkerContext)};
     */
    @Override
    public void start() {
        WorkerContext<MASTER_RESULT, WORKER_RESULT> context = buildContext();
        context.setCurrentIteration(GuaguaConstants.GUAGUA_INIT_STEP);
        for(WorkerInterceptor<MASTER_RESULT, WORKER_RESULT> workerIntercepter: getWorkerInterceptors()) {
            workerIntercepter.preApplication(context);
        }
    }

    /**
     * Stop services from intercepters which is used for resource cleaning or servers shutting down.
     */
    @Override
    public void stop() {
        WorkerContext<MASTER_RESULT, WORKER_RESULT> context = buildContext();
        // the last iteration is used to stop all workers.
        context.setCurrentIteration(context.getCurrentIteration() + 1);
        // the reverse order with preIteration to make sure a complete wrapper for WorkerComputable.
        int interceptersSize = getWorkerInterceptors().size();
        Throwable exception = null;
        for(int i = 0; i < interceptersSize; i++) {
            try {
                getWorkerInterceptors().get(interceptersSize - 1 - i).postApplication(context);
            } catch (Throwable e) {
                // To make sure all intercepters' post can be invoked.
                LOG.error("Error in worker intercepters cleaning.", e);
                if(exception == null) {
                    exception = e;
                }
            }
        }
        // throw first exception to caller
        if(exception != null) {
            throw new GuaguaRuntimeException(exception);
        }
    }

    /**
     * Iterate {@link WorkerComputable#compute(WorkerContext)}, and set hook point before and after each iteration.
     */
    @Override
    public void run(Progressable progress) {
        WorkerContext<MASTER_RESULT, WORKER_RESULT> context = buildContext();
        int initialIteration = context.getCurrentIteration();
        for(int i = initialIteration; i < getTotalIteration(); i++) {
            int iteration = i + 1;
            context.setCurrentIteration(iteration);
            iterate(context, initialIteration + 1, progress);
            // master says we should stop now.
            MASTER_RESULT masterResult = context.getLastMasterResult();
            if((masterResult instanceof HaltBytable) && ((HaltBytable) masterResult).isHalt()) {
                break;
            }
        }
    }

    /**
     * Call each iteration computation and preIteration, postIteration in intercepters.
     */
    protected WORKER_RESULT iterate(WorkerContext<MASTER_RESULT, WORKER_RESULT> context, int initialIteration,
            Progressable progress) {
        int iteration = context.getCurrentIteration();
        String status = "Start worker iteration ( %s/%s ), progress %s%%";
        if(progress != null) {
            progress.progress(iteration - 1, getTotalIteration(), String.format(status, iteration, getTotalIteration(),
                    ((iteration - 1) * 100 / getTotalIteration())), false, false);
        }

        for(WorkerInterceptor<MASTER_RESULT, WORKER_RESULT> workerIntercepter: getWorkerInterceptors()) {
            workerIntercepter.preIteration(context);
        }

        status = "Start worker computing ( %s/%s ), progress %s%%";
        if(progress != null) {
            progress.progress(iteration - 1, getTotalIteration(), String.format(status, iteration, getTotalIteration(),
                    ((iteration - 1) * 100 / getTotalIteration())), false, false);
        }
        long start = System.nanoTime();
        WORKER_RESULT workerResult;
        boolean isKill = false;
        try {
            workerResult = this.workerComputable.compute(context);
            // Set worker result for
            context.setWorkerResult(workerResult);

            long time = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            long threashold = NumberFormatUtils.getLong(
                    context.getProps().getProperty(GuaguaConstants.GUAGUA_COMPUTATION_TIME_THRESHOLD),
                    GuaguaConstants.GUAGUA_DEFAULT_COMPUTATION_TIME_THRESHOLD);
            // in init iteration, we need load data, so it is ok to use lots of time.
            if(time >= threashold && iteration > initialIteration) {
                this.overThresholdCount++;
                if(this.overThresholdCount >= COUNT_THRESHOLD) {
                    this.overThresholdCount = 0;
                    LOG.warn("Computation time is too long:{}, kill itself to make fail-over work. ", time);
                    isKill = true;
                }
            }
            status = "Complete worker computing ( %s/%s ), progress %s%%";
            if(progress != null) {
                progress.progress(iteration - 1, getTotalIteration(), String.format(status, iteration,
                        getTotalIteration(), ((iteration - 1) * 100 / getTotalIteration())), false, false);
            }
        } catch (IOException e) {
            throw new GuaguaRuntimeException("Error in worker computation", e);
        } catch (Exception e) {
            throw new GuaguaRuntimeException("Error in worker computation", e);
        }
        // the reverse order with preIteration to make sure a complete wrapper for masterComputable.
        int interceptersSize = getWorkerInterceptors().size();
        for(int i = 0; i < interceptersSize; i++) {
            getWorkerInterceptors().get(interceptersSize - 1 - i).postIteration(context);
        }
        // isKill should be set here because of some work on postIteration should be finished.
        status = "Complete worker iteration ( %s/%s ), progress %s%%";
        if(progress != null) {
            progress.progress(iteration, getTotalIteration(),
                    String.format(status, iteration, getTotalIteration(), (iteration * 100 / getTotalIteration())),
                    true, isKill);
        }
        return workerResult;
    }

    /**
     * Should be called before {@link #init(Properties)}
     */
    private WorkerContext<MASTER_RESULT, WORKER_RESULT> buildContext() {
        if(getContext() != null) {
            return getContext();
        }
        this.context = new WorkerContext<MASTER_RESULT, WORKER_RESULT>(getTotalIteration(), getAppId(), getProps(),
                getContainerId(), getSplits(), getMasterResultClassName(), getWorkerResultClassName());
        return getContext();
    }

    /**
     * Parameter initialization, such as set result class name, iteration number and so on.
     */
    @Override
    public void init(Properties props) {
        this.setProps(props);
        checkAndSetWorkerIntercepters(props);

        this.setWorkerComputable(newWorkerComputable());
        this.setTotalIteration(Integer.valueOf(this.getProps().getProperty(GuaguaConstants.GUAGUA_ITERATION_COUNT,
                Integer.MAX_VALUE + "")));
        this.setMasterResultClassName(this.getProps().getProperty(GuaguaConstants.GUAGUA_MASTER_RESULT_CLASS));
        this.setWorkerResultClassName(this.getProps().getProperty(GuaguaConstants.GUAGUA_WORKER_RESULT_CLASS));
    }

    @SuppressWarnings("unchecked")
    private void checkAndSetWorkerIntercepters(Properties props) {
        List<WorkerInterceptor<MASTER_RESULT, WORKER_RESULT>> workerIntercepters = new ArrayList<WorkerInterceptor<MASTER_RESULT, WORKER_RESULT>>();

        String systemWorkerInterceptersStr = StringUtils.get(
                props.getProperty(GuaguaConstants.GUAGUA_WORKER_SYSTEM_INTERCEPTERS),
                GuaguaConstants.GUAGUA_WORKER_DEFAULT_SYSTEM_INTERCEPTERS);
        if(systemWorkerInterceptersStr != null && systemWorkerInterceptersStr.length() != 0) {
            String[] intercepters = systemWorkerInterceptersStr.split(GuaguaConstants.GUAGUA_INTERCEPTER_SEPARATOR);
            if(LOG.isInfoEnabled()) {
                LOG.info("System worker intercepters: {}.", Arrays.toString(intercepters));
            }
            for(String intercepter: intercepters) {
                WorkerInterceptor<MASTER_RESULT, WORKER_RESULT> instance = (WorkerInterceptor<MASTER_RESULT, WORKER_RESULT>) ReflectionUtils
                        .newInstance(intercepter.trim());
                if(instance instanceof BasicCoordinator) {
                    String serialierClassName = StringUtils.get(
                            props.getProperty(GuaguaConstants.GUAGUA_MASTER_IO_SERIALIZER),
                            GuaguaConstants.GUAGUA_IO_DEFAULT_SERIALIZER);
                    Serializer<MASTER_RESULT> masterSerializer = ReflectionUtils.newInstance(serialierClassName);
                    ((BasicCoordinator<MASTER_RESULT, WORKER_RESULT>) instance).setMasterSerializer(masterSerializer);
                    serialierClassName = StringUtils.get(
                            props.getProperty(GuaguaConstants.GUAGUA_WORKER_IO_SERIALIZER),
                            GuaguaConstants.GUAGUA_IO_DEFAULT_SERIALIZER);
                    Serializer<WORKER_RESULT> workerSerializer = ReflectionUtils.newInstance(serialierClassName);
                    ((BasicCoordinator<MASTER_RESULT, WORKER_RESULT>) instance).setWorkerSerializer(workerSerializer);
                } else if(instance instanceof InternalWorkerCoordinator) {
                    ((InternalWorkerCoordinator<MASTER_RESULT, WORKER_RESULT>) instance).setCoordinator(this
                            .getCoordinator());
                }
                workerIntercepters.add(instance);
            }
        }

        String workerInterceptersStr = props.getProperty(GuaguaConstants.GUAGUA_WORKER_INTERCEPTERS);
        if(workerInterceptersStr != null && workerInterceptersStr.length() != 0) {
            String[] intercepters = workerInterceptersStr.split(GuaguaConstants.GUAGUA_INTERCEPTER_SEPARATOR);
            if(LOG.isInfoEnabled()) {
                LOG.info("Customized worker intercepters: {}.", Arrays.toString(intercepters));
            }
            for(String intercepter: intercepters) {
                WorkerInterceptor<MASTER_RESULT, WORKER_RESULT> instance = (WorkerInterceptor<MASTER_RESULT, WORKER_RESULT>) ReflectionUtils
                        .newInstance(intercepter.trim());
                workerIntercepters.add(instance);
            }
        }

        this.setWorkerInterceptors(workerIntercepters);
    }

    @SuppressWarnings("unchecked")
    private WorkerComputable<MASTER_RESULT, WORKER_RESULT> newWorkerComputable() {
        WorkerComputable<MASTER_RESULT, WORKER_RESULT> workerComputable;
        try {
            workerComputable = ((WorkerComputable<MASTER_RESULT, WORKER_RESULT>) ReflectionUtils.newInstance(Class
                    .forName(this.getProps().get(GuaguaConstants.WORKER_COMPUTABLE_CLASS).toString())));
        } catch (ClassNotFoundException e) {
            throw new GuaguaRuntimeException(e);
        }
        return workerComputable;
    }

    private WorkerContext<MASTER_RESULT, WORKER_RESULT> getContext() {
        return context;
    }

    public Properties getProps() {
        return props;
    }

    public void setProps(Properties props) {
        this.props = props;
    }

    public List<GuaguaFileSplit> getSplits() {
        return splits;
    }

    @Override
    public void setSplits(List<GuaguaFileSplit> splits) {
        this.splits = splits;
    }

    public String getAppId() {
        return appId;
    }

    @Override
    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getContainerId() {
        return containerId;
    }

    @Override
    public void setContainerId(String containerId) {
        this.containerId = containerId;
    }

    public WorkerComputable<MASTER_RESULT, WORKER_RESULT> getWorkerComputable() {
        return workerComputable;
    }

    public void setWorkerComputable(WorkerComputable<MASTER_RESULT, WORKER_RESULT> workerComputable) {
        this.workerComputable = workerComputable;
    }

    public int getTotalIteration() {
        return totalIteration;
    }

    public void setTotalIteration(int totalIteration) {
        this.totalIteration = totalIteration;
    }

    public List<WorkerInterceptor<MASTER_RESULT, WORKER_RESULT>> getWorkerInterceptors() {
        return workerInterceptors;
    }

    public void setWorkerInterceptors(List<WorkerInterceptor<MASTER_RESULT, WORKER_RESULT>> workerInterceptors) {
        this.workerInterceptors = workerInterceptors;
    }

    public String getMasterResultClassName() {
        return masterResultClassName;
    }

    public void setMasterResultClassName(String masterResultClassName) {
        this.masterResultClassName = masterResultClassName;
    }

    public String getWorkerResultClassName() {
        return workerResultClassName;
    }

    public void setWorkerResultClassName(String workerResultClassName) {
        this.workerResultClassName = workerResultClassName;
    }

    /**
     * @return the coordinator
     */
    public MemoryCoordinator<MASTER_RESULT, WORKER_RESULT> getCoordinator() {
        return coordinator;
    }

    /**
     * @param coordinator
     *            the coordinator to set
     */
    public void setCoordinator(MemoryCoordinator<MASTER_RESULT, WORKER_RESULT> coordinator) {
        this.coordinator = coordinator;
    }

}
