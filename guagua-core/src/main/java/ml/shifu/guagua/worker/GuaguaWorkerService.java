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
package ml.shifu.guagua.worker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import ml.shifu.guagua.BasicCoordinator;
import ml.shifu.guagua.ComputableMonitor;
import ml.shifu.guagua.GuaguaConstants;
import ml.shifu.guagua.GuaguaRuntimeException;
import ml.shifu.guagua.GuaguaService;
import ml.shifu.guagua.InMemoryCoordinator;
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
 * <p>
 * All the properties used to create computable instance, interceptor instances are set in {@link #props}.
 * 
 * <p>
 * After {@link #workerComputable}, {@link #workerInterceptors} are constructed, they will be used in {@link #start()},
 * {@link #run(Progressable)}, {@link #stop()} to do the whole iteration logic.
 * 
 * <p>
 * {@link GuaguaWorkerService} is only a skeleton and all implementations are in the interceptors and computable classes
 * defined by user.
 * 
 * <p>
 * The execution order of preXXXX is different with postXXXX. For preXXX, the order is FIFO, but for postXXX, the order
 * is FILO.
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
     * All interceptors which includes system interceptors like {@link SyncWorkerCoordinator} and customized
     * interceptors.
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
     * Cache for worker context in start, run and stop to buildContext, to make sure they use the same context object.
     */
    private WorkerContext<MASTER_RESULT, WORKER_RESULT> context;

    /**
     * Which is used in in-memory coordination like unit-test.
     */
    private InMemoryCoordinator<MASTER_RESULT, WORKER_RESULT> coordinator;

    /**
     * If 3 times over threshold, kill itself.
     */
    private int overThresholdCount = 0;

    private static final int COUNT_THRESHOLD = 3;

    private int countThresholdDefined = COUNT_THRESHOLD;

    /**
     * Single thread executor to execute master computation if {@link #masterComputable} is monitored.
     */
    private ExecutorService executor;

    /**
     * If {@link #masterComputable} is monitored by {@link ComputableMonitor}.
     */
    private boolean isMonitored;

    /**
     * Start services from interceptors, all services started logic should be wrapperd in
     * {@link WorkerInterceptor#preApplication(WorkerContext)};
     */
    @Override
    public void start() {
        WorkerContext<MASTER_RESULT, WORKER_RESULT> context = buildContext();
        context.setCurrentIteration(GuaguaConstants.GUAGUA_INIT_STEP);
        for(WorkerInterceptor<MASTER_RESULT, WORKER_RESULT> workerInterceptor: getWorkerInterceptors()) {
            workerInterceptor.preApplication(context);
        }
    }

    /**
     * Stop services from interceptors which is used for resource cleaning or servers shutting down.
     */
    @Override
    public void stop() {
        // shut down executor firstly
        if(this.isMonitored && this.executor != null) {
            this.executor.shutdown();
        }

        WorkerContext<MASTER_RESULT, WORKER_RESULT> context = buildContext();
        // the last iteration is used to stop all workers.
        context.setCurrentIteration(context.getCurrentIteration() + 1);
        // the reverse order with preIteration to make sure a complete wrapper for WorkerComputable.
        int interceptorsSize = getWorkerInterceptors().size();
        Throwable exception = null;
        for(int i = 0; i < interceptorsSize; i++) {
            try {
                getWorkerInterceptors().get(interceptorsSize - 1 - i).postApplication(context);
            } catch (Throwable e) {
                // To make sure all interceptors' post can be invoked.
                LOG.error("Error in worker interceptors cleaning.", e);
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

        int firstIteration = context.getCurrentIteration() + 1;
        int iteration = context.getCurrentIteration();
        while(iteration < getTotalIteration()) {
            int currIter = iteration + 1;
            context.setCurrentIteration(currIter);
            iterate(context, firstIteration, progress);
            iteration = context.getCurrentIteration();
            // master says we should stop now.
            MASTER_RESULT masterResult = context.getLastMasterResult();
            if((masterResult instanceof HaltBytable) && ((HaltBytable) masterResult).isHalt()) {
                break;
            }
        }
    }

    /**
     * Call each iteration computation and preIteration, postIteration in interceptors.
     */
    protected WORKER_RESULT iterate(final WorkerContext<MASTER_RESULT, WORKER_RESULT> context, int initialIteration,
            Progressable progress) {
        int iteration = context.getCurrentIteration();
        String status = "Start worker iteration ( %s/%s ), progress %s%%";
        if(progress != null) {
            progress.progress(iteration - 1, getTotalIteration(), String.format(status, iteration, getTotalIteration(),
                    ((iteration - 1) * 100 / getTotalIteration())), false, false);
        }

        for(WorkerInterceptor<MASTER_RESULT, WORKER_RESULT> workerInterceptor: getWorkerInterceptors()) {
            workerInterceptor.preIteration(context);
        }

        status = "Start worker computing ( %s/%s ), progress %s%%";
        if(progress != null) {
            progress.progress(iteration - 1, getTotalIteration(), String.format(status, iteration, getTotalIteration(),
                    ((iteration - 1) * 100 / getTotalIteration())), false, false);
        }
        long start = System.nanoTime();
        WORKER_RESULT workerResult = null;
        boolean isKill = false;
        try {
            if(this.isMonitored) {
                if(this.executor.isTerminated() || this.executor.isShutdown()) {
                    // rebuild this executor if executor is shutdown
                    this.executor = Executors.newSingleThreadExecutor();
                }

                ComputableMonitor monitor = workerComputable.getClass().getAnnotation(ComputableMonitor.class);
                TimeUnit unit = monitor.timeUnit();
                long duration = monitor.duration();

                try {
                    workerResult = executor.submit(new Callable<WORKER_RESULT>() {
                        @Override
                        public WORKER_RESULT call() throws Exception {
                            return workerComputable.compute(context);
                        }
                    }).get(duration, unit);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (ExecutionException e) {
                    LOG.error("Error in master computation:", e);
                } catch (TimeoutException e) {
                    LOG.warn("Time out for master computation, null will be returned");
                    // We should use shutdown to terminate computation in current iteration
                    executor.shutdownNow();
                    workerResult = null;
                }
            } else {
                workerResult = this.workerComputable.compute(context);
            }
            // Set worker result
            context.setWorkerResult(workerResult);

            long time = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            long threashold = NumberFormatUtils.getLong(
                    context.getProps().getProperty(GuaguaConstants.GUAGUA_COMPUTATION_TIME_THRESHOLD),
                    GuaguaConstants.GUAGUA_DEFAULT_COMPUTATION_TIME_THRESHOLD);
            // in init iteration, we need load data, so it is ok to use lots of time.
            if(time >= threashold && iteration > initialIteration) {
                this.overThresholdCount++;
                if(this.overThresholdCount >= this.countThresholdDefined) {
                    this.overThresholdCount = 0;
                    LOG.warn("Computation time is too long:{}, kill itself to make fail-over work.", time);
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
        int interceptorsSize = getWorkerInterceptors().size();
        for(int i = 0; i < interceptorsSize; i++) {
            getWorkerInterceptors().get(interceptorsSize - 1 - i).postIteration(context);
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
        checkAndSetWorkerInterceptors(props);

        this.setWorkerComputable(newWorkerComputable());
        this.isMonitored = this.getWorkerComputable().getClass().isAnnotationPresent(ComputableMonitor.class);
        if(this.isMonitored) {
            this.executor = Executors.newSingleThreadExecutor();
        }

        this.countThresholdDefined = Integer.valueOf(this.getProps().getProperty(
                GuaguaConstants.GUAGUA_STRAGGLER_ITERATORS, COUNT_THRESHOLD + ""));

        this.setTotalIteration(Integer.valueOf(this.getProps().getProperty(GuaguaConstants.GUAGUA_ITERATION_COUNT,
                Integer.MAX_VALUE + "")));
        this.setMasterResultClassName(this.getProps().getProperty(GuaguaConstants.GUAGUA_MASTER_RESULT_CLASS));
        this.setWorkerResultClassName(this.getProps().getProperty(GuaguaConstants.GUAGUA_WORKER_RESULT_CLASS));
    }

    @SuppressWarnings("unchecked")
    private void checkAndSetWorkerInterceptors(Properties props) {
        List<WorkerInterceptor<MASTER_RESULT, WORKER_RESULT>> workerInterceptors = new ArrayList<WorkerInterceptor<MASTER_RESULT, WORKER_RESULT>>();

        String systemWorkerInterceptorsStr = StringUtils.get(
                props.getProperty(GuaguaConstants.GUAGUA_WORKER_SYSTEM_INTERCEPTERS),
                GuaguaConstants.GUAGUA_WORKER_DEFAULT_SYSTEM_INTERCEPTERS);
        if(systemWorkerInterceptorsStr != null && systemWorkerInterceptorsStr.length() != 0) {
            String[] interceptors = systemWorkerInterceptorsStr.split(GuaguaConstants.GUAGUA_INTERCEPTOR_SEPARATOR);
            if(LOG.isInfoEnabled()) {
                LOG.info("System worker interceptors: {}.", Arrays.toString(interceptors));
            }
            for(String interceptor: interceptors) {
                WorkerInterceptor<MASTER_RESULT, WORKER_RESULT> instance = (WorkerInterceptor<MASTER_RESULT, WORKER_RESULT>) ReflectionUtils
                        .newInstance(interceptor.trim());
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
                } else if(instance instanceof LocalWorkerCoordinator) {
                    ((LocalWorkerCoordinator<MASTER_RESULT, WORKER_RESULT>) instance).setCoordinator(this
                            .getCoordinator());
                }
                workerInterceptors.add(instance);
            }
        }

        String workerInterceptorsStr = props.getProperty(GuaguaConstants.GUAGUA_WORKER_INTERCEPTERS);
        if(workerInterceptorsStr != null && workerInterceptorsStr.length() != 0) {
            String[] interceptors = workerInterceptorsStr.split(GuaguaConstants.GUAGUA_INTERCEPTOR_SEPARATOR);
            if(LOG.isInfoEnabled()) {
                LOG.info("Customized worker interceptors: {}.", Arrays.toString(interceptors));
            }
            for(String interceptor: interceptors) {
                WorkerInterceptor<MASTER_RESULT, WORKER_RESULT> instance = (WorkerInterceptor<MASTER_RESULT, WORKER_RESULT>) ReflectionUtils
                        .newInstance(interceptor.trim());
                workerInterceptors.add(instance);
            }
        }

        this.setWorkerInterceptors(workerInterceptors);
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

    public InMemoryCoordinator<MASTER_RESULT, WORKER_RESULT> getCoordinator() {
        return coordinator;
    }

    public void setCoordinator(InMemoryCoordinator<MASTER_RESULT, WORKER_RESULT> coordinator) {
        this.coordinator = coordinator;
    }

}
