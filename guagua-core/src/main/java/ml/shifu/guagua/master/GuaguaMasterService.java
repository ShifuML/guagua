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
package ml.shifu.guagua.master;

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
import ml.shifu.guagua.master.MasterContext.MasterCompletionCallBack;
import ml.shifu.guagua.util.NumberFormatUtils;
import ml.shifu.guagua.util.Progressable;
import ml.shifu.guagua.util.ReflectionUtils;
import ml.shifu.guagua.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link GuaguaMasterService} is the basic implementation as a master for whole guagua application process.
 * 
 * <p>
 * All the properties used to create computable instance, interceptor instances are set in {@link #props}.
 * 
 * <p>
 * After {@link #masterComputable}, {@link #masterInterceptors} are constructed, they will be used in {@link #start()},
 * {@link #run(Progressable)}, {@link #stop()} to do the whole iteration logic.
 * 
 * <p>
 * {@link GuaguaMasterService} is only a skeleton and all implementations are in the interceptors and computable classes
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
public class GuaguaMasterService<MASTER_RESULT extends Bytable, WORKER_RESULT extends Bytable> implements GuaguaService {

    private static final Logger LOG = LoggerFactory.getLogger(GuaguaMasterService.class);

    /**
     * This props is used to store any key-value pairs for configurations. It will be set from hadoop configuration. The
     * reason we don't want to use Configuration is that we don't want to depend on hadoop for guagua-core.
     */
    private Properties props;

    /**
     * All interceptors which includes system interceptors like {@link SyncMasterCoordinator} and customized
     * interceptors.
     */
    private List<MasterInterceptor<MASTER_RESULT, WORKER_RESULT>> masterInterceptors;

    /**
     * Master computable class, should be created from class name.
     */
    private MasterComputable<MASTER_RESULT, WORKER_RESULT> masterComputable;

    /**
     * Total iteration number.
     */
    private int totalIteration;

    /**
     * How many workers, set by client, for MapReduce, it is set by getSplits
     */
    private int workers;

    /**
     * App id for whole application. For example: Job id in MapReduce(hadoop 1.0) or application id in Yarn.
     */
    private String appId;

    /**
     * Container id in yarn, task attempt id in mapreduce.
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
     * The ratio of minimal workers which are done to determine done of that iteration.
     */
    private double minWorkersRatio;

    /**
     * After this time elapsed, we can use {@link #minWorkersRatio} to determine done of that iteration.
     */
    private long minWorkersTimeOut;

    /**
     * Cache for three lines in start, run and stop to buildContext, to make sure they use the same context object.
     */
    private MasterContext<MASTER_RESULT, WORKER_RESULT> context;

    /**
     * Which is used in in-memory coordination like unit-test.
     */
    private InMemoryCoordinator<MASTER_RESULT, WORKER_RESULT> coordinator;

    /**
     * Single thread executor to execute master computation if {@link #masterComputable} is monitored.
     */
    private ExecutorService executor;

    /**
     * If {@link #masterComputable} is monitored by {@link ComputableMonitor}.
     */
    private boolean isMonitored;

    /**
     * Check {@link ComputableMonitor#isSoft()} for details
     */
    @SuppressWarnings("unused")
    private boolean isSoftForComputableTimeout = true;

    /*
     * (non-Javadoc)
     * 
     * @see ml.shifu.guagua.master.GuaguaService#setUp()
     */
    @Override
    public void start() {
        // all services are included in each intecepter.
        MasterContext<MASTER_RESULT, WORKER_RESULT> context = buildContext();
        // iteration 0 is to wait for all workers are available.
        context.setCurrentIteration(GuaguaConstants.GUAGUA_INIT_STEP);
        for(MasterInterceptor<MASTER_RESULT, WORKER_RESULT> masterInterceptor: getMasterInterceptors()) {
            try {
                masterInterceptor.preApplication(context);
            } catch (Throwable e) {
                LOG.error("Error in master interceptors starting.", e);
                throw new GuaguaRuntimeException(e);
            }
        }
    }

    /**
     * Iterate {@link MasterComputable#compute(MasterContext)}, and set hook point before and after each iteration.
     */
    @Override
    public void run(Progressable progress) {
        MasterContext<MASTER_RESULT, WORKER_RESULT> context = buildContext();
        int initialIteration = context.getCurrentIteration();
        try {
            for(int i = initialIteration; i < getTotalIteration(); i++) {
                int iteration = i + 1;
                context.setCurrentIteration(iteration);
                iterate(context, iteration, progress);

                // master says we should stop now.
                MASTER_RESULT masterResult = context.getMasterResult();
                if((masterResult instanceof HaltBytable) && ((HaltBytable) masterResult).isHalt()) {
                    break;
                }
            }
        } finally {
            List<Exception> exceptionList = new ArrayList<Exception>();
            for(MasterCompletionCallBack<MASTER_RESULT, WORKER_RESULT> callBack: context.getCallBackList()) {
                try {
                    callBack.callback(context);
                } catch (Exception e) {
                    // make all call backs run through even exception and throw the first exception at last.
                    LOG.error("Error in master callbacks starting.", e);
                    exceptionList.add(e);
                }
            }
            if(exceptionList.size() > 0) {
                throw new GuaguaRuntimeException(exceptionList.get(0));
            }
        }
    }

    /**
     * Call each iteration computation and preIteration, postIteration in interceptors.
     */
    protected MASTER_RESULT iterate(final MasterContext<MASTER_RESULT, WORKER_RESULT> context, int iteration,
            Progressable progress) {
        String status = "Start master iteration ( %s/%s ), progress %s%%";
        if(progress != null) {
            progress.progress(iteration - 1, getTotalIteration(), String.format(status, iteration, getTotalIteration(),
                    ((iteration - 1) * 100 / getTotalIteration())), false, false);
        }
        for(MasterInterceptor<MASTER_RESULT, WORKER_RESULT> masterInterceptor: getMasterInterceptors()) {
            masterInterceptor.preIteration(context);
        }
        status = "Start master computing ( %s/%s ), progress %s%%";
        if(progress != null) {
            progress.progress(iteration - 1, getTotalIteration(), String.format(status, iteration, getTotalIteration(),
                    ((iteration - 1) * 100 / getTotalIteration())), false, false);
        }

        MASTER_RESULT masterResult = null;
        // if time out in thread
        @SuppressWarnings("unused")
        boolean isTimeOutInThread = false;

        if(this.isMonitored) {
            if(this.executor.isTerminated() || this.executor.isShutdown()) {
                // rebuild this executor if executor is shutdown
                this.executor = Executors.newSingleThreadExecutor();
            }

            ComputableMonitor monitor = masterComputable.getClass().getAnnotation(ComputableMonitor.class);
            TimeUnit unit = monitor.timeUnit();
            long duration = monitor.duration();

            try {
                masterResult = this.executor.submit(new Callable<MASTER_RESULT>() {
                    @Override
                    public MASTER_RESULT call() throws Exception {
                        return masterComputable.compute(context);
                    }
                }).get(duration, unit);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                LOG.error("Error in master computation:", e);
                throw new GuaguaRuntimeException(e);
            } catch (TimeoutException e) {
                isTimeOutInThread = true;
                LOG.warn("Time out for master computation, null will be returned");
                // We should use shutdown to terminate computation in current iteration
                executor.shutdownNow();
                masterResult = null;
            }
        } else {
            masterResult = masterComputable.compute(context);
        }
        context.setMasterResult(masterResult);

        // the reverse order with preIteration to make sure a complete wrapper for masterComputable.
        status = "Complete master computing ( %s/%s ), progress %s%%";
        if(progress != null) {
            progress.progress(iteration - 1, getTotalIteration(), String.format(status, iteration, getTotalIteration(),
                    ((iteration - 1) * 100 / getTotalIteration())), false, false);
        }
        int interceptorsSize = getMasterInterceptors().size();
        for(int i = 0; i < interceptorsSize; i++) {
            getMasterInterceptors().get(interceptorsSize - 1 - i).postIteration(context);
        }
        status = "Complete master iteration ( %s/%s ), progress %s%%";
        if(progress != null) {
            // TODO kill function add to master, look master
            progress.progress(iteration, getTotalIteration(),
                    String.format(status, iteration, getTotalIteration(), (iteration * 100 / getTotalIteration())),
                    true, false);
        }
        return masterResult;
    }

    private MasterContext<MASTER_RESULT, WORKER_RESULT> buildContext() {
        if(getContext() != null) {
            return getContext();
        }
        this.context = new MasterContext<MASTER_RESULT, WORKER_RESULT>(getTotalIteration(), getWorkers(), getProps(),
                getAppId(), getContainerId(), getMasterResultClassName(), getWorkerResultClassName(),
                getMinWorkersRatio(), getMinWorkersTimeOut());
        return getContext();
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

        MasterContext<MASTER_RESULT, WORKER_RESULT> context = buildContext();
        // the last iteration is used to stop all workers.
        context.setCurrentIteration(context.getCurrentIteration() + 1);

        // the reverse order with preIteration to make sure a complete wrapper for masterComputable.
        int interceptorsSize = getMasterInterceptors().size();
        Throwable exception = null;
        for(int i = 0; i < interceptorsSize; i++) {
            try {
                getMasterInterceptors().get(interceptorsSize - 1 - i).postApplication(context);
            } catch (Throwable e) {
                // To make sure all interceptors' post can be invoked.
                LOG.error("Error in master interceptors cleaning.", e);
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
     * Parameter initialization, such as set result class name, iteration number and so on.
     */
    @Override
    public void init(Properties props) {
        this.setProps(props);
        checkAndSetMasterInterceptors(props);
        this.setMasterComputable(newMasterComputable());
        this.isMonitored = this.getMasterComputable().getClass().isAnnotationPresent(ComputableMonitor.class);
        if(this.isMonitored) {
            this.isSoftForComputableTimeout = masterComputable.getClass().getAnnotation(ComputableMonitor.class)
                    .isSoft();
            this.executor = Executors.newSingleThreadExecutor();
        }
        this.setTotalIteration(Integer.valueOf(this.getProps().getProperty(GuaguaConstants.GUAGUA_ITERATION_COUNT,
                Integer.MAX_VALUE + "")));
        this.setWorkers(Integer.valueOf(this.getProps().getProperty(GuaguaConstants.GUAGUA_WORKER_NUMBER)));
        this.setMasterResultClassName(this.getProps().getProperty(GuaguaConstants.GUAGUA_MASTER_RESULT_CLASS));
        this.setWorkerResultClassName(this.getProps().getProperty(GuaguaConstants.GUAGUA_WORKER_RESULT_CLASS));

        double minWorkersRatio = NumberFormatUtils.getDouble(
                this.getProps().getProperty(GuaguaConstants.GUAGUA_MIN_WORKERS_RATIO),
                GuaguaConstants.GUAGUA_DEFAULT_MIN_WORKERS_RATIO);
        this.setMinWorkersRatio(minWorkersRatio);
        long minWorkersTimeOut = NumberFormatUtils.getLong(
                this.getProps().getProperty(GuaguaConstants.GUAGUA_MIN_WORKERS_TIMEOUT), 5 * 1000l);
        this.setMinWorkersTimeOut(minWorkersTimeOut);
    }

    @SuppressWarnings("unchecked")
    private void checkAndSetMasterInterceptors(Properties props) {
        List<MasterInterceptor<MASTER_RESULT, WORKER_RESULT>> masterInterceptors = new ArrayList<MasterInterceptor<MASTER_RESULT, WORKER_RESULT>>();

        String systemMasterInterceptorsStr = StringUtils.get(
                props.getProperty(GuaguaConstants.GUAGUA_MASTER_SYSTEM_INTERCEPTERS),
                GuaguaConstants.GUAGUA_MASTER_DEFAULT_SYSTEM_INTERCEPTERS);
        if(systemMasterInterceptorsStr != null && systemMasterInterceptorsStr.length() != 0) {
            String[] interceptors = systemMasterInterceptorsStr.split(GuaguaConstants.GUAGUA_INTERCEPTOR_SEPARATOR);
            if(LOG.isInfoEnabled()) {
                LOG.info("System master interceptors: {}.", Arrays.toString(interceptors));
            }
            for(String interceptor: interceptors) {
                MasterInterceptor<MASTER_RESULT, WORKER_RESULT> instance = (MasterInterceptor<MASTER_RESULT, WORKER_RESULT>) ReflectionUtils
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
                } else if(instance instanceof LocalMasterCoordinator) {
                    ((LocalMasterCoordinator<MASTER_RESULT, WORKER_RESULT>) instance).setCoordinator(this.coordinator);
                }
                masterInterceptors.add(instance);
            }
        }

        String masterInterceptorsStr = props.getProperty(GuaguaConstants.GUAGUA_MASTER_INTERCEPTERS);
        if(masterInterceptorsStr != null && masterInterceptorsStr.length() != 0) {
            String[] interceptors = masterInterceptorsStr.split(GuaguaConstants.GUAGUA_INTERCEPTOR_SEPARATOR);
            if(LOG.isInfoEnabled()) {
                LOG.info("Customized master interceptors: {}.", Arrays.toString(interceptors));
            }
            for(String interceptor: interceptors) {
                MasterInterceptor<MASTER_RESULT, WORKER_RESULT> instance = (MasterInterceptor<MASTER_RESULT, WORKER_RESULT>) ReflectionUtils
                        .newInstance(interceptor.trim());
                masterInterceptors.add(instance);
            }
        }

        this.setMasterInterceptors(masterInterceptors);
    }

    @SuppressWarnings("unchecked")
    private MasterComputable<MASTER_RESULT, WORKER_RESULT> newMasterComputable() {
        MasterComputable<MASTER_RESULT, WORKER_RESULT> masterComputable;
        try {
            masterComputable = (MasterComputable<MASTER_RESULT, WORKER_RESULT>) ReflectionUtils.newInstance(Class
                    .forName(this.getProps().get(GuaguaConstants.MASTER_COMPUTABLE_CLASS).toString()));
        } catch (ClassNotFoundException e) {
            throw new GuaguaRuntimeException(e);
        }
        return masterComputable;
    }

    /*
     * (non-Javadoc)
     * 
     * @see ml.shifu.guagua.GuaguaService#setSplits(java.util.List)
     */
    @Override
    public void setSplits(List<GuaguaFileSplit> splits) {
        throw new UnsupportedOperationException("Master doesn't need file splits.");
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

    public MasterContext<MASTER_RESULT, WORKER_RESULT> getContext() {
        return context;
    }

    public MasterComputable<MASTER_RESULT, WORKER_RESULT> getMasterComputable() {
        return masterComputable;
    }

    public void setMasterComputable(MasterComputable<MASTER_RESULT, WORKER_RESULT> masterComputable) {
        this.masterComputable = masterComputable;
    }

    public List<MasterInterceptor<MASTER_RESULT, WORKER_RESULT>> getMasterInterceptors() {
        return masterInterceptors;
    }

    public void setMasterInterceptors(List<MasterInterceptor<MASTER_RESULT, WORKER_RESULT>> masterInterceptors) {
        this.masterInterceptors = masterInterceptors;
    }

    public void addMasterInterceptors(MasterInterceptor<MASTER_RESULT, WORKER_RESULT> masterInterceptor) {
        getMasterInterceptors().add(masterInterceptor);
    }

    public int getTotalIteration() {
        return totalIteration;
    }

    public void setTotalIteration(int totalIteration) {
        this.totalIteration = totalIteration;
    }

    public int getWorkers() {
        return workers;
    }

    public void setWorkers(int workers) {
        this.workers = workers;
    }

    public Properties getProps() {
        return props;
    }

    public void setProps(Properties props) {
        this.props = props;
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

    public double getMinWorkersRatio() {
        return minWorkersRatio;
    }

    public long getMinWorkersTimeOut() {
        return minWorkersTimeOut;
    }

    public void setMinWorkersRatio(double minWorkersRatio) {
        this.minWorkersRatio = minWorkersRatio;
    }

    public void setMinWorkersTimeOut(long minWorkersTimeOut) {
        this.minWorkersTimeOut = minWorkersTimeOut;
    }

    public InMemoryCoordinator<MASTER_RESULT, WORKER_RESULT> getCoordinator() {
        return coordinator;
    }

    public void setCoordinator(InMemoryCoordinator<MASTER_RESULT, WORKER_RESULT> coordinator) {
        this.coordinator = coordinator;
    }

}
