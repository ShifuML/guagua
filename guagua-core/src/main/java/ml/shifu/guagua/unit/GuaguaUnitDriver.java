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
package ml.shifu.guagua.unit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import ml.shifu.guagua.GuaguaConstants;
import ml.shifu.guagua.GuaguaRuntimeException;
import ml.shifu.guagua.GuaguaService;
import ml.shifu.guagua.MemoryCoordinator;
import ml.shifu.guagua.io.Bytable;
import ml.shifu.guagua.io.GuaguaFileSplit;
import ml.shifu.guagua.master.GuaguaMasterService;
import ml.shifu.guagua.master.InternalMasterCoordinator;
import ml.shifu.guagua.worker.GuaguaWorkerService;
import ml.shifu.guagua.worker.InternalWorkerCoordinator;

/**
 * {@link GuaguaUnitDriver} is a helper class to run master, worker and intercepters in one jvm instance.
 * 
 * <p>
 * One should provide all the properties by using {@link #GuaguaUnitDriver(Properties)}
 * 
 * @param <MASTER_RESULT>
 *            master result for computation in each iteration.
 * @param <WORKER_RESULT>
 *            worker result for computation in each iteration.
 */
public abstract class GuaguaUnitDriver<MASTER_RESULT extends Bytable, WORKER_RESULT extends Bytable> {

    private static final String GUAGUA_UNIT_TEST = "Guagua Unit Test";

    private Properties props;

    private GuaguaService masterService;

    private List<GuaguaService> workerServices;

    private ExecutorService executor;

    private int iteration;

    private List<GuaguaFileSplit[]> fileSplits;

    /**
     * Constructor with {@link #props} setting.
     * 
     * <p>
     * To make it work, please make sure you set parameters in below:
     * 
     * <pre>
     * props.setProperty(GuaguaConstants.MASTER_COMPUTABLE_CLASS, SumMaster.class.getName());
     * props.setProperty(GuaguaConstants.WORKER_COMPUTABLE_CLASS, SumWorker.class.getName());
     * props.setProperty(GuaguaConstants.GUAGUA_ITERATION_COUNT, &quot;10&quot;);
     * props.setProperty(GuaguaConstants.GUAGUA_WORKER_NUMBER, &quot;3&quot;);
     * props.setProperty(GuaguaConstants.GUAGUA_MASTER_RESULT_CLASS, LongWritable.class.getName());
     * props.setProperty(GuaguaConstants.GUAGUA_WORKER_RESULT_CLASS, LongWritable.class.getName());
     * props.setProperty(GuaguaConstants.GUAGUA_WORKER_RESULT_CLASS, LongWritable.class.getName());
     * </pre>
     */
    public GuaguaUnitDriver(Properties props) {
        this.props = props;
    }

    /**
     * Generate file splits according to inputs. It is like hadoop mapper splits.
     */
    public abstract List<GuaguaFileSplit[]> generateWorkerSplits(String inputs) throws IOException;

    @SuppressWarnings("unchecked")
    protected void setUp() {
        try {
            this.fileSplits = generateWorkerSplits(this.props.getProperty(GuaguaConstants.GUAGUA_INPUT_DIR));
        } catch (IOException e) {
            throw new GuaguaRuntimeException(e);
        }
        this.executor = Executors.newFixedThreadPool(this.fileSplits.size() + 1);
        // hard code system interceptors for unit test.
        this.props.setProperty(GuaguaConstants.GUAGUA_MASTER_SYSTEM_INTERCEPTERS,
                InternalMasterCoordinator.class.getName());
        this.props.setProperty(GuaguaConstants.GUAGUA_WORKER_SYSTEM_INTERCEPTERS,
                InternalWorkerCoordinator.class.getName());
        this.props.setProperty(GuaguaConstants.GUAGUA_WORKER_NUMBER, this.fileSplits.size() + "");

        this.iteration = Integer.parseInt(this.props.getProperty(GuaguaConstants.GUAGUA_ITERATION_COUNT));

        this.workerServices = new ArrayList<GuaguaService>();
        this.masterService = new GuaguaMasterService<MASTER_RESULT, WORKER_RESULT>();

        MemoryCoordinator<MASTER_RESULT, WORKER_RESULT> coordinator = new MemoryCoordinator<MASTER_RESULT, WORKER_RESULT>(
                this.fileSplits.size(), this.iteration);
        this.masterService.setAppId(GUAGUA_UNIT_TEST);
        this.masterService.setContainerId("0");
        ((GuaguaMasterService<MASTER_RESULT, WORKER_RESULT>) this.masterService).setCoordinator(coordinator);
        this.masterService.init(this.props);

        for(int i = 0; i < this.fileSplits.size(); i++) {
            GuaguaService workerService = new GuaguaWorkerService<MASTER_RESULT, WORKER_RESULT>();
            workerService.setAppId(GUAGUA_UNIT_TEST);
            workerService.setContainerId((i + 1) + "");
            workerService.setSplits(Arrays.asList(this.fileSplits.get(i)));
            ((GuaguaWorkerService<MASTER_RESULT, WORKER_RESULT>) workerService).setCoordinator(coordinator);
            workerService.init(this.props);
            this.workerServices.add(workerService);
        }
    }

    /**
     * To run master-workers iteration.
     */
    public void run() {
        // setup at firstly
        this.setUp();

        this.doRun();

        // tear down at last
        this.tearDown();
    }

    /**
     * Real running logic
     */
    protected void doRun() {
        this.executor.submit(new Runnable() {
            @Override
            public void run() {
                GuaguaUnitDriver.this.masterService.start();
                GuaguaUnitDriver.this.masterService.run(null);
                GuaguaUnitDriver.this.masterService.stop();
            }
        });

        for(final GuaguaService workerService: this.workerServices) {
            this.executor.submit(new Runnable() {
                @Override
                public void run() {
                    workerService.start();
                    workerService.run(null);
                    workerService.stop();
                }
            });
        }
    }

    protected void tearDown() {
        this.executor.shutdown();
        try {
            this.executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}
