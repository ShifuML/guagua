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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import ml.shifu.guagua.io.Bytable;
import ml.shifu.guagua.io.GuaguaFileSplit;
import ml.shifu.guagua.io.GuaguaRecordReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract implementation to load data one by one.
 * 
 * <p>
 * To load data successfully, make sure {@link GuaguaRecordReader} is initialized firstly.
 * 
 * <p>
 * TODO how to add the first iteration to computation.
 * 
 * <p>
 * TODO add multi-thread version to load data.
 * 
 * @param <MASTER_RESULT>
 *            master result for computation in each iteration.
 * @param <WORKER_RESULT>
 *            worker result for computation in each iteration.
 * @param <KEY>
 *            key type for each record
 * @param <VALUE>
 *            value type for each record
 */
public abstract class AbstractWorkerComputable<MASTER_RESULT extends Bytable, WORKER_RESULT extends Bytable, KEY extends Bytable, VALUE extends Bytable>
        implements WorkerComputable<MASTER_RESULT, WORKER_RESULT> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractWorkerComputable.class);

    private AtomicBoolean isLoaded = new AtomicBoolean(false);

    private GuaguaRecordReader<KEY, VALUE> recordReader;

    /*
     * (non-Javadoc)
     * 
     * @see ml.shifu.guagua.worker.WorkerComputable#compute(ml.shifu.guagua.worker.WorkerContext)
     */
    @Override
    public WORKER_RESULT compute(WorkerContext<MASTER_RESULT, WORKER_RESULT> workerContext) throws IOException {
        if(this.isLoaded.compareAndSet(false, true)) {
            init(workerContext);

            long start = System.nanoTime();
            preLoad(workerContext);
            long count = 0;
            for(GuaguaFileSplit fileSplit: workerContext.getFileSplits()) {
                LOG.info("Loading filesplit: {}", fileSplit);
                try {
                    initRecordReader(fileSplit);
                    while(getRecordReader().nextKeyValue()) {
                        load(getRecordReader().getCurrentKey(), getRecordReader().getCurrentValue(), workerContext);
                        ++count;
                    }
                } finally {
                    if(getRecordReader() != null) {
                        getRecordReader().close();
                    }
                }
            }
            postLoad(workerContext);
            LOG.info("Load {} records.", count);
            LOG.info("Data loading time:{}ms", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
        }

        long start = System.nanoTime();
        try {
            return doCompute(workerContext);
        } finally {
            LOG.info("Computation time for application {} container {} iteration {}: {}ms.", workerContext.getAppId(),
                    workerContext.getContainerId(), workerContext.getCurrentIteration(),
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
        }
    }

    /**
     * Do some pre work before loading data.
     */
    protected void preLoad(WorkerContext<MASTER_RESULT, WORKER_RESULT> workerContext) {
    }

    /**
     * Do some post work before loading data.
     */
    protected void postLoad(WorkerContext<MASTER_RESULT, WORKER_RESULT> workerContext) {
    }

    /**
     * Each {@link GuaguaFileSplit} must be initialized before loading data.
     */
    public abstract void initRecordReader(GuaguaFileSplit fileSplit) throws IOException;

    /**
     * Initialization work for the whole computation
     */
    public abstract void init(WorkerContext<MASTER_RESULT, WORKER_RESULT> workerContext);

    /**
     * Real computation logic after data loading.
     */
    public abstract WORKER_RESULT doCompute(WorkerContext<MASTER_RESULT, WORKER_RESULT> workerContext);

    /**
     * Load data one by one before computation.
     */
    public abstract void load(KEY currentKey, VALUE currentValue,
            WorkerContext<MASTER_RESULT, WORKER_RESULT> workerContext);

    public GuaguaRecordReader<KEY, VALUE> getRecordReader() {
        return recordReader;
    }

    public void setRecordReader(GuaguaRecordReader<KEY, VALUE> recordReader) {
        this.recordReader = recordReader;
    }

}
