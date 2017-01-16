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
package ml.shifu.guagua.worker;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import ml.shifu.guagua.io.Bytable;
import ml.shifu.guagua.io.GuaguaFileSplit;
import ml.shifu.guagua.io.GuaguaRecordReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A high-effective implementation to load data and do computation. This is different with
 * {@link AbstractWorkerComputable}, only {@link #doCompute(Bytable, Bytable, WorkerContext)} for each record are
 * published to user. But the first iteration to load data is included in computation.
 * 
 * <p>
 * Worker result should be updated in {@link #doCompute(Bytable, Bytable, WorkerContext)}, and which will also be
 * populated to Master when all records are processed in one iteration.
 * 
 * <p>
 * To load data successfully, make sure {@link GuaguaRecordReader} is initialized firstly. in
 * {@link #initRecordReader(GuaguaFileSplit)}:
 * 
 * <pre>
 * this.setRecordReader(new GuaguaSequenceAsTextRecordReader());
 * this.getRecordReader().initialize(fileSplit);
 * </pre>
 * 
 * or directly use other constructors:
 * 
 * <pre>
 * this.setRecordReader(new GuaguaSequenceAsTextRecordReader(fileSplit));
 * </pre>
 * 
 * <p>
 * After data is loaded in the first iteration, one can store the data into collections (meomory or disk) to do later
 * iteration logic. But OOM issue should be taken care by users.
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
public abstract class AbstractCombineWorkerComputable<MASTER_RESULT extends Bytable, WORKER_RESULT extends Bytable, KEY extends Bytable, VALUE extends Bytable>
        implements WorkerComputable<MASTER_RESULT, WORKER_RESULT> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractCombineWorkerComputable.class);

    private AtomicBoolean isLoaded = new AtomicBoolean(false);

    private GuaguaRecordReader<KEY, VALUE> recordReader;

    // By using map to store data into memory, please control memory by your self to avoid OOM in worker.
    private Map<KEY, VALUE> dataMap = null;

    protected AbstractCombineWorkerComputable() {
        this(false);
    }

    protected AbstractCombineWorkerComputable(boolean isOrder) {
        if(isOrder) {
            dataMap = new TreeMap<KEY, VALUE>();
        } else {
            dataMap = new HashMap<KEY, VALUE>();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see ml.shifu.guagua.worker.WorkerComputable#compute(ml.shifu.guagua.worker.WorkerContext)
     */
    @Override
    public WORKER_RESULT compute(WorkerContext<MASTER_RESULT, WORKER_RESULT> context) throws IOException {
        if(context.isFirstIteration()) {
            if(this.isLoaded.compareAndSet(false, true)) {
                init(context);

                long start = System.nanoTime();
                preLoad(context);
                long count = 0;
                for(GuaguaFileSplit fileSplit: context.getFileSplits()) {
                    LOG.info("Loading filesplit: {}", fileSplit);
                    try {
                        initRecordReader(fileSplit);
                        while(getRecordReader().nextKeyValue()) {
                            KEY currentKey = getRecordReader().getCurrentKey();
                            VALUE currentValue = getRecordReader().getCurrentValue();
                            doCompute(currentKey, currentValue, context);
                            dataMap.put(currentKey, currentValue);
                            count += 1L;
                        }
                    } finally {
                        if(getRecordReader() != null) {
                            getRecordReader().close();
                        }
                    }
                }
                if(count == 0L) {
                    throw new IllegalStateException(
                            "Record account in such worker is zero, please check if any exceptions in your input data.");
                }
                postLoad(context);
                LOG.info("Load {} records.", count);
                LOG.info("Data loading time with first iteration computing:{}ms",
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
            }
        } else {
            long start = System.nanoTime();
            try {
                for(Map.Entry<KEY, VALUE> entry: dataMap.entrySet()) {
                    doCompute(entry.getKey(), entry.getValue(), context);
                }
            } finally {
                LOG.info("Computation time for application {} container {} iteration {}: {}ms.", context.getAppId(),
                        context.getContainerId(), context.getCurrentIteration(),
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
            }
        }
        return context.getWorkerResult();
    }

    /**
     * Do some pre work before loading data.
     */
    protected void preLoad(WorkerContext<MASTER_RESULT, WORKER_RESULT> workerContext) {
    }

    /**
     * Do some post work after loading data.
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
     * Computation by each record, all update can be set to WORKER_RESULT by
     * {@code context.setCurrentWorkerResult(WORKER_RESULT)};
     */
    public abstract void doCompute(KEY currentKey, VALUE currentValue,
            WorkerContext<MASTER_RESULT, WORKER_RESULT> context);

    public GuaguaRecordReader<KEY, VALUE> getRecordReader() {
        return recordReader;
    }

    public void setRecordReader(GuaguaRecordReader<KEY, VALUE> recordReader) {
        this.recordReader = recordReader;
    }

}
