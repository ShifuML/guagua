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
package ml.shifu.guagua.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import ml.shifu.guagua.GuaguaRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A lock with a predicate that was be used to synchronize events and keep the job updated while waiting.
 */
public class ProgressLock {
    /** Class logger */
    private static final Logger LOG = LoggerFactory.getLogger(ProgressLock.class);
    /** Default msecs to refresh the progress meter */
    private static final int DEFAULT_MSEC_PERIOD = 10000;
    /** Actual mses to refresh the progress meter */
    private final int msecPeriod;
    /** Lock */
    private Lock lock = new ReentrantLock();
    /** Condition associated with lock */
    private Condition cond = lock.newCondition();
    /** Predicate */
    private boolean eventOccurred = false;

    /**
     * Constructor with default values.
     */
    public ProgressLock() {
        this(DEFAULT_MSEC_PERIOD);
    }

    /**
     * Constructor.
     * 
     * @param msecPeriod
     *            Msecs between progress reports
     */
    public ProgressLock(int msecPeriod) {
        this.msecPeriod = msecPeriod;
    }

    public void reset() {
        this.lock.lock();
        try {
            this.eventOccurred = false;
        } finally {
            this.lock.unlock();
        }
    }

    public void signal() {
        this.lock.lock();
        try {
            this.eventOccurred = true;
            this.cond.signalAll();
        } finally {
            this.lock.unlock();
        }
    }

    public boolean waitMsecs(int msecs) {
        if(msecs < 0) {
            throw new GuaguaRuntimeException("waitMsecs: msecs cannot be negative!");
        }
        long maxMsecs = System.currentTimeMillis() + msecs;
        int curMsecTimeout = 0;
        this.lock.lock();
        try {
            while(!this.eventOccurred) {
                curMsecTimeout = Math.min(msecs, this.msecPeriod);
                // to avoid log flood
                if(maxMsecs % 50 == 0) {
                    LOG.info("waitMsecs: Wait for {}.", curMsecTimeout);
                }
                try {
                    boolean signaled = cond.await(curMsecTimeout, TimeUnit.MILLISECONDS);
                    if(maxMsecs % 50 == 0) {
                        LOG.info("waitMsecs: Got timed signaled of {}.", signaled);
                    }
                } catch (InterruptedException e) {
                    throw new IllegalStateException("waitMsecs: Caught interrupted " + "exception on cond.await() "
                            + curMsecTimeout, e);
                }
                if(System.currentTimeMillis() > maxMsecs) {
                    return false;
                }
                msecs = Math.max(0, msecs - curMsecTimeout);
            }
        } finally {
            this.lock.unlock();
        }
        return true;
    }

    public void waitForever() {
        while(!waitMsecs(this.msecPeriod)) {
        }
    }
}
