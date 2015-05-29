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
package ml.shifu.guagua.yarn;

/**
 * {@link GuaguaIterationStatus} is used to send iteration status from master and worker to Guagua application master.
 */
public class GuaguaIterationStatus {

    /**
     * Current partition number in that task, each task has its own partition number.
     */
    private int partition;

    /**
     * Current iteration.
     */
    private int currentIteration;

    /**
     * Total iteration
     */
    private int totalIteration;

    /**
     * Time started for current iteration.
     */
    private long time;

    /**
     * Whether to kill container. For straggler container, this is to notice AppMaster to kill contianer and make fault
     * tolerance work to restart a contianer.
     */
    private boolean isKillContainer;

    /**
     * Default constructor.
     */
    public GuaguaIterationStatus() {
    }

    /**
     * Constructor with partition, current iteration and total iteration setting.
     */
    public GuaguaIterationStatus(int partition, int currentIteration, int totalIteration) {
        this.partition = partition;
        this.currentIteration = currentIteration;
        this.totalIteration = totalIteration;
        this.time = System.currentTimeMillis();
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public int getCurrentIteration() {
        return currentIteration;
    }

    public void setCurrentIteration(int currentIteration) {
        this.currentIteration = currentIteration;
    }

    public int getTotalIteration() {
        return totalIteration;
    }

    public void setTotalIteration(int totalIteration) {
        this.totalIteration = totalIteration;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    /**
     * @return the isKillContainer
     */
    public boolean isKillContainer() {
        return isKillContainer;
    }

    /**
     * @param isKillContainer
     *            the isKillContainer to set
     */
    public void setKillContainer(boolean isKillContainer) {
        this.isKillContainer = isKillContainer;
    }

    @Override
    public String toString() {
        return String
                .format("GuaguaIterationStatus [partition=%s, currentIteration=%s, totalIteration=%s, time=%s, isKillContainer=%s]",
                        partition, currentIteration, totalIteration, time, isKillContainer);
    }

}
