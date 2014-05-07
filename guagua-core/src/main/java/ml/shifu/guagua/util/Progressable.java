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

/**
 * A facility for reporting progress.
 * 
 * <p>
 * {@code isKillTask} is helpful for you to determine whether to kill itself to involve fail-over mechanism. By using
 * fail-over, we can launch task in other machines which will improve the efficiency.
 * 
 * <p>
 * For 10 tasks, 9 of them are very fast while the last one is very slow, which makes the whole iteration is very slow.
 * It is important to kill this task and launch it in other machines.
 * 
 * <p>
 * Since no too much computation on master, {@code isKillTask} only works on worker nodes so far.
 */
public interface Progressable {

    /**
     * Report progress to caller.
     * 
     * @param currentIteration
     *            current iteration.
     * @param totalIteration
     *            total iteration.
     * @param status
     *            report current status description to caller.
     * @param isLastUpdate
     *            whether it is last update of current iteration
     * @param isKillTask
     *            Whether to kill itself because of computation is too long over a threshold or a statistics time over
     *            95% other tasks.
     */
    public void progress(int currentIteration, int totalIteration, String status, boolean isLastUpdate,
            boolean isKillTask);
}
