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
package ml.shifu.guagua;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

import ml.shifu.guagua.master.MasterComputable;

/**
 * Master and Worker computable maximal time out setting. If {@link ComputableMonitor} is attached with master or worker
 * computable function, null will return if over time out setting. Worker result should be checked by null in
 * MasterContext or master result should be checked by null in WorkerContext.
 * 
 * <p>
 * Please check this example:
 * 
 * <pre>
 *  @ComputableMonitor(timeUnit = TimeUnit.SECONDS, duration = 60)
 *  public class SumWorker ...
 * </pre>
 * 
 * <p>
 * In {@link MasterComputable}, worker result should be wrapped by null checking:
 * 
 * <pre>
 *   if(workerResult != null) {
 *      ...
 *   }
 * </pre>
 */
@Documented
@Inherited
@Target({ ElementType.TYPE })
@Retention(value = RetentionPolicy.RUNTIME)
public @interface ComputableMonitor {

    /**
     * Time Units in which to measure timeout value.
     * 
     * @return Time Units in which to measure timeout value.
     */
    TimeUnit timeUnit() default TimeUnit.SECONDS;

    /**
     * Number of time units after which the execution should be halted and default returned.
     * 
     * @return Number of time units after which the execution should be halted or default returned.
     */
    long duration() default 60;

    /**
     * Soft means to return null if worker or master is time out. If not soft means to kill process if time out.
     */
    boolean isSoft() default true;
}
