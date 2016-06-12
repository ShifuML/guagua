/*
 * Copyright [2013-2015] PayPal Software Foundation
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
package ml.shifu.guagua.io;

/**
 * {@link Combinable} is to combine {@link Bytable} results together to save memory in master computation.
 * 
 * <p>
 * In the future, {@link Combinable} can also be used to combine worker results in two-level scheduling.
 */
public interface Combinable<RESULT extends Bytable> {

    /**
     * Combine result to existing one.
     */
    RESULT combine(RESULT from);

}
