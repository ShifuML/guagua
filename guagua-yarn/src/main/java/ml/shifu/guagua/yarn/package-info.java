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

/**
 * This package contains all the guagua-yarn implementation which is used to launch guagua master workers job on YARN 
 * framework.
 * 
 * <p>
 * {@link com.paypal.guagua.yarn.GuaguaYarnClient} is the entry point to trigger a guagua job by invoking its 
 * <code>main</code> method. 
 * 
 * <p>
 * {@link com.paypal.guagua.yarn.GuaguaAppMaster} is served as an application master implementation for YARM.
 * 
 * <p>
 * {@link com.paypal.guagua.yarn.GuaguaYarnTask} is used to lanuch master and worker containers on hadoop YARN framework.
 */
package ml.shifu.guagua.yarn;

