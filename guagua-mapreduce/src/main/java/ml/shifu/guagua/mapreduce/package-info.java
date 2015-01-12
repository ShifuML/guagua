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
 * This package contains guagua-mapreduce implementation details based one only mapper hadoop job.
 * 
 * <p>
 * {@link com.paypal.guagua.mapreduce.GuaguaMapReduceClient} is the entry point to trigger a guagua job by invoking its 
 * <code>main</code> method. 
 * 
 * <p>
 * {@link com.paypal.guagua.mapreduce.GuaguaMRUnitDriver} is a class for unit test, it will run guagua master-workers in 
 * one jvm instance. See SumTest in guagua-mapreduce-examples project for details.
 * 
 * <p>
 * Hadoop input format and output format customization for guagua is in 
 * {@link com.paypal.guagua.mapreduce.GuaguaInputFormat} and {@link com.paypal.guagua.mapreduce.GuaguaOutputFormat} and 
 * other related classes.
 */
package ml.shifu.guagua.mapreduce;

