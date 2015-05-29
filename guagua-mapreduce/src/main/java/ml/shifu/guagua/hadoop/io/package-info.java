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

/**
 * This package contains hadoop related common io classes.
 * 
 * <p>
 * Noticed that each class in this package is the same package and same name with the one in guagua-yarn. This is 
 * important to make sure such common hadoop io utils can be used in both guagua mapreduce and yarn implementations. 
 * 
 * <p>
 * IO related classes including {@link com.paypal.guagua.hadoop.io.GuaguaWritableAdapter} and 
 * {@link com.paypal.guagua.hadoop.io.GuaguaWritableSerializer} can be used to combine hadoop 
 * {@link org.apache.hadoop.io.Writable} with guagua {@link com.paypal.guagua.io.Bytable}.
 * 
 * <p>
 * Hadoop input format and output format customization for guagua is in 
 * {@link com.paypal.guagua.hadoop.io.GuaguaInputFormat} and {@link com.paypal.guagua.mapreduce.GuaguaOutputFormat} and 
 * other related classes.
 */
package ml.shifu.guagua.hadoop.io;

