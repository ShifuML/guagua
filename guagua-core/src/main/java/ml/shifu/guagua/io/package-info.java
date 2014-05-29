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

/**
 * This package contains guagua own io settings.
 * 
 * <p>
 * {@link com.paypal.guagua.io.Bytable} is a copy from Hadoop Writable, the reason is that guagua doesn't want to depend 
 * on Hadoop platform since guagua is designed to support all kinds of computation platform. Check GuaguaWritableAdapter 
 * in guagua-mapreduce to see how to use Hadoop Writable with guagua {@link com.paypal.guagua.io.Bytable}.
 * 
 * <p>
 * Compressed serialzer like {@link com.paypal.guagua.io.GZIPBytableSerializer} is used to support gz format. And 
 * {@link com.paypal.guagua.io.Bzip2BytableSerializer} is for bz2 format.
 * 
 */
package ml.shifu.guagua.io;

