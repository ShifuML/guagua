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
 * This package contains our own io settings for guagua.
 * 
 * <p>
 * {@link com.paypal.guagua.io.Bytable} is a copy from hadoop Writable, the reason is that we don't want to depend on 
 * hadoop platform since guagua is designed to support all kinds of computation platform. One can check 
 * GuaguaWritableAdapter in guagua-mapreduce to see how to use hadoop writable with guagua 
 * {@link com.paypal.guagua.io.Bytable}.
 * 
 * <p>
 * Compressed serialzer like {@link com.paypal.guagua.io.GZIPBytableSerializer} is used to support gz format. And 
 * {@link com.paypal.guagua.io.Bzip2BytableSerializer} is for bz2 format.
 * 
 */
package ml.shifu.guagua.io;

