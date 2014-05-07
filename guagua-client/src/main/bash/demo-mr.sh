#!/bin/bash
#
#
# Copyright [2013-2014] eBay Software Foundation
#  
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#  
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# please follow ../README.md to run this demo shell.

./guagua -Dmapred.job.queue.name=default \
        -Dguagua.sum.output=sum-output \
        -Dguagua.master.intercepters=ml.shifu.guagua.mapreduce.example.sum.SumOutput \
        ../mapreduce-lib/guagua-mapreduce-examples-0.4.0.jar \
        -i sum  \
        -z ${zookeeper.server}  \
        -w ml.shifu.guagua.mapreduce.example.sum.SumWorker  \
        -m ml.shifu.guagua.mapreduce.example.sum.SumMaster  \
        -c 10 \
        -n "Guagua-Sum-Master-Workers-Job" \
        -mr org.apache.hadoop.io.LongWritable \
        -wr org.apache.hadoop.io.LongWritable
                                           
