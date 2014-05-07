#!/bin/bash
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

OLD_HADOOP_CLASSPATH="$HADOOP_CLASSPATH"

# set new HADOOP_CLASSPATH to run guagua
export HADOOP_CLASSPATH="../lib/guagua-yarn-examples-0.4.0.jar:../lib/guagua-yarn-0.4.0.jar:../lib/gson-2.2.2.jar:../lib/guava-11.0.2.jar:../lib/guagua-core-0.4.0.jar:../lib/zookeeper-3.4.5.jar"

hadoop jar ../lib/guagua-yarn-examples-0.4.0.jar \
    ml.shifu.guagua.yarn.GuaguaYarnClient  \
    -libjars ../lib/guagua-yarn-examples-0.4.0.jar,../lib/guagua-yarn-0.4.0.jar,../lib/gson-2.2.2.jar,../lib/guava-11.0.2.jar,../lib/guagua-core-0.4.0.jar,../lib/zookeeper-3.4.5.jar \
    -i sum  \
    -z ${zookeeper.server}  \
    -w ml.shifu.guagua.yarn.example.sum.SumWorker  \
    -m ml.shifu.guagua.yarn.example.sum.SumMaster  \
    -c 10 \
    -n "guagua" \
    -mr org.apache.hadoop.io.LongWritable \
    -wr org.apache.hadoop.io.LongWritable \
    -Dmapred.job.queue.name=default \
    -Dguagua.sum.output=sum-output \
    -Dguagua.yarn.queue.name=default \
    -Dguagua.master.intercepters=ml.shifu.guagua.yarn.example.sum.SumOutput

## restore HADOOP_CLASSPATH 
export HADOOP_CLASSPATH="$OLD_HADOOP_CLASSPATH"
