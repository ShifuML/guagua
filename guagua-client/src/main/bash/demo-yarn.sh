#!/bin/bash
#
# Copyright [2013-2014] PayPal Software Foundation
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

# Guagua YARN implementation is still in beta version, For Hadoop 2.0, 
# we should use '-y' to enable it or we will still use mapreduce to run 
# Guagua application.

# please follow ../README.md to run this demo shell.

# prepare input data
BIN_DIR="$( cd -P "$( dirname "${BASH_SOURCE:-0}" )" && pwd )"
hadoop fs -put $BIN_DIR/../data/sum /user/$USER/

# Comments for all parameters:
#  '-i sum': '-i' means guagua application input, should be HDFS input file or folder
#  '-z ${ZOOKEEPER_SERVERS}': '-z' is used to configure zookeeper server, this should be placed by real zookeeper server.
#       The format is like '<zkServer1:zkPort1,zkServer2:zkPort2>'
#      If user doesn't specify this parameter, a zookeeper server in CLI host will be embeded.
#  '-w ml.shifu.guagua.example.sum.SumWorker': Worker computable implementation class setting
#  '-m ml.shifu.guagua.example.sum.SumMaster': Master computable implementation class setting
#  '-c 10': Total iteration number setting
#      If user doesn't specify this parameter, default 10 will be used.
#  '-n Guagua-Sum-Master-Workers-Job': Hadoop job name or YARN application name specified
#  '-mr org.apache.hadoop.io.LongWritable': Master result class setting
#  '-wr org.apache.hadoop.io.LongWritable': Worker result class setting
#  '../yarn-lib/guagua-examples-0.7.1.jar': Jar files include master, worker and user intercepters
#  '-Dmapred.job.queue.name=default': Queue name setting
#  '-Dguagua.yarn.queue.name=default': Queue name setting for guagua YARN application
#  '-Dguagua.sum.output=sum-output': Output file, this is used in 'ml.shifu.guagua.example.sum.SumOutput'
#  '-Dguagua.master.intercepters=ml.shifu.guagua.example.sum.SumOutput': User master intercepters, SumOutput is 
#        used to save global sum result to HDFS.

$BIN_DIR/guagua -y \
        jar $BIN_DIR/../yarn-lib/guagua-examples-0.7.1.jar \
        -i sum  \
        -w ml.shifu.guagua.example.sum.SumWorker  \
        -m ml.shifu.guagua.example.sum.SumMaster  \
        -n "Guagua-Sum-Master-Workers-Job" \
        -mr org.apache.hadoop.io.LongWritable \
        -wr org.apache.hadoop.io.LongWritable \
        -Dmapred.job.queue.name=default \
        -Dguagua.sum.output=sum-output \
        -Dguagua.yarn.queue.name=default \
        -Dguagua.master.intercepters=ml.shifu.guagua.example.sum.SumOutput

