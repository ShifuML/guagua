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

# Comments for all parameters:
#  '../mapreduce-lib/guagua-mapreduce-examples-0.5.0-SNAPSHOT.jar': Jar files include master, worker and user intercepters
#  '-i sum': '-i' means guagua application input, should be HDFS input file or folder
#  '-z ${ZOOKEEPER_SERVERS}': '-z' is used to configure zookeeper server, this should be placed by real zookeeper server
#                            The format is like '<zkServer1:zkPort1,zkServer2:zkPort2>'
#  '-w ml.shifu.guagua.mapreduce.example.sum.SumWorker': Worker computable implementation class setting
#  '-m ml.shifu.guagua.mapreduce.example.sum.SumMaster': Master computable implementation class setting
#  '-c 10': Total iteration number setting
#  '-n Guagua-Sum-Master-Workers-Job': Hadoop job name or YARN application name specified
#  '-mr org.apache.hadoop.io.LongWritable': Master result class setting
#  '-wr org.apache.hadoop.io.LongWritable': Worker result class setting
#  '-Dmapred.job.queue.name=default': Queue name setting
#  '-Dguagua.sum.output=sum-output': Output file, this is used in 'ml.shifu.guagua.mapreduce.example.sum.SumOutput'
#  '-Dguagua.master.intercepters=ml.shifu.guagua.mapreduce.example.sum.SumOutput': User master intercepters

ZOOKEEPER_SERVERS=

if [ "${ZOOKEEPER_SERVERS}X" == "X" ] ; then
  echo "Zookeeper server should be provided for guagua coordination. Set 'ZOOKEEPER_SERVERS' at first please."
  exit 1
fi

./guagua jar ../mapreduce-lib/guagua-mapreduce-examples-0.5.0-SNAPSHOT.jar \
        -i sum  \
        -z ${ZOOKEEPER_SERVERS}  \
        -w ml.shifu.guagua.mapreduce.example.sum.SumWorker  \
        -m ml.shifu.guagua.mapreduce.example.sum.SumMaster  \
        -c 10 \
        -n "Guagua-Sum-Master-Workers-Job" \
        -mr org.apache.hadoop.io.LongWritable \
        -wr org.apache.hadoop.io.LongWritable \
        -Dmapred.job.queue.name=default \
        -Dguagua.sum.output=sum-output \
        -Dguagua.master.intercepters=ml.shifu.guagua.mapreduce.example.sum.SumOutput
