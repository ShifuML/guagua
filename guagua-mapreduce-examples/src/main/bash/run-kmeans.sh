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

ZOOKEEPER_SERVERS=

if [ "${ZOOKEEPER_SERVERS}X" == "X" ] ; then
  echo "Zookeeper server should be provided for guagua coordination. Set 'ZOOKEEPER_SERVERS' at first please."
  exit 1
fi

KMEANS_INIT_CENTERS=1,1:0,0:-1,-1

if [ "${KMEANS_INIT_CENTERS}X" == "X" ] ; then
  echo "Kmeans initial centers should be set firstly. Set 'KMEANS.INIT.CENTERS' at first please."
  exit 1
fi

./guagua jar ../mapreduce-lib/guagua-mapreduce-examples-0.5.0-SNAPSHOT.jar \
        -i nn  \
        -z ${ZOOKEEPER_SERVERS}  \
        -w ml.shifu.guagua.mapreduce.example.kmeans.KMeansWorker  \
        -m ml.shifu.guagua.mapreduce.example.kmeans.KMeansMaster  \
        -c 10 \
        -n "Guagua-KMeans-Master-Workers-Job" \
        -mr ml.shifu.guagua.mapreduce.example.kmeans.KMeansMasterParams \
        -wr ml.shifu.guagua.mapreduce.example.kmeans.KMeansWorkerParams \
        -Dmapred.job.queue.name=default \
        -Dkmeans.k.number=3 \
        -Dkmeans.data.seperator=| \
        -Dkmeans.column.number=2 \
        -Dkmeans.centers.output=kmeans-centers \
        -Dkmeans.data.output=kmeans-tags \
        -Dkmeans.k.centers=${KMEANS_INIT_CENTERS} \
        -Dguagua.master.intercepters=ml.shifu.guagua.mapreduce.example.kmeans.KMeansCenterOutput
