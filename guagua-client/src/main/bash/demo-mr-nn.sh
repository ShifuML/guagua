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

# prepare input data
BIN_DIR="$( cd -P "$( dirname "${BASH_SOURCE:-0}" )" && pwd )"
hadoop fs -put $BIN_DIR/../data/nn /user/$USER/

$BIN_DIR/guagua jar $BIN_DIR/../mapreduce-lib/guagua-examples-0.6.1.jar \
    -i nn  \
    -w ml.shifu.guagua.example.nn.NNWorker  \
    -m ml.shifu.guagua.example.nn.NNMaster  \
    -c 100 \
    -n "Guagua NN Master-Workers Job" \
    -mr ml.shifu.guagua.example.nn.meta.NNParams \
    -wr ml.shifu.guagua.example.nn.meta.NNParams \
    -Dguagua.nn.output=NN.model -Dnn.test.scale=1 -Dnn.record.scale=1 \
    -Dguagua.master.intercepters=ml.shifu.guagua.example.nn.NNOutput  \
    -Dmapred.job.queue.name=default
