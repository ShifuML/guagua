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
hadoop fs -put $BIN_DIR/../data/lr /user/$USER/

$BIN_DIR/guagua jar $BIN_DIR/../mapreduce-lib/guagua-examples-0.6.0.jar \
        -i lr  \
        -w ml.shifu.guagua.example.lr.LogisticRegressionWorker  \
        -m ml.shifu.guagua.example.lr.LogisticRegressionMaster  \
        -n "Guagua-Logistic-Regression-Master-Workers-Job" \
        -mr ml.shifu.guagua.example.lr.LogisticRegressionParams \
        -wr ml.shifu.guagua.example.lr.LogisticRegressionParams \
        -Dmapred.job.queue.name=default \
        -Dlr.model.output=lr-output \
        -Dguagua.master.intercepters=ml.shifu.guagua.example.lr.LogisticRegressionOutput
