guagua-yarn-examples
======

## Pre Requirements

*  JDK 1.6 or JDK 1.7
*  Maven 2.x or Maven 3.x

## Get package

*  cd guagua
*  mvn clean install

You will get package at guagua-yarn-examples/target/guagua-yarn-examples-${version}.tar.gz

## Run in Hadoop

*  scp guagua-yarn-examples/target/guagua-yarn-examples-${version}.tar.gz ${hadoop-cli}:~/
*  ssh ${hadoop-cli}
*  tar zxvf guagua-yarn-examples-${version}.tar.gz
*  cd guagua-yarn-examples-${version}/
*  hadoop fs -put data/sum /user/${username}
*  Set input in bin/run-sum.sh with '-i sum'
*  Set zookeeper server in  bin/run-sum.sh with '-z ${zkserver:zkport}'
*  bin/run-sum.sh

## Run NN Example in Hadoop

*  scp guagua-yarn-examples/target/guagua-yarn-examples-${version}.tar.gz ${hadoop-cli}:~/
*  ssh ${hadoop-cli}
*  tar zxvf guagua-yarn-examples-${version}.tar.gz
*  cd guagua-yarn-examples-${version}/
*  hadoop fs -put data/nn /user/${username}
*  Set input in bin/run-nn.sh with '-i nn'
*  Set zookeeper server in  bin/run-sum.sh with '-z ${zkserver:zkport}'
*  bin/run-nn.sh

