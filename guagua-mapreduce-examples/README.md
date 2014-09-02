guagua-mapreduce-examples
======

## Pre Requirements

*  JDK 1.6 or JDK 1.7
*  Maven 2.x or Maven 3.x

## Get Package

*  cd guagua
*  mvn clean install

You will get package at guagua-mapreduce-examples/target/guagua-mapreduce-examples-${version}.tar.gz

## Run Sum Example in Hadoop

*  scp guagua-mapreduce-examples/target/guagua-mapreduce-examples-${version}.tar.gz ${hadoop-cli}:~/
*  ssh ${hadoop-cli}
*  tar zxvf guagua-mapreduce-examples-${version}.tar.gz
*  cd guagua-mapreduce-examples-${version}/
*  hadoop fs -put data/sum /user/${username}
*  Set input in bin/run-sum.sh with '-i sum'
*  bin/run-sum.sh

## Run NN Example in Hadoop

*  scp guagua-mapreduce-examples/target/guagua-mapreduce-examples-${version}.tar.gz ${hadoop-cli}:~/
*  ssh ${hadoop-cli}
*  tar zxvf guagua-mapreduce-examples-${version}.tar.gz
*  cd guagua-mapreduce-examples-${version}/
*  hadoop fs -put data/nn /user/${username}
*  Set input in bin/run-nn.sh with '-i nn'
*  bin/run-nn.sh
