guagua-mapreduce-examples
======

## Pre Requirements

*  JDK 1.6 or JDK 1.7
*  Maven 2.x or Maven 3.x

## Get package

*  cd guagua
*  ./package.sh

You will get package at guagua-client/target/guagua-${version}.tar.gz

## Run Example in Hadoop MapReuce

*  scp guagua-client/target/guagua-${version}.tar.gz ${hadoop-cli}:~/
*  ssh <hadoop-cli>
*  tar zxvf guagua-${version}.tar.gz
*  cd guagua-${version}/
*  hadoop fs -put data/sum /user/<username>
*  Set input in bin/demo-mr.sh with '-i sum'
*  Set zookeeper server in bin/demo-mr.sh with '-z ${zkserver:zkport}'
*  bin/demo-mr.sh

## Run Example in Hadoop YARN

*  scp guagua-client/target/guagua-${version}.tar.gz ${hadoop-cli}:~/
*  ssh <hadoop-cli>
*  tar zxvf guagua-${version}.tar.gz
*  cd guagua-${version}/
*  hadoop fs -put data/sum /user/<username>
*  Set input in bin/demo-yarn.sh with '-i sum'
*  Set zookeeper server in bin/demo-yarn.sh with '-z ${zkserver:zkport}'
*  bin/demo-yarn.sh
