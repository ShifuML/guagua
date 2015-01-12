guagua-examples
======

## Pre Requirements

*  JDK 1.6 or JDK 1.7
*  Maven 2.x or Maven 3.x

## Get Package

*  cd guagua
*  ./package.sh

You will get package at guagua-client/target/guagua-${version}.tar.gz

## Run Example in Hadoop MapReduce

*  scp guagua-client/target/guagua-${version}.tar.gz ${hadoop-cli}:~/
*  ssh <hadoop-cli>
*  tar zxvf guagua-${version}.tar.gz
*  cd guagua-${version}/
*  bin/demo-mr*.sh

## Run Example in Hadoop YARN

*  scp guagua-client/target/guagua-${version}.tar.gz ${hadoop-cli}:~/
*  ssh <hadoop-cli>
*  tar zxvf guagua-${version}.tar.gz
*  cd guagua-${version}/
*  bin/demo-yarn.sh