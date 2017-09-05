[<img src="images/guagua_duck_50.png" alt="Guagua" align="left">](http://shifu.ml)<div align="right"><div>[![Build Status](https://travis-ci.org/ShifuML/guagua.svg)](https://travis-ci.org/ShifuML/shifu)</div><div>[![Maven Central](https://maven-badges.herokuapp.com/maven-central/ml.shifu/guagua/badge.svg)](https://maven-badges.herokuapp.com/maven-central/ml.shifu/guagua)</div></div>

## Guagua

An iterative computing framework on both Hadoop MapReduce and Hadoop YARN.

## News

Guagua 0.7.7 is released with a lot of improvements. Check our [changes] (https://github.com/ShifuML/guagua/blob/master/CHANGES.txt#L19)

## Conference

[QCON Shanghai 2014](http://2014.qconshanghai.com/node/474) [Slides](http://www.slideshare.net/pengshanzhang/guagua-an-iterative-computing-framework-on-hadoop)

## Getting Started

Please visit [Guagua wiki site](https://github.com/ShifuML/guagua/wiki) for tutorials.

## What is Guagua?
**Guagua**, a sub-project of Shifu, is a distributed, pluggable and scalable iterative computing framework based on Hadoop MapReduce and YARN.

This graph shows the iterative computing process for **Guagua**.

![Guagua Process](images/guagua-process.png)

Typical use cases for **Guagua** are distributed machine learning model training based on Hadoop. By using **Guagua**, we implement distributed neural network algorithm which can reduce model training time from days to hours on 1TB data sets. Distributed neural network algorithm is based on [Encog](http://www.heatonresearch.com/encog) and **Guagua**. Any details please check our example [source code](https://github.com/ShifuML/guagua/tree/master/guagua-mapreduce-examples/src/main/java/ml/shifu/guagua/mapreduce/example/nn).

## Google Group

Please join [Guagua group](https://groups.google.com/forum/#!forum/shifu-guagua) if questions, bugs or anything else.

## Copyright and License

Copyright 2013-2017, PayPal Software Foundation under the [Apache License V2.0](LICENSE.txt).
