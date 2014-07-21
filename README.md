[<img src="images/guagua_duck_50.png" alt="Guagua" align="left">](http://shifu.ml/docs/stable/guide/guagua/)<div align="right"> [![Build Status](https://travis-ci.org/ShifuML/guagua.svg?branch=master)](https://travis-ci.org/ShifuML/guagua)</div>

## Guagua

An iterative computing framework on both Hadoop MapReduce and Hadoop YARN.

## Getting Started

Please visit [guagua site](http://shifu.ml/docs/stable/guide/guagua/) for tutorials.

## What is Guagua?
**Guagua**, a sub-project of Shifu, is a distributed, pluggable and scalable iterative computing framework based on Hadoop MapReduce and YARN.

This graph shows the iterative computing process for **Guagua**.

![Guagua Process](images/guagua-process.png)

Typical use cases for **Guagua** are distributed machine learning model traing based on Hadoop. By using **Guagua**, we implement distributed neural network algorithm which can reduce model training time from days to hours on 500GB data sets. For distributed neural network algorithm, it is based on [Encog](http://www.heatonresearch.com/encog). For any details please check our example [source code](https://github.com/ShifuML/guagua/tree/master/guagua-mapreduce-examples/src/main/java/ml/shifu/guagua/mapreduce/example/nn).


## Copyright and License

Copyright 2013-2014, eBay Software Foundation under [the Apache License](LICENSE.txt).
