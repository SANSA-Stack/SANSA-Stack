# SANSA-ML
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/net.sansa-stack/sansa-ml-parent_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/net.sansa-stack/sansa-ml-parent_2.11)
[![Build Status](https://ci.aksw.org/jenkins/job/SANSA-ML/job/develop/badge/icon)](https://ci.aksw.org/jenkins/job/SANSA-ML//job/develop/)
[![Coverage Status](https://coveralls.io/repos/github/SANSA-Stack/SANSA-ML/badge.svg?branch=develop)](https://coveralls.io/github/SANSA-Stack/SANSA-ML?branch=develop)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Twitter](https://img.shields.io/twitter/follow/SANSA_Stack.svg?style=social)](https://twitter.com/SANSA_Stack)

SANSA-ML is the Machine Learning (ML) library in the SANSA stack (see http://sansa-stack.net). Algorithms in this repository perform various machine learning tasks directly on [RDF](https://en.wikipedia.org/wiki/Resource_Description_Framework)/[OWL](https://en.wikipedia.org/wiki/Web_Ontology_Language) input data. While most machine learning algorithms are based on processing simple features, the machine learning algorithms in SANSA-ML exploit the graph structure and semantics of the background knowledge specified using the RDF and OWL standards. In many cases, this allows to obtain either more accurate or more human-understandable results. In contrast to most other algorithms supporting background knowledge, they scale horizontally using [Apache Spark](https://spark.apache.org) and [Apache Flink](https://flink.apache.org).

The ML layer currently supports the following algorithms:
* RDF graph clustering (Power Iteration, Border Flow, Link based clustering, Modularity based clustering, Silvia Link Clustering)
* Rule mining in RDF graphs based on [AMIE+](https://www.mpi-inf.mpg.de/departments/databases-and-information-systems/research/yago-naga/amie/)
* Semantic similarity measures (Jaccard similarity,Rodríguez and Egenhofer similarity, Tversky Ratio Model, Batet Similarity)
* Knowledge graph embedding approaches: 
  * TransE (beta status)
  * DistMult (beta status)
* Terminological Decision Trees for the classification of concepts(beta status)
* Anomaly detection (beta status)
* RDF graph kernel based on [A Fast and Simple Graph Kernel for RDF](http://ceur-ws.org/Vol-1082/paper2.pdf)

Please see https://github.com/SANSA-Stack/SANSA-Examples/tree/master/sansa-examples-spark/src/main/scala/net/sansa_stack/examples/spark/ml for examples on how to use the above machine learning approaches.

Several further algorithms are in development. Please create a pull request and/or contact [Jens Lehmann](http://jens-lehmann.org) if you are interested in contributing algorithms to SANSA-ML.

## How to Contribute
We always welcome new contributors to the project! Please see [our contribution guide](http://sansa-stack.net/contributing-to-sansa/) for more details on how to get started contributing to SANSA.