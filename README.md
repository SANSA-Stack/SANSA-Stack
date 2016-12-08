# SANSA-ML

SANSA-ML is the Machine Learning (ML) library in the SANSA stack (see http://sansa-stack.net). Algorithms in this repository perform various machine learning tasks directly on [RDF](https://en.wikipedia.org/wiki/Resource_Description_Framework)/[OWL](https://en.wikipedia.org/wiki/Web_Ontology_Language) input data. While most machine learning algorithms are based on processing simple features, the machine learning algorithms in SANSA-ML exploit the graph structure and semantics of the background knowledge specified using the RDF and OWL standards. In many cases, this allows to obtain either more accurate or more human-understandable results. In contrast to most other algorithms supporting background knowledge, they scale horizontally using [Apache Spark](https://spark.apache.org). 

The ML layer currently supports the following algorithms:
* RDF graph clustering
* Rule mining in RDF graphs based on [AMIE+](https://www.mpi-inf.mpg.de/departments/databases-and-information-systems/research/yago-naga/amie/)

Usage example for clusting:
```scala
RDFByModularityClustering(sparkSession.sparkContext, numIterations, input, output)
```

Please see https://github.com/SANSA-Stack/SANSA-Examples/tree/master/sansa-examples-spark/src/main/scala/net/sansa_stack/examples/spark/ml for further examples.

Several further algorithms are in development. Please create a pull request and/or contact [Jens Lehmann](http://jens-lehmann.org) if you are interested in contributing algorithms to SANSA-ML.

Support for [Apache Flink](https://flink.apache.org) is planned in future releases.
