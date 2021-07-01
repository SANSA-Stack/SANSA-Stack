---
title: Overview
nav_order: 1
---

# Documentation
This module provides the scala doc documentation. You can find the scala docs [here](https://sansa-stack.github.io/SANSA-Stack/scaladocs/0.8.0/net/sansa_stack/index.html). The module tree is accessible on the right-hand side.
We also provide ReadMe files in the SANSA-Stack Github project. The Github project is available [here](https://github.com/SANSA-Stack/SANSA-Stack).
## Layers
The SANSA project is structured in the following five layers developed in their respective sub-folders:
* RDF [scala-docs](https://sansa-stack.github.io/SANSA-Stack/scaladocs/0.8.0/net/sansa_stack/rdf/index.html), [readme](https://github.com/SANSA-Stack/SANSA-Stack/blob/develop/sansa-rdf/README.md), [package](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-rdf)
* OWL [scala-docs](https://sansa-stack.github.io/SANSA-Stack/scaladocs/0.8.0/net/sansa_stack/owl/index.html), [readme](https://github.com/SANSA-Stack/SANSA-Stack/blob/develop/sansa-owl/README.md), [package](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-owl)
* Query [scala-docs](https://sansa-stack.github.io/SANSA-Stack/scaladocs/0.8.0/net/sansa_stack/query/index.html), [readme](https://github.com/SANSA-Stack/SANSA-Stack/blob/develop/sansa-query/README.md), [package](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-query)
* Inference [scala-docs](https://sansa-stack.github.io/SANSA-Stack/scaladocs/0.8.0/net/sansa_stack/inference/index.html), [readme](https://github.com/SANSA-Stack/SANSA-Stack/blob/develop/sansa-inference/README.md), [package](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-inference)
* Machine Learning [scala-docs](https://sansa-stack.github.io/SANSA-Stack/scaladocs/0.8.0/net/sansa_stack/ml/index.html), [readme](https://github.com/SANSA-Stack/SANSA-Stack/blob/develop/sansa-ml/README.md), [package](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-ml)


## Paper Specific Documentation
For recently published papers, we provide here a fast entry point to the provided modules. All of those modules are as well accessible over the standard documentation
### DistSim ICSC Paper Documentation
the documentation in scala docs are available [here](https://sansa-stack.github.io/SANSA-Stack/scaladocs/0.7.1_ICSC_paper/#package)
the respective similarity estimation models are in this [github directory](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-ml/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/similarity) and further needed utils are [here](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-ml/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/utils)

#### ScalaDocs:
* [Feature Extractor](https://sansa-stack.github.io/SANSA-Stack/scaladocs/0.7.1_ICSC_paper/#net.sansa_stack.ml.spark.utils.FeatureExtractorModel)
* [Similarity Estiamtion Modules](https://sansa-stack.github.io/SANSA-Stack/scaladocs/0.7.1_ICSC_paper/#net.sansa_stack.ml.spark.similarity.similarityEstimationModels.package)
* [Minmial Examples](https://sansa-stack.github.io/SANSA-Stack/scaladocs/0.7.1_ICSC_paper/#net.sansa_stack.ml.spark.similarity.examples.package)
* [Evaluation of Experiment Class](https://sansa-stack.github.io/SANSA-Stack/scaladocs/0.7.1_ICSC_paper/#net.sansa_stack.ml.spark.similarity.experiment.SimilarityPipelineExperiment$)
* [Metagraph Creation](https://sansa-stack.github.io/SANSA-Stack/scaladocs/0.7.1_ICSC_paper/#net.sansa_stack.ml.spark.utils.SimilarityExperimentMetaGraphFactory)

#### Code to Modules:
* [Feature Extractor](https://github.com/SANSA-Stack/SANSA-Stack/blob/develop/sansa-ml/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/utils/FeatureExtractorModel.scala)
* [Similarity Estiamtion Modules](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-ml/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/similarity/similarityEstimationModels)
* [Minmial Examples](https://github.com/SANSA-Stack/SANSA-Stack/blob/develop/sansa-ml/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/similarity/examples/minimalCalls.scala)
* [Evaluation of Experiment Class](https://github.com/SANSA-Stack/SANSA-Stack/blob/develop/sansa-ml/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/similarity/experiment/SimilarityPipelineExperiment.scala)
* [Metagraph Creation](https://github.com/SANSA-Stack/SANSA-Stack/blob/develop/sansa-ml/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/utils/SimilarityExperimentMetaGraphFactory.scala)
