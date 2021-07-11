---
title: Overview
nav_order: 1
---

# Documentation
This module provides the scala doc documentation. The module tree is accessible on the right-hand side.
We also provide ReadMe files in the SANSA-Stack Github project. The Github project is available [here](https://github.com/SANSA-Stack/SANSA-Stack).
## Layers
The SANSA project is structured in the following five layers developed in their respective sub-folders:
* RDF [readme](https://github.com/SANSA-Stack/SANSA-Stack/blob/develop/sansa-rdf/README.md), [package](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-rdf)
* OWL [readme](https://github.com/SANSA-Stack/SANSA-Stack/blob/develop/sansa-owl/README.md), [package](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-owl)
* Query [readme](https://github.com/SANSA-Stack/SANSA-Stack/blob/develop/sansa-query/README.md), [package](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-query)
* Inference [readme](https://github.com/SANSA-Stack/SANSA-Stack/blob/develop/sansa-inference/README.md), [package](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-inference)
* Machine Learning [readme](https://github.com/SANSA-Stack/SANSA-Stack/blob/develop/sansa-ml/README.md), [package](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-ml)


## Paper Specific Documentation
For recently published papers, we provide here a fast entry point to the provided modules. All of those modules are as well accessible over the standard documentation
### DistSim ICSC Paper Documentation
the documentation in docs are available [here](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-ml)
the respective similarity estimation models are in this [github directory](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-ml/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/similarity) and further needed utils are [here](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-ml/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/utils)

#### Code to Modules:
* [Feature Extractor](https://github.com/SANSA-Stack/SANSA-Stack/blob/develop/sansa-ml/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/utils/FeatureExtractorModel.scala)
* [Similarity Estiamtion Modules](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-ml/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/similarity/similarityEstimationModels)
* [Minmial Examples](https://github.com/SANSA-Stack/SANSA-Stack/blob/develop/sansa-ml/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/similarity/examples/minimalCalls.scala)
* [Evaluation of Experiment Class](https://github.com/SANSA-Stack/SANSA-Stack/blob/develop/sansa-ml/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/similarity/experiment/SimilarityPipelineExperiment.scala)
* [Metagraph Creation](https://github.com/SANSA-Stack/SANSA-Stack/blob/develop/sansa-ml/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/utils/SimilarityExperimentMetaGraphFactory.scala)
