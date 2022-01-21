---
parent: ML
title: Publications
has_children: true
nav_order: 2
---
# Paper Specific Documentation
For recently published papers, we provide here a fast entry point to the provided modules. All of those modules are as well accessible over the standard documentation

## SimE4KG: Explainable Distributed multi-modal Semantic Similarity Estimation for Knowledge Graphs

This framework includes all of the most recent developments for the SimE4KG framework.
SimE4KG is the Explainable Distributed In-Memory multi-modal Semantic Similarity Estimation for Knowledge Graphs.

### Overview
In this release we introduce multiple changes to the Sansa Stack to offer the SimE4KG functionalities
The content is structured as follows:
- Release
- Databricks Notebooks
- ReadMe of novel Modules
- Novel Classes
- Unit Tests
- Data Sets
- Further Reading

### Release
The changes are made available within this release [here](https://github.com/SANSA-Stack/SANSA-Stack/releases/tag/v0.8.2.3_SimE4KG)

### SimE4KG Databricks Notebook
To showcase in a hands on session the usage of SimE4KG modules, we introduce multiple Databricks Notebooks. Those show the Full pipeline but also dedicated parts like the SmartFeature Extractor. Within the notebooks you can see the mixture of Explanations, Sample code and the output of the code snippets. With the Notebooks you can reproduce the functionality within you browser without a need to install the Framework locally.
The Notebooks can be found here:
- [SimE4KG Databricks Notebook](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/6924783690087984/1243120961280565/8524188481975304/latest.html) for sample pipeline building including outputs
- [SmartFeatureExtractor Databricks Notebook](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/6924783690087984/3559605473631626/8524188481975304/latest.html) for multi modal feature extraction with the novel Smart Feature Extrator

### ReadMe
The novel modules of SimE4KG are documented within the SANSA ML ReadMe. For quick links especially to the high level SimE4KG Transformer and the SmartFeatureExtractor, you can use these two links:
- [SimE4KG/Dasim Transformer ReadMe](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-ml#sime4kg-transformer) which is the high leveled Similarity Estimation transformer calling entire pipeline
- [SmartFeatureExtractor ReadMe](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-ml#smartfeatureextractor) which is the novel developed generic multi modal feature extractor transformer

### Novel Classes
Novel Classes developed within this release are especially the Dasim Transformer and the SmartFeature extractor but also the corresponding unit test as well as the Evaluation scripts to test module performance:
- DasimTransformer [Class](https://github.com/SANSA-Stack/SANSA-Stack/blob/develop/sansa-ml/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/similarity/similarityEstimationModels/DaSimEstimator.scala) [Unit Test](https://github.com/SANSA-Stack/SANSA-Stack/blob/develop/sansa-ml/sansa-ml-spark/src/test/scala/net/sansa_stack/ml/spark/similarity/DaSimEstimatorTest.scala)
- Smart Feature Extractor [Class](https://github.com/SANSA-Stack/SANSA-Stack/blob/develop/sansa-ml/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/featureExtraction/SmartFeatureExtractor.scala) [Unit Test](https://github.com/SANSA-Stack/SANSA-Stack/blob/develop/sansa-ml/sansa-ml-spark/src/test/scala/net/sansa_stack/ml/spark/featureExtraction/SmartFeatureExtractorTest.scala)
- Evaluation [Classes](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-examples/sansa-examples-spark/src/main/scala/net/sansa_stack/examples/spark/ml/Similarity) like data size scalability, feature availability evaluation, Smartfeature extractor evaluation and many more ...

### Datasets
As starting point to play around with the developments of this framework, we recommend the Linked Movie Data Base RDF Knowledge Graph. This KG represents in millions of triples data about movies and consists of multi modal features like lists of URIs as the lists of actors, numeric features like the runtime but also timestamp data like the release date. For purposes of Unit test, we propose also an extract of this data which follow the same schema.
- [LMDB](https://www.cs.toronto.edu/~oktie/linkedmdb/linkedmdb-18-05-2009-dump.nt)
- [small LMDB](https://github.com/SANSA-Stack/SANSA-Stack/blob/develop/sansa-ml/sansa-ml-spark/src/test/resources/similarity/sampleMovieDB.nt)

### Further Reading
If you are interested into further reading and background information of other related modules we recommend the following papers:
- [Distributed semantic analytics using the SANSA stack](https://link.springer.com/chapter/10.1007/978-3-319-68204-4_15)
- [Sparklify: A Scalable Software Component for Efficient Evaluation of SPARQL Queries over Distributed RDF Datasets](https://link.springer.com/chapter/10.1007/978-3-030-30796-7_19)
- [DistSim - Scalable Distributed in-Memory Semantic Similarity Estimation for RDF Knowledge Graphs](https://ieeexplore.ieee.org/abstract/document/9364473)
- [DistRDF2ML - Scalable Distributed In-Memory Machine Learning Pipelines for RDF Knowledge Graphs](https://dl.acm.org/doi/abs/10.1145/3459637.3481999)

### Other
- In addition, we provide the full jar of this version below


## DistRDF2ML

### Release
The changes are made available within this release [here](https://github.com/SANSA-Stack/SANSA-Stack/releases/tag/v0.8.1_DistRDF2ML)

### Docs
The documentation with sample code snippets are available within the [SANSA ML Readme](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-ml) which include:
* [Literal2Feature - AutoSparql Generation for Feature Extraction](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-ml#literal2feature-autosparql-generation-for-feature-extraction)
* [SparqlFrame Feature Extractor](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-ml#sparqlframe-feature-extractor)
* [Smart Vector Assembler](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-ml#smart-vector-assembler)
* [ML2Graph](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-ml#ml2graph)
* [DistAD](https://github.com/SANSA-Stack/SANSA-Stack/blob/feature/distad/sansa-ml/README.md#distad-distributed-anomaly-detection)

### Code to Modules:
This release majorly provides the modules:
* [SparqlFrame](https://github.com/SANSA-Stack/SANSA-Stack/blob/develop/sansa-ml/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/featureExtraction/SparqlFrame.scala)
* [SmartVectorAssembler](https://github.com/SANSA-Stack/SANSA-Stack/blob/develop/sansa-ml/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/featureExtraction/SmartVectorAssembler.scala)
* [ML2Graph](https://github.com/SANSA-Stack/SANSA-Stack/blob/develop/sansa-ml/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/utils/ML2Graph.scala)


## DistSim ICSC Paper Documentation
the documentation in docs are available [here](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-ml)
the respective similarity estimation models are in this [github directory](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-ml/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/similarity) and further needed utils are [here](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-ml/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/utils)

### Code to Modules:
* [Feature Extractor](https://github.com/SANSA-Stack/SANSA-Stack/blob/develop/sansa-ml/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/utils/FeatureExtractorModel.scala)
* [Similarity Estiamtion Modules](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-ml/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/similarity/similarityEstimationModels)
* [Minmial Examples](https://github.com/SANSA-Stack/SANSA-Stack/blob/develop/sansa-ml/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/similarity/examples/minimalCalls.scala)
* [Evaluation of Experiment Class](https://github.com/SANSA-Stack/SANSA-Stack/blob/develop/sansa-ml/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/similarity/experiment/SimilarityPipelineExperiment.scala)
* [Metagraph Creation](https://github.com/SANSA-Stack/SANSA-Stack/blob/develop/sansa-ml/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/utils/SimilarityExperimentMetaGraphFactory.scala)

## DistAD ICSC Paper Documentation
The documentation in docs are available [here](https://github.com/SANSA-Stack/SANSA-Stack/tree/feature/distad/sansa-ml).
The modules are in this [github directory](https://github.com/SANSA-Stack/SANSA-Stack/tree/feature/distad/sansa-ml/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/anomalydetection).
