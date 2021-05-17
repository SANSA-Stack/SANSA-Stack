# DistRDF2ML - Overview and Examples
Within this ReadMe we introduce the usage of the DistRDF2ML framework as part of the sansa stack. The modules SparqlFrame and SmartVectorAssembler offer easy and geeric creation of Spark (MLlib) driven machine learning pipelines on RDF Knowledge Graphs.

## SANSA DistRDF2ML Examples
We offer pipelines for:
- [Regressions](https://github.com/SANSA-Stack/SANSA-Stack/blob/feature/MmDistSIm/sansa-examples/sansa-examples-spark/src/main/scala/net/sansa_stack/examples/spark/ml/DistRDF2ML/DistRDF2ML_Regression.scala), [DistRDF2ML - Regression - Databricks Notebook](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/6924783690087984/1369407433773997/8524188481975304/latest.html)
- [Classification](https://github.com/SANSA-Stack/SANSA-Stack/blob/feature/MmDistSIm/sansa-examples/sansa-examples-spark/src/main/scala/net/sansa_stack/examples/spark/ml/DistRDF2ML/DistRDF2ML_Classification.scala)
- [Clustering](https://github.com/SANSA-Stack/SANSA-Stack/blob/feature/MmDistSIm/sansa-examples/sansa-examples-spark/src/main/scala/net/sansa_stack/examples/spark/ml/DistRDF2ML/DistRDF2ML_Clustering.scala)

## DistRDF2ML Evaluation
The processing times of modules across different cluster configurations, Sparql Complexities, and other hyperparameter adjustements are evaluated with this [class](https://github.com/SANSA-Stack/SANSA-Stack/blob/feature/MmDistSIm/sansa-examples/sansa-examples-spark/src/main/scala/net/sansa_stack/examples/spark/ml/DistRDF2ML/DistRDF2ML_Evaluation.scala). 

## Sample Data and configuration
To run the samples given above you need to provide a dataset and also set spark master
### Dataset - LMDB
The pipelines make use out of the Linked Movie Database RDF Knowledge Graph which of multimodal features.
It can be downloaded [here](http://www.cs.toronto.edu/~oktie/linkedmdb/linkedmdb-18-05-2009-dump.nt)
### Spark Master
The spark master needs to be set. Within IDE this can be done by VM options:
```
-Dspark.master=local[*]
```

## SANSA-Databricks
In this repository we collect valuable content to bring the SANSA stack to Databricks platform
Within these [slides](https://github.com/SANSA-Stack/SANSA-Databricks/blob/main/SANSA%20through%20Databricks.pdf) you get a step by step tutorial, how-to setup Databricks and SANSA, such that you can use SANSA within you browser