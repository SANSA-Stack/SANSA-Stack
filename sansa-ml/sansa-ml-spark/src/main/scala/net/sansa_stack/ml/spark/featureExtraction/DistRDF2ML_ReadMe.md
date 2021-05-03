# DistRDF2ML - Overview and Examples
Within this ReadMe we introduce the usage of the DistRDF2ML framework as part of the sansa stack. The modules SparqlFrame and SmartVectorAssembler offer easy and geeric creation of Spark (MLlib) driven machine learning pipelines on RDF Knowledge Graphs.

## SANSA DistRDF2ML Examples
We offer pipelines for:
- [Regressions]()
- [Classification]()
- [Clustering]()

## SANSA-Databricks
In this repository we collect valuable content to bring the SANSA stack to Databricks platform

### Instruction Slides
Within these [slides](https://github.com/SANSA-Stack/SANSA-Databricks/blob/main/SANSA%20through%20Databricks.pdf) you get a step by step tutorial, how-to setup Databricks and SANSA, such that you can use SANSA within you browser

### Spark configuration
int he setup of DAtabricks cluster one has to set the following lines in a Spark setup:
```
spark.databricks.delta.preview.enabled true
spark.serializer org.apache.spark.serializer.KryoSerializer
spark.kryo.registrator net.sansa_stack.rdf.spark.io.JenaKryoRegistrator, net.sansa_stack.query.spark.sparqlify.KryoRegistratorSparqlify
```

### Sansa Jar
Jars are available in the respective Release. Releases are listed [here](https://github.com/SANSA-Stack/SANSA-Stack/releases)

### Sample files
One sample file to process on Machine Learning pipelines is the Linked Movie Database. It can be downloaded [here](http://www.cs.toronto.edu/~oktie/linkedmdb/linkedmdb-18-05-2009-dump.nt)

### Sample Databricks Notebooks
Here we list Databricks notebooks to import for several sample SANSA usages
- [Read In RDF data into SANSA](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/6924783690087984/4016092937227443/8524188481975304/latest.html)
- [DistSim: RDF Similarity Estimation](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/6924783690087984/3848631259312629/8524188481975304/latest.html)