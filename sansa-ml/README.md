# SANSA ML Readme
The SANSA ML stack is currently under major refactoring.
It steers to a support of Scala 2.12 and Spark 3.
The functionalities are covered by Scala unit tests and are documented within Scaladoc. The Readme provides
* [Code snippets as usage description of currently available modules](#current-modules)
* [Roadmap](#module-roadmap)
* [Recent Experimental Research Implementations](#research-and-experimental-projects)
* [How to Contribute](#how-to-contribute)

## Current Modules
The current stack provides:
- [Sparql Transformer](#sparql-transformer)
- [RDF2Feature - AutoSparql Generation for Feature Extraction](#rdf2feature-autosparql-generation-for-feature-extraction)
- [Feature Based Semantic Similarity Estimations](#feature-based-semantic-similarity-estimations) for further description checkout this [ReadMe](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-ml/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/similarity/ReadMe.md) or take a look into [minimal examples](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-ml/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/similarity/examples/MinimalCalls.scala).

### Sparql Transformer
[Sparql Transformer](https://sansa-stack.github.io/SANSA-Stack/scaladocs/0.8.0/net/sansa_stack/ml/spark/utils/SPARQLQuery.html):
The SPARQL Transformer is implemented as a [Spark MLlib Transformer](https://spark.apache.org/docs/latest/ml-pipeline.html#transformers). It reads RDF data as a `DataSet` and produces a `DataFrame` of type Apache Jena `Node`. Currently supported are up to 5 projection variables. A sample usage could be:
```Scala
val spark = SparkSession.builder()
    .appName(sc.appName)
    .master(sc.master)
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryo.registrator", "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator")
    .config("spark.sql.crossJoin.enabled", "true")
    .getOrCreate()

private val dataPath = this.getClass.getClassLoader.getResource("utils/test_data.nt").getPath
val data = spark.read.rdf(Lang.NTRIPLES)(dataPath).toDS()

val sparqlQueryString =
    "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
    "PREFIX owl: <http://www.w3.org/2002/07/owl#> " +
    "SELECT ?s " +
    "WHERE {" +
    "  ?s rdf:type owl:ObjectProperty " +
    "}"
val sparqlQuery: SPARQLQuery = SPARQLQuery(sparqlQueryString)

val res: DataFrame = sparqlQuery.transform(data)
val resultNodes: Array[Node] = res.as[Node].collect()
```
this sample is taken from a [Scala unit test](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-ml/sansa-ml-spark/src/test/scala/net/sansa_stack/ml/spark/utils/SPARQLQueryTest.scala)

### RDF2Feature AutoSparql Generation for Feature Extraction
[AutoSparql Generation for Feature Extraction](https://sansa-stack.github.io/SANSA-Stack/scaladocs/0.8.0/net/sansa_stack/ml/spark/utils/FeatureExtractingSparqlGenerator$.html):
This module [(scaladocs)](https://sansa-stack.github.io/SANSA-Stack/scaladocs/0.8.0/net/sansa_stack/ml/spark/utils/FeatureExtractingSparqlGenerator$.html) creates a SPARQL query traversing the tree to gain literals which can be used as features for common feature based Machine Learning Approaches. The user needs only to specify the WHERE clause, how to reach the entities, which should be considered as seeds/roots for graph traversal. This traversal will then provide a SPARQL Query to fetch connected features from Literals. As sample usage would be:
```
val inputFilePath: String = this.getClass.getClassLoader.getResource("utils/test.ttl").getPath
val seedVarName = "?seed"
val whereClauseForSeed = "?seed a <http://dig.isi.edu/Person>"
val maxUp: Int = 5
val maxDown: Int = 5
val seedNumber: Int = 0
val seedNumberAsRatio: Double = 1.0

// setup spark session
val spark = SparkSession.builder
  .appName(s"tryout sparql query transformer")
  .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  .config("spark.sql.crossJoin.enabled", true)
  .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

implicit val nodeTupleEncoder = Encoders.kryo(classOf[(Node, Node, Node)])

// first mini file:
val df = spark.read.rdf(Lang.TURTLE)(inputFilePath)

val (totalSparqlQuery: String, var_names: List[String]) = autoPrepo(
  df = df,
  seedVarName = seedVarName,
  seedWhereClause = whereClauseForSeed,
  maxUp = maxUp,
  maxDown = maxDown,
  numberSeeds = seedNumber,
  ratioNumberSeeds = seedNumberAsRatio
)
println(totalSparqlQuery)
```
This sample is taken from [scala unit test](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-ml/sansa-ml-spark/src/test/scala/net/sansa_stack/ml/spark/utils/FeatureExtractingSparqlGeneratorTest.scala)

### Feature Based Semantic Similarity Estimations
[DistSim - Feature Based Semantic Similarity Estimations (code)](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-ml/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/similarity):
DistSim is the scalable distributed in-memory Semantic Similarity Estimation for RDF Knowledge Graph Frameworks which has been integrated into the SANSA stack in the SANSA Machine Learning package. The Scaladoc is available [here](https://sansa-stack.github.io/SANSA-Stack/scaladocs/0.7.1_ICSC_paper/#package), the respective similarity estimation models are in this [Github directory](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-ml/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/similarity) and further needed utils can be found [here](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-ml/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/utils)

#### ScalaDocs:
* [Feature Extractor](https://sansa-stack.github.io/SANSA-Stack/scaladocs/0.8.0/net/sansa_stack/ml/spark/utils/FeatureExtractorModel.html)
* [Similarity Estiamtion Modules](https://sansa-stack.github.io/SANSA-Stack/scaladocs/0.8.0/net/sansa_stack/ml/spark/similarity/similarityEstimationModels/index.html)
* [Evaluation of Experiment Class](hhttps://sansa-stack.github.io/SANSA-Stack/scaladocs/0.8.0/net/sansa_stack/ml/spark/similarity/experiment/SimilarityPipelineExperiment$.html)
* [Metagraph Creation](https://sansa-stack.github.io/SANSA-Stack/scaladocs/0.8.0/net/sansa_stack/ml/spark/utils/SimilarityExperimentMetaGraphFactory.html)

#### Usage of Modules
**Feature Extraction**
How to use Semantic Similarity Pipeline Modules:
```
val featureExtractorModel = new FeatureExtractorModel()
       .setMode("an")
val extractedFeaturesDataFrame = featureExtractorModel
       .transform(triplesDf)
       .filter(t => t.getAs[String]("uri").startsWith("m"))
extractedFeaturesDataFrame.show()
```
Transform features to indexed feature representation:
```
val cvModel: CountVectorizerModel = new CountVectorizer()
        .setInputCol("extractedFeatures")
        .setOutputCol("vectorizedFeatures")
        .fit(filteredFeaturesDataFrame)
val tmpCvDf: DataFrame = cvModel.transform(filteredFeaturesDataFrame)
```

(optional but recommended) filter out feature vectors which does not contain any feature

```
val isNoneZeroVector = udf({ v: Vector => v.numNonzeros > 0 }, DataTypes.BooleanType)
       val countVectorizedFeaturesDataFrame: DataFrame = tmpCvDf.filter(isNoneZeroVector(col("vectorizedFeatures"))).select("uri", "vectorizedFeatures")
       countVectorizedFeaturesDataFrame.show()
```

**Semantic Similarity Estimations**

now the data is prepared to run Semantic Similarity Estimations.

We have always two options.
* Option 1:
  * nearestNeighbors provides for one feature vector and a Dataframe the k nearest neighbors in the DataFrame to the key feature vector. a feature vector as key could be: `val sample_key: Vector = countVectorizedFeaturesDataFrame.take(1)(0).getAs[Vector]("vectorizedFeatures")`
* Option 2:
  * similarityJoin calculates for two DataFrames of feature vectors all pairs of similarity. This DataFrame the is limited by a minimal threshold.

Currently, we provide these similarity estimation models:
* Batet
* Braun-Blanquet
* Dice
* Jaccard
* MinHash(probabilistic Jaccard)
* Ochiai
* Simpson
* Tversky

**Usage of MinHash**
```
val minHashModel: MinHashLSHModel = new MinHashLSH()
      .setInputCol("vectorizedFeatures")
      .setOutputCol("hashedFeatures")
      .fit(countVectorizedFeaturesDataFrame)
minHashModel.approxNearestNeighbors(countVectorizedFeaturesDataFrame, sample_key, 10, "minHashDistance").show()
minHashModel.approxSimilarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, 0.8, "distance").show()
```

**Usage of Jaccard**
```
val jaccardModel: JaccardModel = new JaccardModel()
      .setInputCol("vectorizedFeatures")
     jaccardModel.nearestNeighbors(countVectorizedFeaturesDataFrame, sample_key, 10).show()
     jaccardModel.similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, threshold = 0.5).show()
```

**Usage of Tversky**
```
val tverskyModel: TverskyModel = new TverskyModel()
       .setInputCol("vectorizedFeatures")
       .setAlpha(1.0)
       .setBeta(1.0)
tverskyModel.nearestNeighbors(countVectorizedFeaturesDataFrame, sample_key, 10).show()
tverskyModel.similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, threshold = 0.5).show()
```

## Module Roadmap
* Generic Feature Extractor Pipeline
* Domain Aware Semantic Similarity Estimation

Several further algorithms are in development. Please create a pull request and/or contact [Jens Lehmann](http://jens-lehmann.org) if you are interested in contributing algorithms to SANSA-ML.

## Research and Experimental Projects
In recent research projects further experimental approaches have been implemented.
Due to the ongoing refactoring and re-design of Data Analytics Functionality, these methods are available in the [Release 0.7.1](https://github.com/SANSA-Stack/Archived-SANSA-ML/tree/master/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark) [Machine Learning Layer](https://github.com/SANSA-Stack/Archived-SANSA-ML).
They are currently not maintained but can be used as inspiration for further developments.
The developed approaches cover:
- [Classification (Spark)](https://github.com/SANSA-Stack/Archived-SANSA-ML/tree/master/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/classification) [(Flink)](https://github.com/SANSA-Stack/Archived-SANSA-ML/blob/master/sansa-ml-flink/src/main/scala/net/sansa_stack/ml/flink/clustering/RDFByModularityClustering.scala)
- [Clustering](https://github.com/SANSA-Stack/Archived-SANSA-ML/tree/master/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/clustering)
  - [Paper](http://jens-lehmann.org/files/2019/eswc_pd_poi_clustering.pdf) Clustering Pipelines of large RDF POI Data by Rajjat Dadwal1, Damien Graux, Gezim Sejdiu, Hajira Jabeen, and Jens Lehmann
  - Masterthesis, Distributed RDF Clustering Framework, Tina Boroukhian
- [Kernel](https://github.com/SANSA-Stack/Archived-SANSA-ML/tree/master/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/kernel)
- [Kge/Linkprediction](https://github.com/SANSA-Stack/Archived-SANSA-ML/tree/master/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/kge/linkprediction)
- [Mining/AmieSpark](https://github.com/SANSA-Stack/Archived-SANSA-ML/tree/master/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/mining/amieSpark)
  - Bachelorthesis, Association Rule Mining of Linked Data Using Apache Spark by Theresa Nathan
- [Outliers/Anomaly Detection](https://github.com/SANSA-Stack/Archived-SANSA-ML/tree/master/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/outliers)
  - [Paper](http://jens-lehmann.org/files/2018/ekaw_conod.pdf), Divided we stand out! Forging Cohorts fOr Numeric Outlier Detection in large scale knowledge graphs (CONOD) by Hajira Jabeen 1, Rajjat Dadwal, Gezim Sejdiu, and Jens Lehmann
  - Masterthesis, Scalable Numerical Outlier Detection in Knowledge Graphs, Rajjat Dadwal
- [WordNetDistance](https://github.com/SANSA-Stack/Archived-SANSA-ML/tree/master/sansa-ml-common/src/main/scala/net/sansa_stack/ml/common/nlp/wordnet)

Some further Usage examples of these modules are available in the [archived SANSA_Examples Repository](https://github.com/SANSA-Stack/Archived-SANSA-Examples/tree/master/sansa-examples-spark/src/main/scala/net/sansa_stack/examples/spark/ml)

## How to Contribute
We always welcome new contributors to the project! Please see [our contribution guide](http://sansa-stack.net/contributing-to-sansa/) for more details on how to get started contributing to SANSA.
