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
- [SmartFeatureExtractor](#smartfeatureextractor)
- [Literal2Feature - AutoSparql Generation for Feature Extraction](#literal2feature-autosparql-generation-for-feature-extraction)
- [SparqlFrame Feature Extractor](#sparqlframe-feature-extractor)
- [Smart Vector Assembler](#smart-vector-assembler)
- [ML2Graph](#ml2graph)
- [SimE4KG - Similarity Estimation for Knowledge Graphs](#sime4kg-transformer)
- [Feature Based Semantic Similarity Estimations](#feature-based-semantic-similarity-estimations) for further description checkout this [ReadMe](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-ml/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/similarity/ReadMe.md) or take a look into [minimal examples](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-ml/sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/similarity/examples/MinimalCalls.scala).
- [Sparql Transformer](#sparql-transformer)
- [DistAD](https://github.com/SANSA-Stack/SANSA-Stack/tree/feature/distad/sansa-ml#distad-distributed-anomaly-detection)

### SmartFeatureExtractor
This feature extractor creates out of a Apache Spark Dataset of Apache Jena Triple a Dataframe which contains entity feature information for further machine learning approaches.
With an initial entity selecting method, for all the specified URIs all features are collected by the pivot function of Spark.
The resulting feature columns are collapsed s.t. for each entity we have later one row in the dataframe.
If the extracted feature is of type Literal, the column is casted to the assigned literal data type: e.g. StringType, DoubleType, Timestamp, ...
```scala
val dataset: Dataset[graph.Triple] = [...]

/** Smart Feature Extractor */
val sfeNoFilter = new SmartFeatureExtractor()
  // .setEntityColumnName("s")
  // .setObjectFilter("http://data.linkedmdb.org/movie/film")
  // .setSparqlFilter("SELECT ?s WHERE { ?s ?p <http://data.linkedmdb.org/movie/film> }")

/** Feature Extracted DataFrame */
val feDf = sfeNoFilter
  .transform(dataset)
feDf
  .show(false)
```
here is an example with an initial filter by object
```scala
val sfeObjectFilter = new SmartFeatureExtractor()
  // .setEntityColumnName("s")
  .setObjectFilter("http://data.linkedmdb.org/movie/film")

val feDf1 = sfeObjectFilter
  .transform(dataset)
feDf1
  .show(false)
```
and finally an example with an initial sparql filter
```scala
val sfeSparqlFilter = new SmartFeatureExtractor()
  // .setEntityColumnName("s")
  .setSparqlFilter("SELECT ?s WHERE { ?s ?p <http://data.linkedmdb.org/movie/film> }")

val feDf2 = sfeSparqlFilter
  .transform(dataset)
feDf2
  .show()
```
The results looks like this:
```
+--------------------+--------------------+--------------------+-------+--------------------+---------------------+--------------------+--------------------+
|                   s|               actor|initial_release_date|runtime|              writer|22-rdf-syntax-ns#type|    rdf-schema#label|               genre|
+--------------------+--------------------+--------------------+-------+--------------------+---------------------+--------------------+--------------------+
|https://sansa.sam...|[https://sansa.sa...| 2002-01-01 00:00:00|  141.0|http://data.linke...| http://data.linke...| Catch Me If You Can|[https://sansa.sa...|
|https://sansa.sam...|[https://sansa.sa...| 1994-01-01 00:00:00|  142.0|http://data.linke...| http://data.linke...|The Shawshank Red...|[https://sansa.sa...|
|https://sansa.sam...|[https://sansa.sa...| 1999-01-01 00:00:00|  189.0|http://data.linke...| http://data.linke...|          Green Mile|[https://sansa.sa...|
+--------------------+--------------------+--------------------+-------+--------------------+---------------------+--------------------+--------------------+
```

### Literal2Feature AutoSparql Generation for Feature Extraction
[AutoSparql Generation for Feature Extraction](https://sansa-stack.github.io/SANSA-Stack/scaladocs/0.8.0/net/sansa_stack/ml/spark/utils/FeatureExtractingSparqlGenerator$.html):
This module [(scaladocs)](https://sansa-stack.github.io/SANSA-Stack/scaladocs/0.8.0/net/sansa_stack/ml/spark/utils/FeatureExtractingSparqlGenerator$.html) creates a SPARQL query traversing the tree to gain literals which can be used as features for common feature based Machine Learning Approaches. The user needs only to specify the WHERE clause, how to reach the entities, which should be considered as seeds/roots for graph traversal. This traversal will then provide a SPARQL Query to fetch connected features from Literals. As sample usage would be:
```scala
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
    val dataset: Dataset[org.apache.jena.graph.Triple] = spark.rdf(Lang.TURTLE)(inputFilePath).toDS().cache()


val (totalSparqlQuery: String, var_names: List[String]) = FeatureExtractingSparqlGenerator.createSparql(
  ds = dataset,
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
A sample query creaed and adjsuted by the Literal2Feature module created the following feature extracting SPARQL query for thr Linked Movie Data base dataset:
```
SELECT
?movie
?movie__down_genre__down_film_genre_name
?movie__down_title
(<http://www.w3.org/2001/XMLSchema#int>(?movie__down_runtime) as ?movie__down_runtime_asInt)
?movie__down_runtime
?movie__down_actor__down_actor_name
WHERE {
    ?movie <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://data.linkedmdb.org/movie/film> .
    ?movie <http://data.linkedmdb.org/movie/genre> ?movie__down_genre . ?movie__down_genre <http://data.linkedmdb.org/movie/film_genre_name> ?movie__down_desiredGenre__down_film_genre_name .
    
    OPTIONAL { ?movie <http://purl.org/dc/terms/title> ?movie__down_title . }
    OPTIONAL { ?movie <http://data.linkedmdb.org/movie/runtime> ?movie__down_runtime . }
    OPTIONAL { ?movie <http://data.linkedmdb.org/movie/actor> ?movie__down_actor . ?movie__down_actor <http://data.linkedmdb.org/movie/actor_name> ?movie__down_actor__down_actor_name . }
    OPTIONAL { ?movie <http://data.linkedmdb.org/movie/genre> ?movie__down_genre . ?movie__down_genre <http://data.linkedmdb.org/movie/film_genre_name> ?movie__down_genre__down_film_genre_name . }
    
    FILTER (?movie__down_desiredGenre__down_film_genre_name = 'Superhero' || ?movie__down_desiredGenre__down_film_genre_name = 'Fantasy' )
}
```

### SparqlFrame Feature Extractor
With SparqlFrame we provide a Transformer which takes a String representing a sparql query. You can also use our [Literal2Feature - AutoSparql Generation for Feature Extraction](#literal2feature-autosparql-generation-for-feature-extraction).
It uses SPARQLIFY from query layer to gain query results. THe values are casted to String if not all elements in a repective feature column are of a respective type like Integer.
```scala 
/**
* transformer that collect the features from the Dataset[Triple] to a common spark Dataframe
* collapsed
*/
val sparqlFrame = new SparqlFrame()
    .setSparqlQuery(sparqlString)
    .setCollapsByKey(true) // optional, default is false
    .setCollapsColumnName("seed") // optional

/**
* dataframe with resulting features
* in this collapsed by the movie column
*/
val extractedFeaturesDf = sparqlFrame
.transform(dataset)
.cache()
```
this creates a dataframe e.g. of such a shape
```
+--------------------------+--------------+---------------+
|seed                      |seed__down_age|seed__down_name|
+--------------------------+--------------+---------------+
|http://dig.isi.edu/Mary   |25            |Mary           |
|http://dig.isi.edu/John   |28            |John           |
|http://dig.isi.edu/John_jr|2             |John Jr.       |
+--------------------------+--------------+---------------+
```
this dataframe can then be manipulated by native apache spark mllib transformers for desired scenario or the Smart Vector assembler.

### Smart Vector Assembler
This Transformer creates a needed Dataframe for common ML approaches in Spark MLlib.
The resulting Dataframe consists of a column features which is a numeric vector for each entity
The other columns are the id/identifier column like the node id
And optional column for label
```scala 
/*
 val smartVectorAssembler = new SmartVectorAssembler()
    .setEntityColumn("seed")
    .setLabelColumn("age")
    .setNullReplacement("string", "") // optional
    .setNullReplacement("digit", -1) // optional
    .setWord2VecSize(5) // optional
    .setWord2VecMinCount(1) // optional

val assembledDf: DataFrame = smartVectorAssembler
    .transform(postprocessedFeaturesDf)
```
this creates a dataframe e.g. of such a shape
```
+--------------------------+-----+------------------------+
|id                        |label|features                |
+--------------------------+-----+------------------------+
|http://dig.isi.edu/Mary   |25   |[28.0,-1.0,2.0,0.0,-1.0]|
|http://dig.isi.edu/John   |28   |[25.0,-1.0,1.0,1.0,-1.0]|
|http://dig.isi.edu/John_jr|2    |[-1.0,25.0,0.0,-1.0,1.0]|
|http://dig.isi.edu/John_jr|2    |[-1.0,28.0,0.0,-1.0,0.0]|
+--------------------------+-----+------------------------+
```

### ML2Graph
The module ML2Graph can be used to transform the tabular represenation of ML results into RDF KNowledge Graphs t enrich the initial input KG.

```
// Make predictions.
val predictions = model.transform(testData)

// Select example rows to display.
predictions.select("entityID", "prediction", "label", "features").show(10)
predictions.show()

val ml2Graph = new ML2Graph()
  .setEntityColumn("entityID")
  .setValueColumn("prediction")

val metagraph: RDD[Triple] = ml2Graph.transform(predictions)
metagraph.take(10).foreach(println(_))
```

### SimE4KG Transformer
 This modules provides a sementic similarity estimation Transformer. It needs a dataset of triple as input.
 Then you specify which seeds you want to consider. Either you do this over a sparwl or over the object filter.
 next we reuse the DistSim similarity estimation to gather candidate pairs. 
 Based on this candidate pairs, the multi modal simialrity estimation is calculated.
 The features of this are extracted by the SmartFeatureExtractor.
 ```scala
 val lang = Lang.TURTLE
 val originalDataRDD = spark.rdf(lang)("/Users/.../Datasets/sampleMovieDB.nt").persist()

 val dataset: Dataset[Triple] = originalDataRDD
   .toDS()
   .cache()

 val dse = new DaSimEstimator()
   // .setSparqlFilter("SELECT ?o WHERE { ?s <https://sansa.sample-stack.net/genre> ?o }")
   .setObjectFilter("http://data.linkedmdb.org/movie/film")
   .setDistSimFeatureExtractionMethod("os")
   .setSimilarityValueStreching(false)
   .setImportance(Map("initial_release_date_sim" -> 0.2, "rdf-schema#label_sim" -> 0.0, "runtime_sim" -> 0.2, "writer_sim" -> 0.1, "22-rdf-syntax-ns#type_sim" -> 0.0, "actor_sim" -> 0.3, "genre_sim" -> 0.2))

 val resultSimDf = dse
   .transform(dataset)

 resultSimDf.show(false)

 val metagraph: RDD[Triple] = dse
    .semantification(resultSimDf)
 ```
 Apart of the code snippets here, we also provide a sample [Dataricks Notebook](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/6924783690087984/1243120961280565/8524188481975304/latest.html)

### Sparql Transformer
[Sparql Transformer](https://sansa-stack.github.io/SANSA-Stack/scaladocs/0.8.0/net/sansa_stack/ml/spark/utils/SPARQLQuery.html):
The SPARQL Transformer is implemented as a [Spark MLlib Transformer](https://spark.apache.org/docs/latest/ml-pipeline.html#transformers). It reads RDF data as a `DataSet` and produces a `DataFrame` of type Apache Jena `Node`. Currently supported are up to 5 projection variables. A sample usage could be:
```scala 
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
```scala
val featureExtractorModel = new FeatureExtractorModel()
       .setMode("an")
val extractedFeaturesDataFrame = featureExtractorModel
       .transform(triplesDf)
       .filter(t => t.getAs[String]("uri").startsWith("m"))
extractedFeaturesDataFrame.show()
```
Transform features to indexed feature representation:
```scala
val cvModel: CountVectorizerModel = new CountVectorizer()
        .setInputCol("extractedFeatures")
        .setOutputCol("vectorizedFeatures")
        .fit(filteredFeaturesDataFrame)
val tmpCvDf: DataFrame = cvModel.transform(filteredFeaturesDataFrame)
```

(optional but recommended) filter out feature vectors which does not contain any feature

```scala
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
```scala
val minHashModel: MinHashLSHModel = new MinHashLSH()
      .setInputCol("vectorizedFeatures")
      .setOutputCol("hashedFeatures")
      .fit(countVectorizedFeaturesDataFrame)
minHashModel.approxNearestNeighbors(countVectorizedFeaturesDataFrame, sample_key, 10, "minHashDistance").show()
minHashModel.approxSimilarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, 0.8, "distance").show()
```

**Usage of Jaccard**
```scala
val jaccardModel: JaccardModel = new JaccardModel()
      .setInputCol("vectorizedFeatures")
     jaccardModel.nearestNeighbors(countVectorizedFeaturesDataFrame, sample_key, 10).show()
     jaccardModel.similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, threshold = 0.5).show()
```

**Usage of Tversky**
```scala
val tverskyModel: TverskyModel = new TverskyModel()
       .setInputCol("vectorizedFeatures")
       .setAlpha(1.0)
       .setBeta(1.0)
tverskyModel.nearestNeighbors(countVectorizedFeaturesDataFrame, sample_key, 10).show()
tverskyModel.similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, threshold = 0.5).show()
```

## Module Roadmap
* Domain Aware Semantic Similarity Estimation
* Anomaly Detection
* KGE

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
