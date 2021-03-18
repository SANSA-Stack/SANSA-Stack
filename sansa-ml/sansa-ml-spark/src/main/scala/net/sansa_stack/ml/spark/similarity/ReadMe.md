# ReadMe 
Distributed Semantic Similarity Estimation on RDF Knowledge Graphs

This package provides models for semantic similarity estimation.

DistrSim is the Scalable distributed in-Memory Semantic Similarity Estimation for RDF Knowledge Graph Frameworks which has been integrated into the SANSA stack in the SANSA Machine Learning package.

## Documentation
### DistSim 
the documentation in scaladocs are available [here](https://sansa-stack.github.io/SANSA-Stack/scaladocs/0.7.1_ICSC_paper/#package)
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

## Usage of Modules
How to use Semantic Similarity Pipeline Modules:

First, start up you spark session:

```
val spark = SparkSession.builder
       .appName(s"Minimal Semantic Similarity Estimation Calls")
       .master("local[*]")
       .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
       .getOrCreate()
```
       
### Get feature Dataframe Representation
              
Specify the RDF file path:

```
val inputPath = "/GitHub/SANSA-ML/sansa-ml-spark/src/main/resources/movie.nt"
```

To read in the data as a dataframe you use:

```
val triplesDf: DataFrame = spark.read.rdf(Lang.NTRIPLES)(inputPath)
```

Extract the features for each URI:

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
       
### Semantic Similarity Estimations
now the data is prepared to run Semantic SImilarity Estimations.

We have always two options. 

Option 1:

nearestNeighbors provides for one feature vector and a dataframe the k nearest neighbors in the DataFrame to the key feature vector.
a feature vector as key could be: `val sample_key: Vector = countVectorizedFeaturesDataFrame.take(1)(0).getAs[Vector]("vectorizedFeatures")`

Option 2:

similarityJoin calculates for two DataFrames of feature vectors all pairs of similarity. THis dataframe the is limited by a minimal threshold.

Currently we provide these similarity estimation models:
batet, braun-blanquet, dice, jaccard, minHash(probabilistic jaccard) ochiai, simpson, tversky

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
