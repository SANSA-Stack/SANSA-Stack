# ReadMe 
Distributed Semantic Similarity Estimation on RDF Knowledge Graphs

This package provides models for semantic similarity estimation.

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