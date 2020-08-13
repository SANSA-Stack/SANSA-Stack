package net.sansa_stack.ml.spark.similarity.examples

import net.sansa_stack.ml.spark.similarity.similarity_measures.{BatetModel, BraunBlanquetModel, DiceModel, JaccardModel, OchiaiModel, SimpsonModel, TverskyModel}
import net.sansa_stack.ml.spark.utils.FeatureExtractorModel
import org.apache.jena.riot.Lang
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import net.sansa_stack.rdf.spark.io._
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, MinHashLSH, MinHashLSHModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.DataTypes

object minimalCalls {
  def main(args: Array[String]): Unit = {

    // setup spark session
    val spark = SparkSession.builder
      .appName(s"MinMal Semantic Similarity Estimation Calls")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // define inputpath if it is not parameter
    val inputPath = "/Users/carstendraschner/GitHub/SANSA-ML/sansa-ml-spark/src/main/resources/movie.nt"

    // read in data as Data`Frame
    val triplesDf: DataFrame = spark.read.rdf(Lang.NTRIPLES)(inputPath)

    triplesDf.show()

    // feature extraction
    val featureExtractorModel = new FeatureExtractorModel()
      .setMode("an")
    val extractedFeaturesDataFrame = featureExtractorModel
      .transform(triplesDf)
      .filter(t => t.getAs[String]("uri").startsWith("m"))
    extractedFeaturesDataFrame.show()

    // count Vectorization
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("extractedFeatures")
      .setOutputCol("vectorizedFeatures")
      .fit(extractedFeaturesDataFrame)
    val tmpCvDf: DataFrame = cvModel.transform(extractedFeaturesDataFrame)
    val isNoneZeroVector = udf({ v: Vector => v.numNonzeros > 0 }, DataTypes.BooleanType)
    val countVectorizedFeaturesDataFrame: DataFrame = tmpCvDf.filter(isNoneZeroVector(col("vectorizedFeatures"))).select("uri", "vectorizedFeatures")
    countVectorizedFeaturesDataFrame.show()
    countVectorizedFeaturesDataFrame.filter(col("uri").startsWith("m")).show()

    // similarity Estimations Overview
    // for nearestNeighbors we need one key which is a Vector to search for NN
    val sample_key: Vector = countVectorizedFeaturesDataFrame.take(1)(0).getAs[Vector]("vectorizedFeatures")

    // minHash similarity estimation
    val minHashModel: MinHashLSHModel = new MinHashLSH()
      .setInputCol("vectorizedFeatures")
      .setOutputCol("hashedFeatures")
      .fit(countVectorizedFeaturesDataFrame)
    minHashModel.approxNearestNeighbors(countVectorizedFeaturesDataFrame, sample_key, 10, "minHashDistance").show()
    minHashModel.approxSimilarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, 0.8, "distance").show()

    // Jaccard similarity
    val jaccardModel: JaccardModel = new JaccardModel()
     .setInputCol("vectorizedFeatures")
    jaccardModel.nearestNeighbors(countVectorizedFeaturesDataFrame, sample_key, 10).show()
    jaccardModel.similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, threshold = 0.5).show()

    // Batet Distance
    val batetModel: BatetModel = new BatetModel()
      .setInputCol("vectorizedFeatures")
    batetModel.nearestNeighbors(countVectorizedFeaturesDataFrame, sample_key, 10).show()
    batetModel.similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, threshold = 0.5).show()

    // Braun Blanquet Similarity
    val braunBlanquetModel: BraunBlanquetModel = new BraunBlanquetModel()
      .setInputCol("vectorizedFeatures")
    braunBlanquetModel.nearestNeighbors(countVectorizedFeaturesDataFrame, sample_key, 10).show()
    braunBlanquetModel.similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, threshold = 0.5).show()

    // Dice Similarity
    val diceModel: DiceModel = new DiceModel()
      .setInputCol("vectorizedFeatures")
    diceModel.nearestNeighbors(countVectorizedFeaturesDataFrame, sample_key, 10).show()
    diceModel.similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, threshold = 0.5).show()

    // Ochiai Similarity
    val ochiaiModel: OchiaiModel = new OchiaiModel()
      .setInputCol("vectorizedFeatures")
    ochiaiModel.nearestNeighbors(countVectorizedFeaturesDataFrame, sample_key, 10).show()
    ochiaiModel.similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, threshold = 0.5).show()

    // Simpson Similarity
    val simpsonModel: SimpsonModel = new SimpsonModel()
      .setInputCol("vectorizedFeatures")
    simpsonModel.nearestNeighbors(countVectorizedFeaturesDataFrame, sample_key, 10).show()
    simpsonModel.similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, threshold = 0.5).show()

    // Tversky Similarity
    val tverskyModel: TverskyModel = new TverskyModel()
      .setInputCol("vectorizedFeatures")
      .setAlpha(1.0)
      .setBeta(1.0)
    tverskyModel.nearestNeighbors(countVectorizedFeaturesDataFrame, sample_key, 10).show()
    tverskyModel.similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, threshold = 0.5).show()

    // Similarity Estimation on Subset (e.g. Movie)
    // but maybe for sure we might only search for movie similaritties. so we would reduce dataframe as input
    // first we take only all movie uris
    val tmpDf = countVectorizedFeaturesDataFrame.filter(col("uri").startsWith("m"))
    // then we select one key of type movie
    val tmpK = tmpDf.take(1)(0).getAs[Vector]("vectorizedFeatures")
    minHashModel.approxNearestNeighbors(tmpDf, tmpK, 10, "minHashDistance").show()
    minHashModel.approxSimilarityJoin(tmpDf, tmpDf, 0.8, "distance").show()

    // Stacking of Approaches (MinHash + Jaccard)
    // this is interesting if we want to have precise resulting similarities but a more scalable approach.
    // for this purpose we first calculate similarity over the scalable minhash
    // and then we use the calculated candidates as filter for the initial dataset to reduce once again the dataframe.
    // then we calculate by common jaccard approach or others
    val minHashedSimilarities = minHashModel
      .approxSimilarityJoin(tmpDf, tmpDf, 0.8, "distance")
      .withColumn("uriA", col("datasetA").getField("uri"))
      .withColumn("uriB", col("datasetB").getField("uri"))
      .select("uriA", "uriB", "distance")
    val uriCandidates = (minHashedSimilarities.select("uriA").rdd.map(r => r(0)).collect().toSet.union(minHashedSimilarities.select("uriB").rdd.map(r => r(0)).collect().toSet)).toList
    val dfGoodCandidatesDf = tmpDf.filter(col("uri").isInCollection(uriCandidates))
    println(countVectorizedFeaturesDataFrame.count(), dfGoodCandidatesDf.count())
    jaccardModel.similarityJoin(dfGoodCandidatesDf, dfGoodCandidatesDf, threshold = 0.5).show()
  }
}
