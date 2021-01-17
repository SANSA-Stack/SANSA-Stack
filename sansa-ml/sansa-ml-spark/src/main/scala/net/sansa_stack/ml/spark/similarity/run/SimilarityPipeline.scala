package net.sansa_stack.ml.spark.similarity.run

import net.sansa_stack.ml.spark.similarity.similarityEstimationModels._
import net.sansa_stack.ml.spark.utils.{FeatureExtractorModel, SimilarityExperimentMetaGraphFactory}
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph
import org.apache.jena.riot.Lang
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SimilarityPipeline {
  def main(args: Array[String]): Unit = {

    // we have two options. you can simply hand over only the path to the file or you give a config
    val inputFilePath: String = args(0)
    val outputFilePath: String = args(1)
    val parameterSimilarityEstimator: String = args(2)
    val paramterterFeatureExtractorMode: String = "at"
    val paramterCountVectorizerMinTf: Int = 1
    val paramterCountVectorizerMinDf: Int = 1
    val paramterCountVectorizerVocabSize: Int = 100000
    val paramterSimilarityEstimatorThreshold: Double = 0.5

    // setup spark session
    val spark = SparkSession.builder
      .appName(s"MinMal Semantic Similarity Estimation Calls")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // read in data as Data`Frame
    println("ReadIn file:" + inputFilePath)
    val triplesDf: DataFrame = spark.read.rdf(Lang.NTRIPLES)(inputFilePath)
    // triplesDf.show()

    // feature extraction
    println("Extract features with mode:" + paramterterFeatureExtractorMode)
    val featureExtractorModel = new FeatureExtractorModel()
      .setMode(paramterterFeatureExtractorMode)
    val extractedFeaturesDataFrame = featureExtractorModel
      .transform(triplesDf)
    // extractedFeaturesDataFrame.show()

    // filter for relevant URIs e.g. only movies
    val filteredFeaturesDataFrame = extractedFeaturesDataFrame // .filter(t => t.getAs[String]("uri").startsWith("m"))
    // filteredFeaturesDataFrame.show()

    // count Vectorization
    println("Count Vectorizer")
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("extractedFeatures")
      .setOutputCol("vectorizedFeatures")
      .setMinTF(paramterCountVectorizerMinTf)
      .setMinDF(paramterCountVectorizerMinDf)
      .setVocabSize(paramterCountVectorizerVocabSize)
      .fit(filteredFeaturesDataFrame)
    val tmpCvDf: DataFrame = cvModel.transform(filteredFeaturesDataFrame)
    val isNoneZeroVector = udf({ v: Vector => v.numNonzeros > 0 }, DataTypes.BooleanType)
    val countVectorizedFeaturesDataFrame: DataFrame = tmpCvDf.filter(isNoneZeroVector(col("vectorizedFeatures"))).select("uri", "vectorizedFeatures")
    // countVectorizedFeaturesDataFrame.show()

    // similarity Estimations Overview
    // for nearestNeighbors we need one key which is a Vector to search for NN
    // val sample_key: Vector = countVectorizedFeaturesDataFrame.take(1)(0).getAs[Vector]("vectorizedFeatures")

    println("EstimatorModel setup")
    val model: GenericSimilarityEstimatorModel = parameterSimilarityEstimator match {
      case "Batet" => new BatetModel()
        .setInputCol("vectorizedFeatures")
      case "BraunBlanquet" => new BraunBlanquetModel()
        .setInputCol("vectorizedFeatures")
      case "Dice" => new DiceModel()
        .setInputCol("vectorizedFeatures")
      case "Jaccard" => new JaccardModel()
        .setInputCol("vectorizedFeatures")
      case "MinHash" => new MinHashModel()
        .setInputCol("vectorizedFeatures")
      case "Ochiai" => new OchiaiModel()
        .setInputCol("vectorizedFeatures")
      case "Simpson" => new SimpsonModel()
        .setInputCol("vectorizedFeatures")
      case "Tversky" => new TverskyModel()
        .setInputCol("vectorizedFeatures")
        .setAlpha(1.0)
        .setBeta(1.0)
    }

    val outputDf1: Dataset[_] = model
      .similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, paramterSimilarityEstimatorThreshold, "distCol")

    val metaGraphFactory = new SimilarityExperimentMetaGraphFactory()
    val metagraph: RDD[graph.Triple] = metaGraphFactory.createRdfOutput(
      outputDataset = outputDf1)(
      modelInformationEstimatorName = model.estimatorName, modelInformationEstimatorType = model.modelType, modelInformationMeasurementType = model.estimatorMeasureType)(
      inputDatasetNumbertOfTriples = triplesDf.count(), dataSetInformationFilePath = inputFilePath)

    metagraph.coalesce(1, shuffle = true).saveAsNTriplesFile(outputFilePath)

  }
}
