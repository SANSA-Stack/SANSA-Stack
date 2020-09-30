package net.sansa_stack.ml.spark.similarity.run

import java.io.File
import java.util.Calendar

import com.typesafe.config.ConfigFactory
import net.sansa_stack.ml.spark.similarity.similarityEstimationModels.{BatetModel, BraunBlanquetModel, DiceModel, GenericSimilarityEstimatorModel, JaccardModel, MinHashModel, OchiaiModel, SimpsonModel, TverskyModel}
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

    // start spark session
    val spark = SparkSession.builder
      .appName(s"JaccardSimilarityEvaluation")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // we have two options. you can simply hand over only the path to the file or you give a config
    val in = args(0)
    if (in.endsWith(".nt")) {
      run(
        spark = spark,
        inputPath = in,
        similarityEstimator = "Jaccard",
        parametersFeatureExtractorMode = "at",
        parameterCountVectorizerMinDf = 1,
        parameterCountVectorizerMaxVocabSize = 100000,
        parameterSimilarityAlpha = 1.0,
        parameterSimilarityBeta = 1.0,
        parameterNumHashTables = 1,
        parameterSimilarityAllPairThreshold = 0.5,
        parameterSimilarityNearestNeighborsK = 20,
        parameterThresholdMinSimilarity = 0.5
      )
      spark.stop()

    }
    else if (in.endsWith(".conf")) {
      val config = ConfigFactory.parseFile(new File(in))
      run(
        spark = spark,
        inputPath = config.getString("inputPath"),
        similarityEstimator = config.getString("similarityEstimator"),
        parametersFeatureExtractorMode = config.getString("parametersFeatureExtractorMode"),
        parameterCountVectorizerMinDf = config.getInt("parameterCountVectorizerMinDf"),
        parameterCountVectorizerMaxVocabSize = config.getInt("parameterCountVectorizerMaxVocabSize"),
        parameterSimilarityAlpha = config.getDouble("parameterSimilarityAlpha"),
        parameterSimilarityBeta = config.getDouble("parameterSimilarityBeta"),
        parameterNumHashTables = config.getInt("parameterNumHashTables"),
        parameterSimilarityAllPairThreshold = config.getDouble("parameterSimilarityAllPairThreshold"),
        parameterSimilarityNearestNeighborsK = config.getInt("parameterSimilarityNearestNeighborsK"),
        parameterThresholdMinSimilarity = config.getDouble("parameterThresholdMinSimilarity")
      )
      spark.stop()

    }
    else {
      throw new Exception("You have to provide either a nt triple file or a conf specifying more parameters")
    }
  }

  //noinspection ScalaStyle
  def run(
     spark: SparkSession,
     inputPath: String,
     similarityEstimator: String,
     parametersFeatureExtractorMode: String,
     parameterCountVectorizerMinDf: Int,
     parameterCountVectorizerMaxVocabSize: Int,
     parameterSimilarityAlpha: Double,
     parameterSimilarityBeta: Double,
     parameterNumHashTables: Int,
     parameterSimilarityAllPairThreshold: Double,
     parameterSimilarityNearestNeighborsK: Int,
     parameterThresholdMinSimilarity: Double
         ): Unit = {

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
    // triplesDf.show()

    // feature extraction
    val featureExtractorModel = new FeatureExtractorModel()
      .setMode("an")
    val extractedFeaturesDataFrame = featureExtractorModel
      .transform(triplesDf)
      .filter(t => t.getAs[String]("uri").startsWith("m"))
    // extractedFeaturesDataFrame.show()

    // filter for relevant URIs e.g. only movies
    val filteredFeaturesDataFrame = extractedFeaturesDataFrame.filter(t => t.getAs[String]("uri").startsWith("m"))
    // filteredFeaturesDataFrame.show()

    // count Vectorization
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("extractedFeatures")
      .setOutputCol("vectorizedFeatures")
      .fit(filteredFeaturesDataFrame)
    val tmpCvDf: DataFrame = cvModel.transform(filteredFeaturesDataFrame)
    val isNoneZeroVector = udf({ v: Vector => v.numNonzeros > 0 }, DataTypes.BooleanType)
    val countVectorizedFeaturesDataFrame: DataFrame = tmpCvDf.filter(isNoneZeroVector(col("vectorizedFeatures"))).select("uri", "vectorizedFeatures")
    // countVectorizedFeaturesDataFrame.show()

    // similarity Estimations Overview
    // for nearestNeighbors we need one key which is a Vector to search for NN
    // val sample_key: Vector = countVectorizedFeaturesDataFrame.take(1)(0).getAs[Vector]("vectorizedFeatures")

    val model: GenericSimilarityEstimatorModel = similarityEstimator match {
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
      .similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, 0.5, "distCol")

    val metaGraphFactory = new SimilarityExperimentMetaGraphFactory()
    val metagraph: RDD[graph.Triple] = metaGraphFactory.createRdfOutput(
      outputDataset = outputDf1)(
      modelInformationEstimatorName = model.estimatorName, modelInformationEstimatorType = model.modelType, modelInformationMeasurementType = model.estimatorMeasureType)(
      inputDatasetNumbertOfTriples = triplesDf.count(), dataSetInformationFilePath = inputPath)

    metagraph.coalesce(1, shuffle = true).saveAsNTriplesFile("rdfMetagraphSimilarityEstimation.nt")

  }
}
