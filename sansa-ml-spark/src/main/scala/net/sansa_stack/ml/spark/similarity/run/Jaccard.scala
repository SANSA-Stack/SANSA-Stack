package net.sansa_stack.ml.spark.similarity.run

import java.io.File
import java.util.Calendar

import com.typesafe.config.ConfigFactory
import net.sansa_stack.ml.spark.similarity.similarityEstimationModels.JaccardModel
import net.sansa_stack.ml.spark.utils.{FeatureExtractorModel, SimilarityExperimentMetaGraphFactory}
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph
import org.apache.jena.riot.Lang
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, MinHashLSH, MinHashLSHModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, SparkSession}

object Jaccard {
  def main(args: Array[String]): Unit = {

    // start spark session
    val spark = SparkSession.builder
      .appName(s"JaccardSimilarityEvaluation")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // we have two options. you can simply hand over only the path to the file or you give a config
    val in = args(0)
    val out = args(1)
    if (in.endsWith(".nt")) {
      run(
        spark = spark,
        inputPath = in,
        parametersFeatureExtractorMode = "at",
        parameterCountVectorizerMinDf = 1,
        parameterCountVectorizerMaxVocabSize = 100000,
        parameterSimilarityAlpha = 1.0,
        parameterSimilarityBeta = 1.0,
        parameterNumHashTables = 1,
        parameterSimilarityAllPairThreshold = 0.5,
        parameterSimilarityNearestNeighborsK = 20,
        parameterThresholdMinSimilarity = 0.5,
        out
      )
      spark.stop()

    }
    else if (in.endsWith(".conf")) {
      val config = ConfigFactory.parseFile(new File(in))
      run(
        spark = spark,
        inputPath = config.getString("inputPath"),
        parametersFeatureExtractorMode = config.getString("parametersFeatureExtractorMode"),
        parameterCountVectorizerMinDf = config.getInt("parameterCountVectorizerMinDf"),
        parameterCountVectorizerMaxVocabSize = config.getInt("parameterCountVectorizerMaxVocabSize"),
        parameterSimilarityAlpha = config.getDouble("parameterSimilarityAlpha"),
        parameterSimilarityBeta = config.getDouble("parameterSimilarityBeta"),
        parameterNumHashTables = config.getInt("parameterNumHashTables"),
        parameterSimilarityAllPairThreshold = config.getDouble("parameterSimilarityAllPairThreshold"),
        parameterSimilarityNearestNeighborsK = config.getInt("parameterSimilarityNearestNeighborsK"),
        parameterThresholdMinSimilarity = config.getDouble("parameterThresholdMinSimilarity"),
        out
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
     parametersFeatureExtractorMode: String,
     parameterCountVectorizerMinDf: Int,
     parameterCountVectorizerMaxVocabSize: Int,
     parameterSimilarityAlpha: Double,
     parameterSimilarityBeta: Double,
     parameterNumHashTables: Int,
     parameterSimilarityAllPairThreshold: Double,
     parameterSimilarityNearestNeighborsK: Int,
     parameterThresholdMinSimilarity: Double,
     outputFilePath: String
         ): Unit = {


    // metagraph creator
    // Strings for relation names, maybe this can be later defined in an onthology and only be imported here
    val metagraphElementRelation: String = "element"
    val metagraphValueRelation: String = "value"
    val metagraphExperimentTypeRelation: String = "experiment_type"
    val metagraphExperimentNameRelation: String = "experiment_name"
    val metagraphExperimentMeasurementTypeRelation: String = "experiment_measurement_type"
    val metagraphExperimentDatetimeRelation: String = "experiment_datetime"
    // Strings for uris and literals
    val metagraphExperimentName: String = "Jaccard" // TODO this will be got from the model itself because iformation is stored is there
    val metagraphExperimentType: String = "Sematic Similarity Estimation"
    val metagraphExperimentMeasurementType: String = "distance" // TODO this will be got from the model itself because iformation is stored is there

    // metagraph store parameters
    val output = "/Users/carstendraschner/Downloads/experiment_results"

    // read in data as Data`Frame
    val triplesDf: DataFrame = spark.read.rdf(Lang.NTRIPLES)(inputPath)

    // feature extraction
    val featureExtractorModel = new FeatureExtractorModel()
    val extractedFeaturesDataFrame = featureExtractorModel.transform(triplesDf)

    // count Vectorization
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("extractedFeatures")
      .setOutputCol("vectorizedFeatures")
      .fit(extractedFeaturesDataFrame)
    val tmpCvDf: DataFrame = cvModel.transform(extractedFeaturesDataFrame)
    val isNoneZeroVector = udf({ v: Vector => v.numNonzeros > 0 }, DataTypes.BooleanType)
    val countVectorizedFeaturesDataFrame: DataFrame = tmpCvDf.filter(isNoneZeroVector(col("vectorizedFeatures"))).select("uri", "vectorizedFeatures")

   // Jaccard similarity
    val model: JaccardModel = new JaccardModel()
      .setInputCol("vectorizedFeatures")
    val allPairSimilarity = model.similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, threshold = 0.1)

    allPairSimilarity.show()


    // Metagraph creation

    /*
    val similarity_metagraph_creator = new SimilarityExperimentMetaGraphFactory()
    val experiment_metagraph = similarity_metagraph_creator.transform(
      allPairSimilarity
    )(
      metagraphExperimentName,
      metagraphExperimentType,
      metagraphExperimentMeasurementType
    )(
      metagraphElementRelation,
      metagraphValueRelation,
      metagraphExperimentTypeRelation,
      metagraphExperimentNameRelation,
      metagraphExperimentMeasurementTypeRelation,
      metagraphExperimentDatetimeRelation)

     */

    val metaGraphFactory = new SimilarityExperimentMetaGraphFactory()
    val metagraph: RDD[graph.Triple] = metaGraphFactory.createRdfOutput(
      outputDataset = allPairSimilarity)(
      modelInformationEstimatorName = model.estimatorName, modelInformationEstimatorType = model.modelType, modelInformationMeasurementType = model.estimatorMeasureType)(
      inputDatasetNumbertOfTriples = triplesDf.count(), dataSetInformationFilePath = inputPath)
    metagraph.foreach(println(_))

    // Store metagraph over sansa rdf layer
    // dt to enforce different outputstrings so no conflicts occur
    val dt = Calendar.getInstance().getTime()
      .toString // make string out of it, in future would be better to allow date nativly in rdf
      .replaceAll("\\s", "") // remove spaces to reduce confusions with some foreign file readers
      .replaceAll(":", "")
    metagraph.coalesce(1, shuffle = true).saveAsNTriplesFile(output + dt)
  }
}
