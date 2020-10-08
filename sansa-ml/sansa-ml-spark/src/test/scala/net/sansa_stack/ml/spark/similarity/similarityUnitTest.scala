package net.sansa_stack.ml.spark.similarity

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.ml.spark.similarity.similarityEstimationModels.{BatetModel, BraunBlanquetModel, DiceModel, JaccardModel, MinHashModel, OchiaiModel, SimpsonModel, TverskyModel}
import net.sansa_stack.ml.spark.utils.{FeatureExtractorModel, SimilarityExperimentMetaGraphFactory}
import org.apache.jena.riot.Lang
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.FunSuite
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, MinHashLSH, MinHashLSHModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.DataTypes

class similarityUnitTest extends FunSuite with DataFrameSuiteBase {

  // define inputpath if it is not parameter
  val inputPath = "src/test/resources/similarity/movie.nt"
  // var triplesDf: DataFrame = spark.read.rdf(Lang.NTRIPLES)(inputPath).cache()

  // println(inputPath)

  test("Test DistSim Modules") {

    // read in data as DataFrame
    println("Read in RDF Data as DataFrame")
    val triplesDf: DataFrame = spark.read.rdf(Lang.NTRIPLES)(inputPath).cache()

    // test feature extractor
    println("Test Feature Extractor")
    val modesToTest = List("an", "at", "as", "or")

    for (mode <- modesToTest) {
      val featureExtractorModel = new FeatureExtractorModel()
        .setMode(mode)
      val extractedFeaturesDataFrame = featureExtractorModel
        .transform(triplesDf)
        .filter(t => t.getAs[String]("uri").startsWith("m"))
      extractedFeaturesDataFrame.count()
    }

    // feature extraction
    val featureExtractorModel = new FeatureExtractorModel()
      .setMode("at")
    val extractedFeaturesDataFrame = featureExtractorModel
      .transform(triplesDf)
      .cache()
    // filter for relevant URIs e.g. only movies
    val filteredFeaturesDataFrame = extractedFeaturesDataFrame
      .filter(t => t.getAs[String]("uri").startsWith("m"))
      .cache()

    // count Vectorization
    println("Count Vectorization")
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("extractedFeatures")
      .setOutputCol("vectorizedFeatures")
      .fit(filteredFeaturesDataFrame)
    val tmpCvDf: DataFrame = cvModel.transform(filteredFeaturesDataFrame)
    val isNoneZeroVector = udf({ v: Vector => v.numNonzeros > 0 }, DataTypes.BooleanType)
    val countVectorizedFeaturesDataFrame: DataFrame = tmpCvDf
      .filter(isNoneZeroVector(col("vectorizedFeatures")))
      .select("uri", "vectorizedFeatures")
      .cache()

    // similarity Estimations Overview
    // for nearestNeighbors we need one key which is a Vector to search for NN
    println("Evaluate Similarity Estimations")
    println("For nearestNeighbors we need one key which is a Vector to search for NN")
    val sample_key: Vector = countVectorizedFeaturesDataFrame
      .take(1)(0)
      .getAs[Vector]("vectorizedFeatures")

    // Batet Similarity
    println("Test Batet Similarity")
    val batetModel = new BatetModel()
      .setInputCol("vectorizedFeatures")
    batetModel.nearestNeighbors(countVectorizedFeaturesDataFrame, sample_key, 10).collect()
    batetModel.similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, threshold = 0.5).collect()

    // Braun Blanquet Similarity
    println("Test Braun Blanquet Similarity")
    val braunBlanquetModel: BraunBlanquetModel = new BraunBlanquetModel()
      .setInputCol("vectorizedFeatures")
    braunBlanquetModel.nearestNeighbors(countVectorizedFeaturesDataFrame, sample_key, 10).collect()
    braunBlanquetModel.similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, threshold = 0.5).collect()

    // Dice Similarity
    println("Test Dice Similarity")
    val diceModel: DiceModel = new DiceModel()
      .setInputCol("vectorizedFeatures")
    diceModel.nearestNeighbors(countVectorizedFeaturesDataFrame, sample_key, 10).collect()
    diceModel.similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, threshold = 0.5).collect()

    // Jaccard similarity
    println("Test Jaccard similarity")
    val jaccardModel: JaccardModel = new JaccardModel()
      .setInputCol("vectorizedFeatures")
    jaccardModel.nearestNeighbors(countVectorizedFeaturesDataFrame, sample_key, 10).collect()
    jaccardModel.similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, threshold = 0.5).collect()

    // minHash similarity estimation
    println("Test MinHash similarity estimation")
    val minHashModel: MinHashModel = new MinHashModel()
      .setInputCol("vectorizedFeatures")
      .setNumHashTables(1)
    minHashModel.nearestNeighbors(countVectorizedFeaturesDataFrame, sample_key, 10, "minHashDistance").collect()
    minHashModel.similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, 0.8, "distance").collect()

    // Ochiai Similarity
    println("Test Ochiai Similarity")
    val ochiaiModel: OchiaiModel = new OchiaiModel()
      .setInputCol("vectorizedFeatures")
    ochiaiModel.nearestNeighbors(countVectorizedFeaturesDataFrame, sample_key, 10).collect()
    ochiaiModel.similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, threshold = 0.5).collect()

    // Simpson Similarity
    println("Test Simpson Similarity")
    val simpsonModel: SimpsonModel = new SimpsonModel()
      .setInputCol("vectorizedFeatures")
    simpsonModel.nearestNeighbors(countVectorizedFeaturesDataFrame, sample_key, 10).collect()
    simpsonModel.similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, threshold = 0.5).collect()

    // Tversky Similarity
    println("Test Tversky Similarity")
    val tverskyModel: TverskyModel = new TverskyModel()
      .setInputCol("vectorizedFeatures")
      .setAlpha(1.0)
      .setBeta(1.0)
    tverskyModel.nearestNeighbors(countVectorizedFeaturesDataFrame, sample_key, 10).collect()
    tverskyModel.similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, threshold = 0.5).collect()

    // Metagraphcreation
    println("Test Metagraph Creation")
    val model = tverskyModel // minHashModel

    val outputDf1: Dataset[_] = model
      .similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, threshold = 0.5).cache()

    // outputDf1.show(false)

    val metaGraphFactory = new SimilarityExperimentMetaGraphFactory()
    val metagraph: RDD[graph.Triple] = metaGraphFactory.createRdfOutput(
      outputDataset = outputDf1)(
      modelInformationEstimatorName = model.estimatorName, modelInformationEstimatorType = model.modelType, modelInformationMeasurementType = model.estimatorMeasureType)(
      inputDatasetNumbertOfTriples = triplesDf.count(), dataSetInformationFilePath = inputPath)
    metagraph.collect() // foreach(println(_))

  }
}
