package net.sansa_stack.ml.spark.similarity

import com.holdenkarau.spark.testing.DataFrameSuiteBase
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
import org.apache.spark.sql.{DataFrame, Dataset}
import org.scalatest.FunSuite

class SimilarityUnitTest extends FunSuite with DataFrameSuiteBase {

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
        .setOutputCol("extractedFeatures")
      val extractedFeaturesDataFrame = featureExtractorModel
        .transform(triplesDf)
        .filter(t => t.getAs[String]("uri").startsWith("m"))

      println("  Test Feature Extraction mode: " + mode)

      val features = extractedFeaturesDataFrame.select("extractedFeatures").rdd.map(r => r(0)).collect()
      for (feature <- features) {
        // println(feature)
      }

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
    println("  For nearestNeighbors we need one key which is a Vector to search for NN")
    val sampleUri: String = countVectorizedFeaturesDataFrame
      .take(1)(0)
      .getAs[String]("uri")
    val sample_key: Vector = countVectorizedFeaturesDataFrame
      .take(1)(0)
      .getAs[Vector]("vectorizedFeatures")
    // println(sampleUri)
    // println(sample_key)
    println("  The element is: " + sampleUri + " and the corresponding Dense Vector Representation is: " + sample_key)

    val modelNames = List("BatetModel", "BraunBlanquetModel", "DiceModel", "JaccardModel", "MinHashModel", "OchiaiModel", "SimpsonModel", "TverskyModel")

    // evaluate all models
    for (modelName <- modelNames) {
      println("Test model: " + modelName)

      // model setup
      val model = modelName match {
        case "BatetModel" => new BatetModel()
          .setInputCol("vectorizedFeatures")
        case "BraunBlanquetModel" => new BraunBlanquetModel()
          .setInputCol("vectorizedFeatures")
        case "DiceModel" => new DiceModel()
          .setInputCol("vectorizedFeatures")
        case "JaccardModel" => new JaccardModel()
          .setInputCol("vectorizedFeatures")
        case "MinHashModel" => new MinHashModel()
          .setInputCol("vectorizedFeatures")
          .setNumHashTables(1)
        case "OchiaiModel" => new OchiaiModel()
          .setInputCol("vectorizedFeatures")
        case "SimpsonModel" => new SimpsonModel()
          .setInputCol("vectorizedFeatures")
        case "TverskyModel" => new TverskyModel()
          .setInputCol("vectorizedFeatures")
          .setAlpha(1.0)
          .setBeta(1.0)
      }

      // nearestNeighbor Evalaution
      println("  nearest neighbor evaluation")
      val nndf = model.nearestNeighbors(countVectorizedFeaturesDataFrame, sample_key, 10, "m1", "distCol", false)
      val simValues = nndf.select("distCol").rdd.map(r => r.getAs[Double]("distCol")).collect()
      for (value <- simValues) {
        // check if similarity values are in between 0 and 1
        assert((value  <= 1.0 ) && (value  >= 0.0 ))
      }
      assert((nndf.rdd.map(r => r(1)).collect().length >= 0) && (nndf.rdd.map(r => r(1)).collect().length <= 3))

      // allPairSimilarity Evalaution
      println("  all pair similarity evaluation")
      val apsdf = model.similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, threshold = 0.5, valueColumn = "distCol")
      val simValuesAp = apsdf.select("distCol").rdd.map(r => r.getAs[Double]("distCol")).collect()
      for (value <- simValuesAp) {
        // check if similarity values are in between 0 and 1
        assert((value  <= 1.0 ) && (value  >= 0.0 ))
      }
      assert((nndf.rdd.map(r => r(1)).collect().length >= 0) && (nndf.rdd.map(r => r(1)).collect().length <= 3))

      // Metagraphcreation
      println("  Metagraph Creation")
      val outputDf1: Dataset[_] = apsdf

      val metaGraphFactory = new SimilarityExperimentMetaGraphFactory()
      val metagraph: RDD[graph.Triple] = metaGraphFactory.createRdfOutput(
        outputDataset = outputDf1)(
        modelInformationEstimatorName = model.estimatorName, modelInformationEstimatorType = model.modelType, modelInformationMeasurementType = model.estimatorMeasureType)(
        inputDatasetNumbertOfTriples = triplesDf.count(), dataSetInformationFilePath = inputPath)

      // metagraph.sparql("aefafaef")

      metagraph.collect() // .foreach(println(_))
    }
  }
}
