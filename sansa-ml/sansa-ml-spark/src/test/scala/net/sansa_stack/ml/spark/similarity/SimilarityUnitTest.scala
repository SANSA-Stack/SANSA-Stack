package net.sansa_stack.ml.spark.similarity

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.ml.spark.similarity.similarityEstimationModels._
import net.sansa_stack.ml.spark.utils.{FeatureExtractorModel, SimilarityExperimentMetaGraphFactory}
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph
import org.apache.jena.riot.Lang
import org.apache.jena.sys.JenaSystem
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.scalactic.TolerantNumerics
import org.scalatest.funsuite.AnyFunSuite

// Remove dummy argument to un-ignore!
class SimilarityUnitTest(ignored: String) extends AnyFunSuite with DataFrameSuiteBase {

  // define inputpath if it is not parameter
  private val inputPath = this.getClass.getClassLoader.getResource("similarity/movie.nt").getPath

  // var triplesDf: DataFrame = spark.read.rdf(Lang.NTRIPLES)(inputPath).cache()

  // for value comparison we want to allow some minor differences in number comparison
  val epsilon = 1e-4f

  implicit val doubleEq: org.scalactic.Equality[Double] = TolerantNumerics.tolerantDoubleEquality(epsilon)

  override def beforeAll(): Unit = {
    super.beforeAll()

    JenaSystem.init()
  }

  test("Test DistSim Modules") {

    // read in data as DataFrame
    println("Read in RDF Data as DataFrame")
    val triplesDf: DataFrame = spark.read.rdf(Lang.NTRIPLES)(inputPath).cache()

    triplesDf.show(false)

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

      extractedFeaturesDataFrame.show(false)

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
    val isNoneZeroVector = udf({ v: Vector => v.numNonzeros > 0 })
    val countVectorizedFeaturesDataFrame: DataFrame = tmpCvDf
      .filter(isNoneZeroVector(col("vectorizedFeatures")))
      .select("uri", "vectorizedFeatures")
      .cache()

    // similarity Estimations Overview
    // for nearestNeighbors we need one key which is a Vector to search for NN
    println("Evaluate Similarity Estimations")
    println("  For nearestNeighbors we need one key which is a Vector to search for NN")
    val sampleUri: String = "m2"
    val sample_key: Vector = countVectorizedFeaturesDataFrame
      .filter(countVectorizedFeaturesDataFrame("uri") === sampleUri)
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

      val k = 10

      // nearestNeighbor Evalaution
      println("  nearest neighbor evaluation")
      val nndf = model.nearestNeighbors(countVectorizedFeaturesDataFrame, sample_key, k, "m1", "distCol", false).cache()
      val simValues = nndf.select("distCol").rdd.map(r => r.getAs[Double]("distCol")).collect()
      for (value <- simValues) {
        // check if similarity values are in between 0 and 1
        assert((value  <= 1.0 ) && (value  >= 0.0 ))
      }
      assert((nndf.rdd.map(r => r(1)).collect().length >= 0) && (nndf.rdd.map(r => r(1)).collect().length <= 3))

      // check if only k elements are provided
      assert(nndf.collect().length <= k)

      // nndf.show(false)

      // allPairSimilarity Evalaution
      println("  all pair similarity evaluation")
      val apsdf = model.similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, threshold = 0.95, valueColumn = "distCol").cache()
      val simValuesAp = apsdf.select("distCol").rdd.map(r => r.getAs[Double]("distCol")).collect()
      for (value <- simValuesAp) {
        // check if similarity values are in between 0 and 1
        assert((value  <= 1.0 ) && (value  >= 0.0 ))
      }
      assert((nndf.rdd.map(r => r(1)).collect().length >= 0) && (nndf.rdd.map(r => r(1)).collect().length <= 3))

      // apsdf.show(false)

      // Metagraphcreation
      println("  Metagraph Creation")
      val outputDf1: Dataset[_] = apsdf

      val metaGraphFactory = new SimilarityExperimentMetaGraphFactory()
      val metagraph: RDD[graph.Triple] = metaGraphFactory.createRdfOutput(
        outputDataset = outputDf1)(
        modelInformationEstimatorName = model.estimatorName, modelInformationEstimatorType = model.modelType, modelInformationMeasurementType = model.estimatorMeasureType)(
        inputDatasetNumbertOfTriples = triplesDf.count(), dataSetInformationFilePath = inputPath)

      metagraph.collect() //
      // metagraph.foreach(println(_))

      if (List("MinHashModel", "BatetModel").contains(modelName)) {
        // similar values have to have distance 0.0
        nndf.filter(nndf("uriA") === sampleUri).select("distCol").rdd.map(r => r.getAs[Double]("distCol")).collect().foreach(x => assert(x == 0.0))
        apsdf.filter(apsdf("uriA") === apsdf("uriB")).select("distCol").rdd.map(r => r.getAs[Double]("distCol")).collect().foreach(x => assert(x == 0.0))
      }
      else {
        // similar values have to have similarity 1.0
        nndf.filter(nndf("uriA") === sampleUri).select("distCol").rdd.map(r => r.getAs[Double]("distCol")).collect().foreach(x => assert(x == 1.0))
        apsdf.filter(apsdf("uriA") === apsdf("uriB")).select("distCol").rdd.map(r => r.getAs[Double]("distCol")).collect().foreach(x => assert(x == 1.0))
      }
      // apsdf.show(false)
      // apsdf.filter((apsdf("uriA") === "m3") && (apsdf("uriB") === "m2")).show(false)

      // FIXME This unit test fails non-deterministically with maven build - (works in intelliJ) ~Claus 2023-03-03
      val valueM1M2NN: Double = nndf.filter(nndf("uriA") === "m3").select("distCol").rdd.map(r => r.getAs[Double]("distCol")).collect().take(1)(0)

      val valueM1M2AP: Double = apsdf.filter((apsdf("uriA") === "m3") && (apsdf("uriB") === "m2")).select("distCol").rdd.map(r => r.getAs[Double]("distCol")).collect().take(1)(0)
      // println(valueM1M2NN, valueM1M2AP)

      if (modelName == "BatetModel") {
        val desiredValue = 0.7369655941662061
        assert(valueM1M2NN === desiredValue)
        assert(valueM1M2AP === desiredValue)
      }
      else if (modelName == "BraunBlanquetModel") {
        val desiredValue = 0.4
        assert(valueM1M2NN === desiredValue)
        assert(valueM1M2AP === desiredValue)
      }
      else if (modelName == "DiceModel") {
        val desiredValue = 0.5
        assert(valueM1M2NN === desiredValue)
        assert(valueM1M2AP === desiredValue)
      }
      else if (modelName == "JaccardModel") {
        val desiredValue = 0.3333333333333
        assert(valueM1M2NN === desiredValue)
        assert(valueM1M2AP === desiredValue)
      }
      else if (modelName == "MinHashModel") {
        val desiredValue = 0.666666
        assert(valueM1M2NN === desiredValue)
        assert(valueM1M2AP === desiredValue)
      }
      else if (modelName == "OchiaiModel") {
        val desiredValue = 0.51639777
        assert(valueM1M2NN === desiredValue)
        assert(valueM1M2AP === desiredValue)
      }
      else if (modelName == "SimpsonModel") {
        val desiredValue = 0.66666666666
        assert(valueM1M2NN === desiredValue)
        assert(valueM1M2AP === desiredValue)
      }
      else if (modelName == "TverskyModel") {
        val desiredValue = 0.33333333
        assert(valueM1M2NN === desiredValue)
        assert(valueM1M2AP === desiredValue)
      }
    }
  }
}
