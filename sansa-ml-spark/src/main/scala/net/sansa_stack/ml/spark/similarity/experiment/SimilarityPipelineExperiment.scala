package net.sansa_stack.ml.spark.similarity.experiment

import java.util.Calendar

import net.sansa_stack.ml.spark.similarity.similarity_measures.JaccardModel
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io._
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, MinHashLSH, MinHashLSHModel, StringIndexer, Tokenizer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import net.sansa_stack.ml.spark.utils.FeatureExtractorModel
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, DoubleType, IntegerType, LongType, StringType, StructField, StructType}

import scala.collection.mutable.ListBuffer

object SimilarityPipelineExperiment {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName(s"MinHash  tryout") // TODO where is this displayed?
      .master("local[*]") // TODO why do we need to specify this?
      // .master("spark://172.18.160.16:3090") // to run on server
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // TODO what is this for?
      .getOrCreate()

    val inputs: Seq[String] = Seq(
      "/Users/carstendraschner/GitHub/py_rdf_sim/notebooks/sampleMovieRDF10.nt",
      "/Users/carstendraschner/GitHub/py_rdf_sim/notebooks/sampleMovieRDF100.nt",
      "/Users/carstendraschner/GitHub/py_rdf_sim/notebooks/sampleMovieRDF1000.nt",
      "/Users/carstendraschner/GitHub/py_rdf_sim/notebooks/sampleMovieRDF10000.nt"
    )

    val similarityEstimationModes: Seq[String] = Seq(
      "MinHash",
      "Jaccard"
    )

    // experiment Information
    // var inputPath: String = args(0)
    // var similarityEstimationMode: String = "Jaccard"
    var parametersFeatureExtractorMode: String = "as"
    var parameterCountVectorizerMinDf: Int = 1
    var parameterCountVectorizerMaxVocabSize: Int = 1000000
    var parameterSimilarityAlpha: Double = 0.5
    var parameterSimilarityBeta: Double = 0.5
    var parameterNumHashTables: Int = 5
    var parameterSimilarityAllPairThreshold: Double = 0.5
    var parameterSimilarityNearestNeighborsK: Int = 20

    val evaluation_datetime = Calendar.getInstance().getTime().toString
    val outputFilepath = "/Users/carstendraschner/Downloads/experimentResults" + evaluation_datetime + ".csv"

    val schema = StructType(List(
      StructField("inputPath", StringType, true),
      StructField("inputFileName", StringType, true),
      StructField("inputFileSizeNumberTriples", LongType, true),
      StructField("similarityEstimationMode", StringType, true),
      StructField("parametersFeatureExtractorMode", StringType, true),
      StructField("parameterCountVectorizerMinDf", IntegerType, true),
      StructField("parameterCountVectorizerMaxVocabSize", IntegerType, true) ,
      StructField("parameterSimilarityAlpha", DoubleType, true),
      StructField("parameterSimilarityBeta", DoubleType, true),
      StructField("parameterNumHashTables", IntegerType, true),
      StructField("parameterSimilarityAllPairThreshold", DoubleType, true),
      StructField("parameterSimilarityNearestNeighborsK", IntegerType, true),
      StructField("processingTimeReadIn", DoubleType, true),
      StructField("processingTimeFeatureExtraction", DoubleType, true),
      StructField("processingTimeCountVectorizer", DoubleType, true),
      StructField("processingTimeSimilarityEstimatorSetup", DoubleType, true),
      StructField("processingTimeSimilarityEstimatorNearestNeighbors", DoubleType, true),
      StructField("processingTimeSimilarityEstimatorAllPairSimilarity", DoubleType, true),
      StructField("processingTimeTotal", DoubleType, true)
    ))

    val ex_results: scala.collection.mutable.ListBuffer[Row] = ListBuffer()
    for (input <- inputs) {
      for (similarityEstimationMode <- similarityEstimationModes) {
        ex_results += run_experiment(
          spark,
          input,
          similarityEstimationMode,
          parametersFeatureExtractorMode,
          parameterCountVectorizerMinDf,
          parameterCountVectorizerMaxVocabSize,
          parameterSimilarityAlpha,
          parameterSimilarityBeta,
          parameterNumHashTables,
          parameterSimilarityAllPairThreshold,
          parameterSimilarityNearestNeighborsK
        )
      }
    }

    val df: DataFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(
        ex_results
      ),
      schema
    )

    df.show()

    df.repartition(1).write.format("csv").save(outputFilepath)

    spark.stop()
  }

  //noinspection ScalaStyle
  def run_experiment(
    spark: SparkSession,
    inputPath: String,
    similarityEstimationMode: String,
    parametersFeatureExtractorMode: String,
    parameterCountVectorizerMinDf: Int,
    parameterCountVectorizerMaxVocabSize: Int,
    parameterSimilarityAlpha: Double,
    parameterSimilarityBeta: Double,
    parameterNumHashTables: Int,
    parameterSimilarityAllPairThreshold: Double,
    parameterSimilarityNearestNeighborsK: Int
  ): Row = {
    // experiment Information
    val inputFileName: String = inputPath.split("/").last
    val experimentTime: Long = System.nanoTime

    println("experiment started at: " + experimentTime)

    // now run experiment and keep track on processing times
    var startTime: Long = System.nanoTime


    println("1: Specify Input String")
    val input: String = inputPath
    println("\tthe used input string is: " + input)

    println("2: Read in data as Dataframe")
    val lang: Lang = Lang.NTRIPLES
    startTime = System.nanoTime
    val triples_df: DataFrame = spark.read.rdf(lang)(input)
    val inputFileSizeNumberTriples: Long = triples_df.count()
    println("\tthe file has " + inputFileSizeNumberTriples + " triples")
    val processingTimeReadIn: Double = ((System.nanoTime - startTime) / 1e9d)
    println("\tthe read in needed " + processingTimeReadIn + "seconds")

    println("3: Dataframe based feature extractor")
    println("\tfeature extraction mode is: " + parametersFeatureExtractorMode)
    startTime = System.nanoTime
    val fe = new FeatureExtractorModel()
      .setMode(parametersFeatureExtractorMode)
      .setOutputCol("extractedFeatures")
    val fe_features = fe.transform(triples_df)
    fe_features.count()
    val processingTimeFeatureExtraction = ((System.nanoTime - startTime) / 1e9d)
    println("\tthe feature extraction needed " + processingTimeFeatureExtraction + "seconds")
    // fe_features.show()

    println("4: Count Vectorizer from MLlib")
    println("\tmax vocabsize is: " + parameterCountVectorizerMaxVocabSize)
    println("\tmin number documents it has to occur is: " + parameterCountVectorizerMinDf)
    startTime = System.nanoTime
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("extractedFeatures")
      .setOutputCol("vectorizedFeatures")
      .setVocabSize(parameterCountVectorizerMaxVocabSize)
      .setMinDF(parameterCountVectorizerMinDf)
      .fit(fe_features)
    val cv_features: DataFrame = cvModel.transform(fe_features) // .select(col(feature_extractor_uri_column_name), col(count_vectorizer_features_column_name)) // .filter(isNoneZeroVector(col(count_vectorizer_features_column_name)))
    val isNoneZeroVector = udf({v: Vector => v.numNonzeros > 0}, DataTypes.BooleanType)
    val featuresDf: DataFrame = cv_features.filter(isNoneZeroVector(col("vectorizedFeatures"))).select("uri", "vectorizedFeatures")
    featuresDf.count()
    val processingTimeCountVectorizer: Double = ((System.nanoTime - startTime) / 1e9d)
    println("\tthe Count Vectorization needed " + processingTimeCountVectorizer + "seconds")

    var processingTimeSimilarityEstimatorSetup: Double = 1.0
    var processingTimeSimilarityEstimatorNearestNeighbors: Double = -1.0
    var processingTimeSimilarityEstimatorAllPairSimilarity: Double = 1.0

    if (similarityEstimationMode == "MinHash") {
      println("5. Similarity Estimation Process MinHash")
      startTime = System.nanoTime
      val mh: MinHashLSH = new MinHashLSH()
        .setNumHashTables(parameterNumHashTables)
        .setInputCol("vectorizedFeatures")
        .setOutputCol("hashedFeatures")
      val model: MinHashLSHModel = mh.fit(featuresDf)
      processingTimeSimilarityEstimatorSetup = ((System.nanoTime - startTime) / 1e9d)

      println("calculate nearestneigbors for one key")
      startTime = System.nanoTime
      val tmpK: Row = featuresDf.take(1)(0)
      val key: Vector = tmpK.getAs[Vector]("vectorizedFeatures")
      val keyUri: String = tmpK.getAs[String]("uri") // featuresDf.select("cv_features").collect()(0)(0).asInstanceOf[Vector]
      println(keyUri, key)
      val nnDf: DataFrame = model
        .approxNearestNeighbors(featuresDf, key, parameterSimilarityNearestNeighborsK, "distance")
        .withColumn("key_column", lit(keyUri)).select("key_column", "uri", "distance")
      nnDf.count()
      processingTimeSimilarityEstimatorNearestNeighbors = ((System.nanoTime - startTime) / 1e9d)
      println("calculate app pair similarity")
      startTime = System.nanoTime
      val simJoinDf = model.approxSimilarityJoin(featuresDf, featuresDf, parameterSimilarityAllPairThreshold, "distance") // .select("datasetA", "datasetB", "distance")
      simJoinDf.count()
      processingTimeSimilarityEstimatorAllPairSimilarity = ((System.nanoTime - startTime) / 1e9d)
    }
    else if (similarityEstimationMode == "Jaccard") {
      println("5. Similarity Estimation Process Jaccard")
      // Similarity Estimation
      startTime = System.nanoTime
      val similarityModel = new JaccardModel()
        .set_uri_column_name_dfA("uri")
        .set_uri_column_name_dfB("uri")
        .set_features_column_name_dfA("vectorizedFeatures")
        .set_features_column_name_dfB("vectorizedFeatures")
      processingTimeSimilarityEstimatorSetup = ((System.nanoTime - startTime) / 1e9d)
      // model evaluations
      // all pair
      startTime = System.nanoTime
      val all_pair_similarity_df: DataFrame = similarityModel.similarityJoin(featuresDf, featuresDf, parameterSimilarityAllPairThreshold)
      all_pair_similarity_df.count()
      processingTimeSimilarityEstimatorAllPairSimilarity = ((System.nanoTime - startTime) / 1e9d)

      // nearest neighbor
      similarityModel
        .set_uri_column_name_dfA("uri")
        .set_uri_column_name_dfB("uri")
        .set_features_column_name_dfA("vectorizedFeatures")
        .set_features_column_name_dfB("vectorizedFeatures")
      val key: Vector = cv_features.select("vectorizedFeatures").collect()(0)(0).asInstanceOf[Vector]
      startTime = System.nanoTime
      val nn_similarity_df = similarityModel.nearestNeighbors(cv_features, key, parameterSimilarityNearestNeighborsK, "theFirstUri", keep_key_uri_column = false)
      nn_similarity_df.count()
      processingTimeSimilarityEstimatorNearestNeighbors = ((System.nanoTime - startTime) / 1e9d)
    }
    else if (similarityEstimationMode == "Tversky") {

    }
    else {
      throw new Error("you haven't specified a working Similarity Estimation")
    }

    val processingTimeTotal: Double = ((System.nanoTime - experimentTime) / 1e9d)
    println("the complete experiment took " + processingTimeTotal + " seconds")

    // allInformation
    return Row(
      inputPath,
      inputFileName,
      inputFileSizeNumberTriples,
      similarityEstimationMode,
      parametersFeatureExtractorMode,
      parameterCountVectorizerMinDf,
      parameterCountVectorizerMaxVocabSize,
      parameterSimilarityAlpha,
      parameterSimilarityBeta,
      parameterNumHashTables,
      parameterSimilarityAllPairThreshold,
      parameterSimilarityNearestNeighborsK,
      processingTimeReadIn,
      processingTimeFeatureExtraction,
      processingTimeCountVectorizer,
      processingTimeSimilarityEstimatorSetup,
      processingTimeSimilarityEstimatorNearestNeighbors,
      processingTimeSimilarityEstimatorAllPairSimilarity,
      processingTimeTotal
    )
  }
}
