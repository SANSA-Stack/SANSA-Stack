package net.sansa_stack.ml.spark.similarity.experiment

import java.util.Calendar

import net.sansa_stack.ml.spark.similarity.similarity_measures.{JaccardModel, TverskyModel}
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io._
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, MinHashLSH, MinHashLSHModel, StringIndexer, Tokenizer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import net.sansa_stack.ml.spark.utils.{ConfigResolver, FeatureExtractorModel, FileLister}
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import java.io.File
import java.net.URI

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.log4j.{Level, Logger}

import collection.JavaConversions._
import collection.JavaConverters._
import org.spark_project.dmg.pmml.False

import scala.collection.mutable.ListBuffer

object SimilarityPipelineExperiment {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder
      .appName(s"SimilarityPipelineExperiment") // TODO where is this displayed?
      .master("local[*]") // TODO why do we need to specify this?
      // .master("spark://172.18.160.16:3090") // to run on server
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // TODO what is this for?
      .getOrCreate()

    val configFilePath = args(0) // "/Users/carstendraschner/Desktop/parameterConfig.conf"

    val config = new ConfigResolver(configFilePath).getConfig()

    println()
    val pathToFolder: String = config.getString("pathToFolder") // args(0)
    println("For evaluation data we search in path: " +  pathToFolder)

    val inputAll = FileLister.getListOfFiles(pathToFolder).filter(_.endsWith(".nt")) // getListOfFiles(pathToFolder).toSeq
    println("we found in provided path " + inputAll.size)
    println("files are:")
    inputAll.foreach(println(_))
    println()

    // we can specify the number of run throughs for averagging the measurements:
    val numberRuns: Int = config.getInt("numberRuns")

    // Here we specify the hyperparameter grid
    val similarityEstimationModeAll: List[String] = config.getStringList("similarityEstimationModeAll").toList // Seq("MinHash", "Jaccard")
    val parametersFeatureExtractorModeAll: List[String] = config.getStringList("parametersFeatureExtractorModeAll").toList // Seq("at") // , "as")
    val parameterCountVectorizerMinDfAll: List[Int] = config.getIntList("parameterCountVectorizerMinDfAll").toList.map(_.toInt)
    val parameterCountVectorizerMaxVocabSizeAll: List[Int] = config.getIntList("parameterCountVectorizerMaxVocabSizeAll").toList.map(_.toInt)// Seq(100000)
    val parameterSimilarityAlphaAll: List[Double] = config.getDoubleList("parameterSimilarityAlphaAll").toList.map(_.toDouble) // Seq(0.5)
    val parameterSimilarityBetaAll: List[Double] = config.getDoubleList("parameterSimilarityBetaAll").toList.map(_.toDouble)// Seq(0.5)
    val parameterNumHashTablesAll: List[Int] = config.getIntList("parameterNumHashTablesAll").toList.map(_.toInt) // Seq(1, 5)
    val parameterSimilarityAllPairThresholdAll: List[Double] = config.getDoubleList("parameterSimilarityAllPairThresholdAll").toList.map(_.toDouble) // Seq(0.8)
    val parameterSimilarityNearestNeighborsKAll: List[Int] = config.getIntList("parameterSimilarityNearestNeighborsKAll").toList.map(_.toInt) // Seq(20)

    // this is the path to output and we add the current datetime information
    val evaluation_datetime = Calendar.getInstance().getTime().toString
    println()
    val outputFilePath: String = config.getString("outputFilePath") // "/Users/carstendraschner/Downloads/experimentResults" + evaluation_datetime + ".csv"

    // definition of resulting dataframe schema
    val schema = StructType(List(
      StructField("run", IntegerType, true),
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
    for {
      // here we iterate over our hyperparameter room
      run <- 1 to numberRuns
      input <- inputAll
      similarityEstimationMode <- similarityEstimationModeAll
      parametersFeatureExtractorMode <- parametersFeatureExtractorModeAll
      parameterCountVectorizerMinDf <- parameterCountVectorizerMinDfAll
      parameterCountVectorizerMaxVocabSize <- parameterCountVectorizerMaxVocabSizeAll
      parameterSimilarityAlpha <- parameterSimilarityAlphaAll
      parameterSimilarityBeta <- parameterSimilarityBetaAll
      parameterNumHashTables <- parameterNumHashTablesAll
      parameterSimilarityAllPairThreshold <- parameterSimilarityAllPairThresholdAll
      parameterSimilarityNearestNeighborsK <- parameterSimilarityNearestNeighborsKAll
    } {
      val tmpRow: Row = run_experiment(
        spark,
        run,
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
      println("Resulting row for DataFrame:")
      println(tmpRow)
      println()
      ex_results += tmpRow
  }

    val df: DataFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(
        ex_results
      ),
      schema
    )
    // show the resulting dataframe
    df.show()
    // store the data as csv
    val storageFilePath: String = outputFilePath + evaluation_datetime.replace(":", "").replace(" ", "") + ".csv"

    println("we store our file here: " + storageFilePath)
    df.repartition(1).write.option("header", "true").format("csv").save(storageFilePath)
    // stop spark session
    spark.stop()
  }

  //noinspection ScalaStyle
  def run_experiment(
    spark: SparkSession,
    run: Int,
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
    // these are the parameters
    println("These are the parameters:")
    println(
      run,
      inputPath,
      similarityEstimationMode,
      parametersFeatureExtractorMode,
      parameterCountVectorizerMinDf,
      parameterCountVectorizerMaxVocabSize,
      parameterSimilarityAlpha,
      parameterSimilarityBeta,
      parameterNumHashTables,
      parameterSimilarityAllPairThreshold,
      parameterSimilarityNearestNeighborsK)
    println()

    // experiment Information
    val inputFileName: String = inputPath.split("/").last

    // now run experiment and keep track on processing times
    val experimentTime: Long = System.nanoTime
    var startTime: Long = System.nanoTime

    // Input Specification
    println("1: Read in data as Dataframe")
    println("\tthe used input string is: " + inputPath)
    val lang: Lang = Lang.NTRIPLES
    startTime = System.nanoTime
    val triples_df: DataFrame = spark.read.rdf(lang)(inputPath)
    val inputFileSizeNumberTriples: Long = triples_df.count()
    println("\tthe file has " + inputFileSizeNumberTriples + " triples")
    val processingTimeReadIn: Double = ((System.nanoTime - startTime) / 1e9d)
    println("\tthe read in needed " + processingTimeReadIn + "seconds")

    println("2: Dataframe based feature extractor")
    println("\tfeature extraction mode is: " + parametersFeatureExtractorMode)
    startTime = System.nanoTime
    val fe = new FeatureExtractorModel()
      .setMode(parametersFeatureExtractorMode)
      .setOutputCol("extractedFeatures")
    val feFeatures = fe.transform(triples_df)
    feFeatures.count()
    val processingTimeFeatureExtraction = ((System.nanoTime - startTime) / 1e9d)
    println("\tthe feature extraction needed " + processingTimeFeatureExtraction + "seconds")
    feFeatures.show(false)

    println("3: Count Vectorizer from MLlib")
    println("\tmax vocabsize is: " + parameterCountVectorizerMaxVocabSize)
    println("\tmin number documents it has to occur is: " + parameterCountVectorizerMinDf)
    startTime = System.nanoTime
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("extractedFeatures")
      .setOutputCol("vectorizedFeatures")
      .setVocabSize(parameterCountVectorizerMaxVocabSize)
      .setMinDF(parameterCountVectorizerMinDf)
      .fit(feFeatures)
    val cvFeatures: DataFrame = cvModel.transform(feFeatures) // .select(col(feature_extractor_uri_column_name), col(count_vectorizer_features_column_name)) // .filter(isNoneZeroVector(col(count_vectorizer_features_column_name)))
    val isNoneZeroVector = udf({v: Vector => v.numNonzeros > 0}, DataTypes.BooleanType)
    val featuresDf: DataFrame = cvFeatures.filter(isNoneZeroVector(col("vectorizedFeatures"))).select("uri", "vectorizedFeatures")
    featuresDf.count()
    val processingTimeCountVectorizer: Double = ((System.nanoTime - startTime) / 1e9d)
    println("\tthe Count Vectorization needed " + processingTimeCountVectorizer + "seconds")
    featuresDf.show(false)

    var processingTimeSimilarityEstimatorSetup: Double = -1.0
    var processingTimeSimilarityEstimatorNearestNeighbors: Double = -1.0
    var processingTimeSimilarityEstimatorAllPairSimilarity: Double = -1.0

    val tmpK: Row = featuresDf.take(1)(0)
    val key: Vector = tmpK.getAs[Vector]("vectorizedFeatures")
    val keyUri: String = tmpK.getAs[String]("uri")
    println()
    println(keyUri, key)


    if (similarityEstimationMode == "MinHash") {
      println("4. Similarity Estimation Process MinHash")
      println("\tthe number of hash tables is: " + parameterNumHashTables)
      startTime = System.nanoTime
      val similarityModel: MinHashLSHModel = new MinHashLSH()
        .setNumHashTables(parameterNumHashTables)
        .setInputCol("vectorizedFeatures")
        .setOutputCol("hashedFeatures")
        .fit(featuresDf)
      processingTimeSimilarityEstimatorSetup = ((System.nanoTime - startTime) / 1e9d)
      println("\tthe MinHash Setup needed " + processingTimeSimilarityEstimatorSetup + "seconds")


      println("4.1 Calculate nearestneigbors for one key")
      startTime = System.nanoTime
      val nnSimilarityDf: DataFrame = similarityModel
        .approxNearestNeighbors(featuresDf, key, parameterSimilarityNearestNeighborsK, "distance")
        .withColumn("key_column", lit(keyUri)).select("key_column", "uri", "distance")
      val numberOfNn: Long = nnSimilarityDf.count()
      println("\tWe have number NN: " + numberOfNn)
      processingTimeSimilarityEstimatorNearestNeighbors = ((System.nanoTime - startTime) / 1e9d)
      println("\tNearestNeighbors needed " + processingTimeSimilarityEstimatorNearestNeighbors + "seconds")
      nnSimilarityDf.show(false)

      println("4.2 Calculate app pair similarity")
      startTime = System.nanoTime
      val allPairSimilarityDf = similarityModel.approxSimilarityJoin(featuresDf, featuresDf, parameterSimilarityAllPairThreshold, "distance") // .select("datasetA", "datasetB", "distance")
      val lenJoinDf: Long = allPairSimilarityDf.count()
      println("\tWe have number Join: " + lenJoinDf)
      processingTimeSimilarityEstimatorAllPairSimilarity = ((System.nanoTime - startTime) / 1e9d)
      println("\tAllPairSimilarity needed " + processingTimeSimilarityEstimatorAllPairSimilarity + "seconds")
      allPairSimilarityDf.show(false)
    }
    else if (similarityEstimationMode == "Jaccard") {
      println("4. Similarity Estimation Process Jaccard")
      // Similarity Estimation
      startTime = System.nanoTime
      val similarityModel = new JaccardModel()
        .setInputCol("vectorizedFeatures")
      processingTimeSimilarityEstimatorSetup = ((System.nanoTime - startTime) / 1e9d)

      // model evaluations

      // nearest neighbor
      println("4.1 Calculate nearestneigbors for one key")
      startTime = System.nanoTime
      val nnSimilarityDf: DataFrame = similarityModel.nearestNeighbors(cvFeatures, key, parameterSimilarityNearestNeighborsK, "theFirstUri", keepKeyUriColumn = false)
      val numberOfNn: Long = nnSimilarityDf.count()
      println("\tWe have number NN: " + numberOfNn)
      processingTimeSimilarityEstimatorNearestNeighbors = ((System.nanoTime - startTime) / 1e9d)
      println("\tNearestNeighbors needed " + processingTimeSimilarityEstimatorNearestNeighbors + "seconds")
      nnSimilarityDf.show(false)

      // all pair
      println("4.2 Calculate app pair similarity")
      startTime = System.nanoTime
      val allPairSimilarityDf: DataFrame = similarityModel.similarityJoin(featuresDf, featuresDf, parameterSimilarityAllPairThreshold)
      val lenJoinDf: Long = allPairSimilarityDf.count()
      println("\tWe have number Join: " + lenJoinDf)
      processingTimeSimilarityEstimatorAllPairSimilarity = ((System.nanoTime - startTime) / 1e9d)
      println("\tAllPairSimilarity needed " + processingTimeSimilarityEstimatorAllPairSimilarity + "seconds")
      allPairSimilarityDf.show(false)
    }
    else if (similarityEstimationMode == "Tversky") {
      println("4. Similarity Estimation Process Tversky")
      // Similarity Estimation
      startTime = System.nanoTime
      val similarityModel = new TverskyModel()
        .setInputCol("vectorizedFeatures")
        .setAlpha(1.0)
        .setBeta(1.0)
      processingTimeSimilarityEstimatorSetup = ((System.nanoTime - startTime) / 1e9d)

      // model evaluations

      // nearest neighbor
      println("4.1 Calculate nearestneigbors for one key")
      startTime = System.nanoTime
      val nnSimilarityDf = similarityModel.nearestNeighbors(cvFeatures, key, parameterSimilarityNearestNeighborsK, "theFirstUri", keepKeyUriColumn = false)
      val numberOfNn: Long = nnSimilarityDf.count()
      println("\tWe have number NN: " + numberOfNn)
      processingTimeSimilarityEstimatorNearestNeighbors = ((System.nanoTime - startTime) / 1e9d)
      println("\tNearestNeighbors needed " + processingTimeSimilarityEstimatorNearestNeighbors + "seconds")
      nnSimilarityDf.show(false)

      // all pair
      println("4.2 Calculate app pair similarity")
      startTime = System.nanoTime
      val allPairSimilarityDf: DataFrame = similarityModel.similarityJoin(featuresDf, featuresDf, parameterSimilarityAllPairThreshold)
      val lenJoinDf: Long = allPairSimilarityDf.count()
      println("\tWe have number Join: " + lenJoinDf)
      processingTimeSimilarityEstimatorAllPairSimilarity = ((System.nanoTime - startTime) / 1e9d)
      println("\tAllPairSimilarity needed " + processingTimeSimilarityEstimatorAllPairSimilarity + "seconds")
      allPairSimilarityDf.show(false)

    }
    else {
      throw new Error("you haven't specified a working Similarity Estimation")
    }

    val processingTimeTotal: Double = ((System.nanoTime - experimentTime) / 1e9d)
    println("the complete experiment took " + processingTimeTotal + " seconds")

    // allInformation
    return Row(
      run,
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
