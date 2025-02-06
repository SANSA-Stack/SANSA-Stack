package net.sansa_stack.ml.spark.similarity.experiment

import java.util.Calendar

import net.sansa_stack.ml.spark.similarity.similarityEstimationModels._
import net.sansa_stack.ml.spark.utils.{ConfigResolver, FeatureExtractorModel, FileLister}
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.riot.Lang
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, MinHashLSH, MinHashLSHModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.jdk.CollectionConverters._
import scala.collection.mutable.ListBuffer

object SimilarityPipelineExperiment {

  private val Log: Logger = Logger.getLogger(SimilarityPipelineExperiment.getClass.getCanonicalName)

  /**
   * main method reading in the parameter space, creating the spark, starting all combinations of parameters for experiments and storing the resulting processing times in a dataframe
   *
   * @param args args(0) has to be the conf file specifiying all needed (hyper)parameters
   */
  def main(args: Array[String]): Unit = {

    /**
     * read in config file from args
     */
    val configFilePath = args(0)
    val config = new ConfigResolver(configFilePath).getConfig()

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkMaster = config.getString("master")

    /**
     * get the files from specified folder on which we run the experiments
     */
    println()
    val pathToFolder: String = config.getString("pathToFolder") // args(0)
    println("For evaluation data we search in path: " +  pathToFolder)
    val inputAll = FileLister.getListOfFiles(pathToFolder).filter(_.endsWith(".nt")) // getListOfFiles(pathToFolder).toSeq
    println("we found in provided path " + inputAll.size)
    println("files are:")
    inputAll.foreach(println(_))
    println()

    /**
     * specify if we want to see intermediate daframes in command line output
     * if they are printed, runtime is higher
     */
    val showDataFrames: Boolean = config.getBoolean("showDataFrames")

    /**
     * we can specify the number of run throughs for reducing outlier influence of the measurements:
     * later we can do median over dataframe for completly similar experiment setups
     */
    val numberRuns: Int = config.getInt("numberRuns")

    /**
     * Here we read in the specified the hyperparameter grid
     */
    val similarityEstimationModeAll: List[String] = config.getStringList("similarityEstimationModeAll").asScala.toList // Seq("MinHash", "Jaccard")
    val parametersFeatureExtractorModeAll: List[String] = config.getStringList("parametersFeatureExtractorModeAll").asScala.toList // Seq("at") // , "as")
    val parameterCountVectorizerMinDfAll: List[Int] = config.getIntList("parameterCountVectorizerMinDfAll").asScala.toList.map(_.toInt)
    val parameterCountVectorizerMaxVocabSizeAll: List[Int] = config.getIntList("parameterCountVectorizerMaxVocabSizeAll").asScala.toList.map(_.toInt)// Seq(100000)
    val parameterSimilarityAlphaAll: List[Double] = config.getDoubleList("parameterSimilarityAlphaAll").asScala.toList.map(_.toDouble) // Seq(0.5)
    val parameterSimilarityBetaAll: List[Double] = config.getDoubleList("parameterSimilarityBetaAll").asScala.toList.map(_.toDouble)// Seq(0.5)
    val parameterNumHashTablesAll: List[Int] = config.getIntList("parameterNumHashTablesAll").asScala.toList.map(_.toInt) // Seq(1, 5)
    val parameterSimilarityAllPairThresholdAll: List[Double] = config.getDoubleList("parameterSimilarityAllPairThresholdAll").asScala.toList.map(_.toDouble) // Seq(0.8)
    val parameterSimilarityNearestNeighborsKAll: List[Int] = config.getIntList("parameterSimilarityNearestNeighborsKAll").asScala.toList.map(_.toInt) // Seq(20)
    val parameterOnlyMovieSimilarity: Boolean = config.getBoolean("parameterOnlyMovieSimilarity")
    val pipelineComponents: List[String] = config.getStringList("pipelineComponents").asScala.toList

    val totalNumberExperiments = (
      numberRuns *
        inputAll.length *
        similarityEstimationModeAll.length *
        parametersFeatureExtractorModeAll.length *
        parameterCountVectorizerMinDfAll.length *
        parameterCountVectorizerMaxVocabSizeAll.length *
        parameterSimilarityAlphaAll.length *
        parameterSimilarityBetaAll.length *
        parameterNumHashTablesAll.length *
        parameterSimilarityAllPairThresholdAll.length *
        parameterSimilarityNearestNeighborsKAll.length
    )

    /**
     * for documentation of timestamps
     * this is used for the path to output and we add the current datetime information
     */
    val evaluation_datetime = Calendar.getInstance().getTime().toString
    println()
    val outputFilePath: String = config.getString("outputFilePath")

    /**
     * definition of resulting dataframe schema
     * in this dataframe we track all (hyper)parameters and processing times
     */
    val schema = StructType(List(
      StructField("pipelineComponents", StringType, true),
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
      StructField("processingTimeTotal", DoubleType, true),
      StructField("onlyMovieSimilarity", BooleanType, true)
    ))

    val ex_results: scala.collection.mutable.ListBuffer[Row] = ListBuffer()

    /**
     * here we call the the complete space of hyper paramters one by one and hand
     * over the to run function to get a row of processing time etc
     */
    var counterCurrentRun: Int = 0

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
      counterCurrentRun +=1
      println("Experiment (" + counterCurrentRun + "/" + totalNumberExperiments + ")")

      val tmpRow: Row = runExperiment(
        sparkMaster,
        pipelineComponents,
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
        parameterSimilarityNearestNeighborsK,
        parameterOnlyMovieSimilarity,
        showDataFrames
      )
      println("Resulting row for DataFrame:")
      println(tmpRow)
      println()
      ex_results += tmpRow
  }
    /**
     * start spark session
     */
    val spark = SparkSession.builder()
      .appName(s"SimilarityPipelineExperiment")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    /**
     * here we create the resulting experiment tracking dataframe
     */
    val df: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(ex_results.toList), schema).cache()

    /**
     * show the resulting dataframe of our overall experiment and store it
     */
    df.show()
    val storageFilePath: String = outputFilePath + evaluation_datetime.replace(":", "").replace(" ", "") + ".csv"
    println("we store our file here: " + storageFilePath)
    df.repartition(1).write.option("header", "true").format("csv").save(storageFilePath)
    // stop spark session
    spark.stop()
  }

  /**
   * we create call one explicit defined pipeline to aquire processing times
   *
   * @param sparkMaster spark session
   * @param pipelineComponents which part of the pipeline are called
   * @param run number of runs of the same experiment
   * @param inputPath path to the file we evaluate on
   * @param similarityEstimationMode which similarity estimation we take
   * @param parametersFeatureExtractorMode mode how we get gain features for uris throur FeatureExtractorModel
   * @param parameterCountVectorizerMinDf minDf value for count Vectorizer from MLlib
   * @param parameterCountVectorizerMaxVocabSize maxVocabSize value for count Vectorizer from MLlib
   * @param parameterSimilarityAlpha alpha value for tversky and others
   * @param parameterSimilarityBeta beta value for tversky and others
   * @param parameterNumHashTables num hash tables minhash creates
   * @param parameterSimilarityAllPairThreshold threshold for min or max similarity/distance
   * @param parameterSimilarityNearestNeighborsK number of proposed nearestneighbors
   * @param parameterOnlyMovieSimilarity if in comparison only movies from datasets should be taken into account
   * @param showDataFrames whether intermediate resulting dataframes should be proposed
   * @return provides a Row with all entries of hyperparameters and measured processing times
   */
  //noinspection ScalaStyle
  def runExperiment(
    sparkMaster: String,
    pipelineComponents: List[String],
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
    parameterSimilarityNearestNeighborsK: Int,
    parameterOnlyMovieSimilarity: Boolean,
    showDataFrames: Boolean = false
  ): Row = {
    // these are the parameters
    println("These are the parameters:")
    println(
      pipelineComponents,
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
      parameterSimilarityNearestNeighborsK,
      parameterOnlyMovieSimilarity
    )
    println()

    /**
     * start spark session
     */
    val spark = SparkSession.builder()
      .appName(s"SimilarityPipelineExperiment") // TODO where is this displayed?
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // TODO what is this for?
      .getOrCreate()

    // now run experiment and keep track on processing times
    val experimentTime: Long = System.nanoTime
    var startTime: Long = System.nanoTime

    if (!pipelineComponents.contains("ri")) {
      return Row(
        pipelineComponents.toString(),
        run,
        inputPath,
        "no file",
        0,
        similarityEstimationMode,
        parametersFeatureExtractorMode,
        parameterCountVectorizerMinDf,
        parameterCountVectorizerMaxVocabSize,
        parameterSimilarityAlpha,
        parameterSimilarityBeta,
        parameterNumHashTables,
        parameterSimilarityAllPairThreshold,
        parameterSimilarityNearestNeighborsK,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        ((System.nanoTime - experimentTime) / 1e9d),
        parameterOnlyMovieSimilarity
      )
    }
    // experiment Information
    val inputFileName: String = inputPath.split("/").last

    // Input Specification
    println("1: Read in data as Dataframe")
    println("\tthe used input string is: " + inputPath)
    val lang: Lang = Lang.NTRIPLES
    startTime = System.nanoTime
    val triplesDf: DataFrame = spark.read.rdf(lang)(inputPath).cache()
    val inputFileSizeNumberTriples: Long = triplesDf.count()
    println("\tthe file has " + inputFileSizeNumberTriples + " triples")
    val processingTimeReadIn: Double = ((System.nanoTime - startTime) / 1e9d)
    println("\tthe read in needed " + processingTimeReadIn + "seconds")

    if (showDataFrames) triplesDf.limit(10).show()

    if (!pipelineComponents.contains("fe")) {
      return Row(
        pipelineComponents.toString(),
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
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        parameterOnlyMovieSimilarity
      )
    }

    println("2: Dataframe based feature extractor")
    if (parameterOnlyMovieSimilarity) println("\tFeature Dataframe consits only Movies")
    println("\tfeature extraction mode is: " + parametersFeatureExtractorMode)
    startTime = System.nanoTime
    val fe = new FeatureExtractorModel()
      .setMode(parametersFeatureExtractorMode)
      .setOutputCol("extractedFeatures")

    /**
     * Quick and dirty part for filtering, in future we do it over query API
     */
    val feFeatures = if (parameterOnlyMovieSimilarity) fe.transform(triplesDf).filter(t => t.getAs[String]("uri").split("/").last.startsWith("m")).cache() else fe.transform(triplesDf).cache()
    // val feFeatures = if (parameterOnlyMovieSimilarity) fe.transform(triplesDf).filter(t => t.getAs[String]("uri").contains("/film/")) else fe.transform(triplesDf)

    println("\tour extracted dataframe contains of: " + feFeatures.count() + " different uris")
    val processingTimeFeatureExtraction = ((System.nanoTime - startTime) / 1e9d)
    println("\tthe feature extraction needed " + processingTimeFeatureExtraction + "seconds")
    if (showDataFrames) feFeatures.limit(10).show()

    if (!pipelineComponents.contains("cv")) {
      return Row(
        pipelineComponents.toString(),
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
        0.0,
        0.0,
        0.0,
        0.0,
        ((System.nanoTime - experimentTime) / 1e9d),
        parameterOnlyMovieSimilarity
      )
    }

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
    val cvFeatures: DataFrame = cvModel.transform(feFeatures).cache() // .select(col(feature_extractor_uri_column_name), col(count_vectorizer_features_column_name)) // .filter(isNoneZeroVector(col(count_vectorizer_features_column_name)))
    val isNoneZeroVector = udf({v: Vector => v.numNonzeros > 0}, DataTypes.BooleanType)
    val featuresDf: DataFrame = cvFeatures.filter(isNoneZeroVector(col("vectorizedFeatures"))).select("uri", "vectorizedFeatures").cache()
    featuresDf.count()
    val processingTimeCountVectorizer: Double = ((System.nanoTime - startTime) / 1e9d)
    println("\tthe Count Vectorization needed " + processingTimeCountVectorizer + "seconds")
    if (showDataFrames) featuresDf.limit(10).show()

    var processingTimeSimilarityEstimatorSetup: Double = -1.0
    var processingTimeSimilarityEstimatorNearestNeighbors: Double = -1.0
    var processingTimeSimilarityEstimatorAllPairSimilarity: Double = -1.0

    val tmpK: Row = featuresDf.take(1)(0)
    val key: Vector = tmpK.getAs[Vector]("vectorizedFeatures")
    val keyUri: String = tmpK.getAs[String]("uri")
    println()
    println(keyUri, key)

    if (!pipelineComponents.contains("nn")) {
      return Row(
        pipelineComponents.toString(),
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
        0.0,
        0.0,
        0.0,
        ((System.nanoTime - experimentTime) / 1e9d),
        parameterOnlyMovieSimilarity
      )
    }

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
        .withColumn("key_column", lit(keyUri))
        .select("key_column", "uri", "distance")
        .cache()
      val numberOfNn: Long = nnSimilarityDf.count()
      println("\tWe have number NN: " + numberOfNn)
      processingTimeSimilarityEstimatorNearestNeighbors = ((System.nanoTime - startTime) / 1e9d)
      println("\tNearestNeighbors needed " + processingTimeSimilarityEstimatorNearestNeighbors + "seconds")
      if (showDataFrames) nnSimilarityDf.limit(10).show()

      if (!pipelineComponents.contains("ap")) {
        return Row(
          pipelineComponents.toString(),
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
          0.0,
          ((System.nanoTime - experimentTime) / 1e9d),
          parameterOnlyMovieSimilarity
        )
      }

      println("4.2 Calculate all pair similarity")
      startTime = System.nanoTime
      val allPairSimilarityDf = similarityModel
        .approxSimilarityJoin(featuresDf, featuresDf, parameterSimilarityAllPairThreshold, "distance")
        .cache()
      val lenJoinDf: Long = allPairSimilarityDf.count()
      println("\tWe have number Join: " + lenJoinDf)
      processingTimeSimilarityEstimatorAllPairSimilarity = ((System.nanoTime - startTime) / 1e9d)
      println("\tAllPairSimilarity needed " + processingTimeSimilarityEstimatorAllPairSimilarity + "seconds")
      if (showDataFrames) allPairSimilarityDf.limit(10).show()
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
      val nnSimilarityDf: DataFrame = similarityModel
        .nearestNeighbors(cvFeatures, key, parameterSimilarityNearestNeighborsK, "theFirstUri", keepKeyUriColumn = false)
        .cache()
      val numberOfNn: Long = nnSimilarityDf.count()
      println("\tWe have number NN: " + numberOfNn)
      processingTimeSimilarityEstimatorNearestNeighbors = ((System.nanoTime - startTime) / 1e9d)
      println("\tNearestNeighbors needed " + processingTimeSimilarityEstimatorNearestNeighbors + " seconds")
      if (showDataFrames) nnSimilarityDf.limit(10).show()

      // triplesDf.filter(col("p").contains("/title")).join(nnSimilarityDf, col("s") === col("uriA")).filter(col("p").contains("/title")).show(false)

      if (!pipelineComponents.contains("ap")) {
        return Row(
          pipelineComponents.toString(),
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
          0.0,
          ((System.nanoTime - experimentTime) / 1e9d),
          parameterOnlyMovieSimilarity
        )
      }

      // all pair
      println("4.2 Calculate all pair similarity")
      startTime = System.nanoTime
      val allPairSimilarityDf: DataFrame = similarityModel
        .similarityJoin(featuresDf, featuresDf, 1 - parameterSimilarityAllPairThreshold)
        .cache()
      val lenJoinDf: Long = allPairSimilarityDf.count()
      println("\tWe have number Join: " + lenJoinDf)
      processingTimeSimilarityEstimatorAllPairSimilarity = ((System.nanoTime - startTime) / 1e9d)
      println("\tAllPairSimilarity needed " + processingTimeSimilarityEstimatorAllPairSimilarity + "seconds")
      if (showDataFrames) allPairSimilarityDf.limit(10).show()
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
      val nnSimilarityDf = similarityModel
        .nearestNeighbors(cvFeatures, key, parameterSimilarityNearestNeighborsK, "theFirstUri", keepKeyUriColumn = false)
        .cache()
      val numberOfNn: Long = nnSimilarityDf.count()
      println("\tWe have number NN: " + numberOfNn)
      processingTimeSimilarityEstimatorNearestNeighbors = ((System.nanoTime - startTime) / 1e9d)
      println("\tNearestNeighbors needed " + processingTimeSimilarityEstimatorNearestNeighbors + "seconds")
      if (showDataFrames) nnSimilarityDf.limit(10).show()

      if (!pipelineComponents.contains("ap")) {
        return Row(
          pipelineComponents.toString(),
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
          0.0,
          ((System.nanoTime - experimentTime) / 1e9d),
          parameterOnlyMovieSimilarity
        )
      }

      // all pair
      println("4.2 Calculate all pair similarity")
      startTime = System.nanoTime
      val allPairSimilarityDf: DataFrame = similarityModel
        .similarityJoin(featuresDf, featuresDf, 1 - parameterSimilarityAllPairThreshold)
        .cache()
      val lenJoinDf: Long = allPairSimilarityDf.count()
      println("\tWe have number Join: " + lenJoinDf)
      processingTimeSimilarityEstimatorAllPairSimilarity = ((System.nanoTime - startTime) / 1e9d)
      println("\tAllPairSimilarity needed " + processingTimeSimilarityEstimatorAllPairSimilarity + "seconds")
      if (showDataFrames) allPairSimilarityDf.limit(10).show()

    }
    else if (similarityEstimationMode == "Batet") {
      println("4. Similarity Estimation Process Batet")
      // Similarity Estimation
      startTime = System.nanoTime
      val similarityModel = new BatetModel()
        .setInputCol("vectorizedFeatures")
      processingTimeSimilarityEstimatorSetup = ((System.nanoTime - startTime) / 1e9d)

      // model evaluations

      // nearest neighbor
      println("4.1 Calculate nearestneigbors for one key")
      startTime = System.nanoTime
      val nnSimilarityDf = similarityModel
        .nearestNeighbors(cvFeatures, key, parameterSimilarityNearestNeighborsK, "theFirstUri", keepKeyUriColumn = false)
        .cache()
      val numberOfNn: Long = nnSimilarityDf.count()
      println("\tWe have number NN: " + numberOfNn)
      processingTimeSimilarityEstimatorNearestNeighbors = ((System.nanoTime - startTime) / 1e9d)
      println("\tNearestNeighbors needed " + processingTimeSimilarityEstimatorNearestNeighbors + "seconds")
      if (showDataFrames) nnSimilarityDf.limit(10).show()

      if (!pipelineComponents.contains("ap")) {
        return Row(
          pipelineComponents.toString(),
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
          0.0,
          ((System.nanoTime - experimentTime) / 1e9d),
          parameterOnlyMovieSimilarity
        )
      }

      // all pair
      println("4.2 Calculate all pair similarity")
      startTime = System.nanoTime
      val allPairSimilarityDf: DataFrame = similarityModel
        .similarityJoin(featuresDf, featuresDf, parameterSimilarityAllPairThreshold)
        .cache()
      val lenJoinDf: Long = allPairSimilarityDf.count()
      println("\tWe have number Join: " + lenJoinDf)
      processingTimeSimilarityEstimatorAllPairSimilarity = ((System.nanoTime - startTime) / 1e9d)
      println("\tAllPairSimilarity needed " + processingTimeSimilarityEstimatorAllPairSimilarity + "seconds")
      if (showDataFrames) allPairSimilarityDf.limit(10).show()

    }
    else if (similarityEstimationMode == "Braun Blanquet") {
      println("4. Similarity Estimation Process Braun Blanquet")
      // Similarity Estimation
      startTime = System.nanoTime
      val similarityModel = new BraunBlanquetModel()
        .setInputCol("vectorizedFeatures")
      processingTimeSimilarityEstimatorSetup = ((System.nanoTime - startTime) / 1e9d)

      // model evaluations

      // nearest neighbor
      println("4.1 Calculate nearestneigbors for one key")
      startTime = System.nanoTime
      val nnSimilarityDf = similarityModel
        .nearestNeighbors(cvFeatures, key, parameterSimilarityNearestNeighborsK, "theFirstUri", keepKeyUriColumn = false)
        .cache()
      val numberOfNn: Long = nnSimilarityDf.count()
      println("\tWe have number NN: " + numberOfNn)
      processingTimeSimilarityEstimatorNearestNeighbors = ((System.nanoTime - startTime) / 1e9d)
      println("\tNearestNeighbors needed " + processingTimeSimilarityEstimatorNearestNeighbors + "seconds")
      if (showDataFrames) nnSimilarityDf.limit(10).show()

      if (!pipelineComponents.contains("ap")) {
        return Row(
          pipelineComponents.toString(),
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
          0.0,
          ((System.nanoTime - experimentTime) / 1e9d),
          parameterOnlyMovieSimilarity
        )
      }

      // all pair
      println("4.2 Calculate all pair similarity")
      startTime = System.nanoTime
      val allPairSimilarityDf: DataFrame = similarityModel
        .similarityJoin(featuresDf, featuresDf, parameterSimilarityAllPairThreshold)
        .cache()
      val lenJoinDf: Long = allPairSimilarityDf.count()
      println("\tWe have number Join: " + lenJoinDf)
      processingTimeSimilarityEstimatorAllPairSimilarity = ((System.nanoTime - startTime) / 1e9d)
      println("\tAllPairSimilarity needed " + processingTimeSimilarityEstimatorAllPairSimilarity + "seconds")
      if (showDataFrames) allPairSimilarityDf.limit(10).show()

    }
    else if (similarityEstimationMode == "Dice") {
      println("4. Similarity Estimation Process Dice")
      // Similarity Estimation
      startTime = System.nanoTime
      val similarityModel = new DiceModel()
        .setInputCol("vectorizedFeatures")
      processingTimeSimilarityEstimatorSetup = ((System.nanoTime - startTime) / 1e9d)

      // model evaluations

      // nearest neighbor
      println("4.1 Calculate nearestneigbors for one key")
      startTime = System.nanoTime
      val nnSimilarityDf = similarityModel
        .nearestNeighbors(cvFeatures, key, parameterSimilarityNearestNeighborsK, "theFirstUri", keepKeyUriColumn = false)
        .cache()
      val numberOfNn: Long = nnSimilarityDf.count()
      println("\tWe have number NN: " + numberOfNn)
      processingTimeSimilarityEstimatorNearestNeighbors = ((System.nanoTime - startTime) / 1e9d)
      println("\tNearestNeighbors needed " + processingTimeSimilarityEstimatorNearestNeighbors + "seconds")
      if (showDataFrames) nnSimilarityDf.limit(10).show()

      if (!pipelineComponents.contains("ap")) {
        return Row(
          pipelineComponents.toString(),
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
          0.0,
          ((System.nanoTime - experimentTime) / 1e9d),
          parameterOnlyMovieSimilarity
        )
      }

      // all pair
      println("4.2 Calculate all pair similarity")
      startTime = System.nanoTime
      val allPairSimilarityDf: DataFrame = similarityModel
        .similarityJoin(featuresDf, featuresDf, parameterSimilarityAllPairThreshold)
        .cache()
      val lenJoinDf: Long = allPairSimilarityDf.count()
      println("\tWe have number Join: " + lenJoinDf)
      processingTimeSimilarityEstimatorAllPairSimilarity = ((System.nanoTime - startTime) / 1e9d)
      println("\tAllPairSimilarity needed " + processingTimeSimilarityEstimatorAllPairSimilarity + "seconds")
      if (showDataFrames) allPairSimilarityDf.limit(10).show()

    }
    else if (similarityEstimationMode == "Ochiai") {
      println("4. Similarity Estimation Process Ochiai")
      // Similarity Estimation
      startTime = System.nanoTime
      val similarityModel = new OchiaiModel()
        .setInputCol("vectorizedFeatures")
      processingTimeSimilarityEstimatorSetup = ((System.nanoTime - startTime) / 1e9d)

      // model evaluations

      // nearest neighbor
      println("4.1 Calculate nearestneigbors for one key")
      startTime = System.nanoTime
      val nnSimilarityDf = similarityModel
        .nearestNeighbors(cvFeatures, key, parameterSimilarityNearestNeighborsK, "theFirstUri", keepKeyUriColumn = false)
        .cache()
      val numberOfNn: Long = nnSimilarityDf.count()
      println("\tWe have number NN: " + numberOfNn)
      processingTimeSimilarityEstimatorNearestNeighbors = ((System.nanoTime - startTime) / 1e9d)
      println("\tNearestNeighbors needed " + processingTimeSimilarityEstimatorNearestNeighbors + "seconds")
      if (showDataFrames) nnSimilarityDf.limit(10).show()

      if (!pipelineComponents.contains("ap")) {
        return Row(
          pipelineComponents.toString(),
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
          0.0,
          ((System.nanoTime - experimentTime) / 1e9d),
          parameterOnlyMovieSimilarity
        )
      }

      // all pair
      println("4.2 Calculate all pair similarity")
      startTime = System.nanoTime
      val allPairSimilarityDf: DataFrame = similarityModel
        .similarityJoin(featuresDf, featuresDf, parameterSimilarityAllPairThreshold)
        .cache()
      val lenJoinDf: Long = allPairSimilarityDf.count()
      println("\tWe have number Join: " + lenJoinDf)
      processingTimeSimilarityEstimatorAllPairSimilarity = ((System.nanoTime - startTime) / 1e9d)
      println("\tAllPairSimilarity needed " + processingTimeSimilarityEstimatorAllPairSimilarity + "seconds")
      if (showDataFrames) allPairSimilarityDf.limit(10).show()

    }
    else if (similarityEstimationMode == "Simpson") {
      println("4. Similarity Estimation Process Simpson")
      // Similarity Estimation
      startTime = System.nanoTime
      val similarityModel = new SimpsonModel()
        .setInputCol("vectorizedFeatures")
      processingTimeSimilarityEstimatorSetup = ((System.nanoTime - startTime) / 1e9d)

      // model evaluations

      // nearest neighbor
      println("4.1 Calculate nearestneigbors for one key")
      startTime = System.nanoTime
      val nnSimilarityDf = similarityModel
        .nearestNeighbors(cvFeatures, key, parameterSimilarityNearestNeighborsK, "theFirstUri", keepKeyUriColumn = false)
        .cache()
      val numberOfNn: Long = nnSimilarityDf.count()
      println("\tWe have number NN: " + numberOfNn)
      processingTimeSimilarityEstimatorNearestNeighbors = ((System.nanoTime - startTime) / 1e9d)
      println("\tNearestNeighbors needed " + processingTimeSimilarityEstimatorNearestNeighbors + "seconds")
      if (showDataFrames) nnSimilarityDf.limit(10).show()

      if (!pipelineComponents.contains("ap")) {
        return Row(
          pipelineComponents.toString(),
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
          0.0,
          ((System.nanoTime - experimentTime) / 1e9d),
          parameterOnlyMovieSimilarity
        )
      }

      // all pair
      println("4.2 Calculate all pair similarity")
      startTime = System.nanoTime
      val allPairSimilarityDf: DataFrame = similarityModel
        .similarityJoin(featuresDf, featuresDf, parameterSimilarityAllPairThreshold)
        .cache()
      val lenJoinDf: Long = allPairSimilarityDf.count()
      println("\tWe have number Join: " + lenJoinDf)
      processingTimeSimilarityEstimatorAllPairSimilarity = ((System.nanoTime - startTime) / 1e9d)
      println("\tAllPairSimilarity needed " + processingTimeSimilarityEstimatorAllPairSimilarity + "seconds")
      if (showDataFrames) allPairSimilarityDf.limit(10).show()

    }
    else if (similarityEstimationMode == "MinHashJaccardStacked") {
      println("4. Similarity Estimation Process MinHashJaccardStacked")
      println("\tthe number of hash tables is: " + parameterNumHashTables)
      startTime = System.nanoTime
      val similarityModelMinHash: MinHashLSHModel = new MinHashLSH()
        .setNumHashTables(parameterNumHashTables)
        .setInputCol("vectorizedFeatures")
        .setOutputCol("hashedFeatures")
        .fit(featuresDf)
      val similarityModelJaccard = new JaccardModel()
        .setInputCol("vectorizedFeatures")
      processingTimeSimilarityEstimatorSetup = ((System.nanoTime - startTime) / 1e9d)
      println("\tthe MinHash Setup needed " + processingTimeSimilarityEstimatorSetup + "seconds")



      println("4.1 Calculate nearestneigbors for one key")
      startTime = System.nanoTime
      val nnSimilarityDf = similarityModelMinHash
        .approxNearestNeighbors(featuresDf, key, parameterSimilarityNearestNeighborsK, "distance")
        .cache()
        // .withColumn("key_column", lit(keyUri)).select("key_column", "uri", "distance")
      // val shortendedDf = featuresDf.join(nnSimilarityDf, Seq("uri"), "leftsemi")

      val shortendedNnDf = nnSimilarityDf // similarityModelJaccard.nearestNeighbors(shortendedDf, key, parameterSimilarityNearestNeighborsK)

      val numberOfNn: Long = shortendedNnDf.count()
      println("\tWe have number NN: " + numberOfNn)
      processingTimeSimilarityEstimatorNearestNeighbors = ((System.nanoTime - startTime) / 1e9d)
      println("\tNearestNeighbors needed " + processingTimeSimilarityEstimatorNearestNeighbors + "seconds")
      if (showDataFrames) shortendedNnDf.limit(10).show()

      if (!pipelineComponents.contains("ap")) {
        return Row(
          pipelineComponents.toString(),
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
          0.0,
          ((System.nanoTime - experimentTime) / 1e9d),
          parameterOnlyMovieSimilarity
        )
      }

      println("4.2 Calculate all pair similarity")
      startTime = System.nanoTime
      val allPairSimilarityDf = similarityModelMinHash
        .approxSimilarityJoin(featuresDf, featuresDf, 1 - parameterSimilarityAllPairThreshold, "distance")
        .cache()
      val lenJoinDf: Long = allPairSimilarityDf.count()
      println("\tWe have number Join: " + lenJoinDf)
      // allPairSimilarityDf.limit(10).show(false)

      val minHashedSimilarities = allPairSimilarityDf
        .withColumn("uriA", col("datasetA").getField("uri"))
        .withColumn("uriB", col("datasetB").getField("uri"))
        .select("uriA", "uriB", "distance")
        .cache()
      if (showDataFrames) minHashedSimilarities.limit(10).show(false)
      /* val dfGoodCandidatesDf = featuresDf.join(minHashedSimilarities, Seq("uriA", "uriB"), "left") // .withColumn("pair", (col("uriA"), col("uriB"))).filter(col("pair")) // r => (r(0), r(1)).isInCollection(uriCandidates))// col("uri").isInCollection(uriCandidates))
      featuresDf.limit(10).show(false)
      dfGoodCandidatesDf.limit(10).show(false)
      println("We reduced dataframe size from: " + featuresDf.count() + " to " + dfGoodCandidatesDf.count())
      similarityModelJaccard.similarityJoin(dfGoodCandidatesDf, dfGoodCandidatesDf, threshold = 0.5).limit(10).show() */
      processingTimeSimilarityEstimatorAllPairSimilarity = ((System.nanoTime - startTime) / 1e9d)
      println("\tAllPairSimilarity needed " + processingTimeSimilarityEstimatorAllPairSimilarity + "seconds")
    }
    else {
      throw new Error("you haven't specified a working Similarity Estimation")
    }

    val processingTimeTotal: Double = ((System.nanoTime - experimentTime) / 1e9d)
    println("the complete experiment took " + processingTimeTotal + " seconds")

    spark.stop()

    // allInformation
    return Row(
      pipelineComponents.toString(),
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
      processingTimeTotal,
      parameterOnlyMovieSimilarity
    )
  }
}
