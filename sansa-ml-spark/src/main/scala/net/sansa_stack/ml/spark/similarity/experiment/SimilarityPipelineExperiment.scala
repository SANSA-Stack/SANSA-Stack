package net.sansa_stack.ml.spark.similarity.experiment

import java.util.Calendar

import net.sansa_stack.ml.spark.similarity.similarity_measures.JaccardModel
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io._
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, MinHashLSH, MinHashLSHModel, StringIndexer, Tokenizer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import net.sansa_stack.ml.spark.utils.{ConfigResolver, FeatureExtractorModel}
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import java.io.File
import java.net.URI

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import collection.JavaConversions._
import collection.JavaConverters._
import org.spark_project.dmg.pmml.False

import scala.collection.mutable.ListBuffer

object SimilarityPipelineExperiment {
  def main(args: Array[String]): Unit = {

    val configFilePath = args(0) // "/Users/carstendraschner/Desktop/parameterConfig.conf"

    val config = new ConfigResolver(configFilePath).getConfig()

    // we read in our sample data by providing a path where all the files are
    def getListOfFiles(dir: String): List[String] = {
      val config = new ConfigResolver("")
      val tmpHdfsPart: String = config.getHdfsPart(dir)
      val tmpPathPart: String = config.getPathPart(dir)

      println(tmpHdfsPart)
      println(tmpPathPart)

      /* val file = new File(dir)
      file.listFiles.filter(_.isFile)
        .filter(_.getName.endsWith(".nt"))
        .map(_.getPath).toList
      val fs = FileSystem.get(new Configuration())
      val status: Array[FileStatus] = fs.listStatus(new Path(dir))
      val tmp = status.map(x => x.getPath.toString).toList
      tmp  */

      val uri = new URI(tmpHdfsPart)
      val fs = FileSystem.get(uri,new Configuration())
      val filePath = new Path(tmpPathPart)
      val status = fs.listStatus(filePath)
      val tmp = status.map(sts => sts.getPath.toString).toList // .foreach(println)
      tmp
    }

    println(config)
    getListOfFiles("hdfs://172.18.160.17:54310/CarstenDraschner/SimilarityExperimentData/SansaServerExperiments/sampleDataSets/smallSampleData")
    assert(false)

    // val baseConfig = ConfigFactory.load()
    // val config = ConfigFactory.parseFile(new File(configFilePath)) // .withFallback(baseConfig)

    val spark = SparkSession.builder
      .appName(s"SimilarityPipelineExperiment") // TODO where is this displayed?
      // .master("local[*]") // TODO why do we need to specify this?
      // .master("spark://172.18.160.16:3090") // to run on server
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // TODO what is this for?
      .getOrCreate()




    /* val fs = FileSystem.get(new Configuration())
    val status = fs.listStatus(new Path(pathToFolder))
    status.foreach(x=> println(x.getPath)) */


    val pathToFolder: String = config.getString("pathToFolder") // args(0)
    println("For evaluation data we search in path: " +  pathToFolder)
    val inputAll: Seq[String] = getListOfFiles(pathToFolder).toSeq
    println("we found in provided path " + inputAll.size)
    println("files are:")
    inputAll.foreach(println(_))

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
    val outputFilePath: String = config.getString("outputFilePath") // "/Users/carstendraschner/Downloads/experimentResults" + evaluation_datetime + ".csv"

    // definition of resulting dataframe schema
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
    for {
      // here we iterate over our hyperparameter room
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
      println(tmpRow)
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
    val storageFilePath: String = outputFilePath + evaluation_datetime + ".csv"

    println("we store our file here: " + storageFilePath)
    df.repartition(1).write.option("header", "true").format("csv").save(storageFilePath)
    // stop spark session
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
    // these are the parameters
    println(inputPath,
      similarityEstimationMode,
      parametersFeatureExtractorMode,
      parameterCountVectorizerMinDf,
      parameterCountVectorizerMaxVocabSize,
      parameterSimilarityAlpha,
      parameterSimilarityBeta,
      parameterNumHashTables,
      parameterSimilarityAllPairThreshold,
      parameterSimilarityNearestNeighborsK)
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
    val fe_features = fe.transform(triples_df)
    fe_features.count()
    val processingTimeFeatureExtraction = ((System.nanoTime - startTime) / 1e9d)
    println("\tthe feature extraction needed " + processingTimeFeatureExtraction + "seconds")
    // fe_features.show()

    println("3: Count Vectorizer from MLlib")
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

    var processingTimeSimilarityEstimatorSetup: Double = -1.0
    var processingTimeSimilarityEstimatorNearestNeighbors: Double = -1.0
    var processingTimeSimilarityEstimatorAllPairSimilarity: Double = -1.0

    if (similarityEstimationMode == "MinHash") {
      println("4. Similarity Estimation Process MinHash")
      println("\tthe number of hash tables is: " + parameterNumHashTables)
      startTime = System.nanoTime
      val mh: MinHashLSH = new MinHashLSH()
        .setNumHashTables(parameterNumHashTables)
        .setInputCol("vectorizedFeatures")
        .setOutputCol("hashedFeatures")
      val model: MinHashLSHModel = mh.fit(featuresDf)
      processingTimeSimilarityEstimatorSetup = ((System.nanoTime - startTime) / 1e9d)
      println("\tthe MinHash Setup needed " + processingTimeSimilarityEstimatorSetup + "seconds")


      println("4.1 Calculate nearestneigbors for one key")
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
      println("\tNearestNeighbors needed " + processingTimeSimilarityEstimatorNearestNeighbors + "seconds")

      println("4.2 Calculate app pair similarity")
      startTime = System.nanoTime
      val simJoinDf = model.approxSimilarityJoin(featuresDf, featuresDf, parameterSimilarityAllPairThreshold, "distance") // .select("datasetA", "datasetB", "distance")
      simJoinDf.count()
      processingTimeSimilarityEstimatorAllPairSimilarity = ((System.nanoTime - startTime) / 1e9d)
      println("\tAllPairSimilarity needed " + processingTimeSimilarityEstimatorAllPairSimilarity + "seconds")
    }
    else if (similarityEstimationMode == "Jaccard") {
      println("4. Similarity Estimation Process Jaccard")
      // Similarity Estimation
      startTime = System.nanoTime
      val similarityModel = new JaccardModel()
        .setUriColumnNameDfA("uri")
        .setUriColumnNameDfB("uri")
        .setFeaturesColumnNameDfA("vectorizedFeatures")
        .setFeaturesColumnNameDfB("vectorizedFeatures")
      processingTimeSimilarityEstimatorSetup = ((System.nanoTime - startTime) / 1e9d)

      // model evaluations

      // nearest neighbor
      println("4.1 Calculate nearestneigbors for one key")
      similarityModel
        .setUriColumnNameDfA("uri")
        .setUriColumnNameDfB("uri")
        .setFeaturesColumnNameDfA("vectorizedFeatures")
        .setFeaturesColumnNameDfB("vectorizedFeatures")
      val key: Vector = cv_features.select("vectorizedFeatures").collect()(0)(0).asInstanceOf[Vector]
      startTime = System.nanoTime
      val nn_similarity_df = similarityModel.nearestNeighbors(cv_features, key, parameterSimilarityNearestNeighborsK, "theFirstUri", keepKeyUriColumn = false)
      nn_similarity_df.count()
      processingTimeSimilarityEstimatorNearestNeighbors = ((System.nanoTime - startTime) / 1e9d)
      println("\tNearestNeighbors needed " + processingTimeSimilarityEstimatorNearestNeighbors + "seconds")

      // all pair
      println("4.2 Calculate app pair similarity")
      similarityModel
        .setUriColumnNameDfA("uri")
        .setUriColumnNameDfB("uri")
        .setFeaturesColumnNameDfA("vectorizedFeatures")
        .setFeaturesColumnNameDfB("vectorizedFeatures")
      startTime = System.nanoTime
      val all_pair_similarity_df: DataFrame = similarityModel.similarityJoin(featuresDf, featuresDf, parameterSimilarityAllPairThreshold)
      all_pair_similarity_df.count()
      processingTimeSimilarityEstimatorAllPairSimilarity = ((System.nanoTime - startTime) / 1e9d)
      println("\tAllPairSimilarity needed " + processingTimeSimilarityEstimatorAllPairSimilarity + "seconds")
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
