package net.sansa_stack.ml.spark.anomalydetection

import net.sansa_stack.ml.spark.anomalydetection.DistADLogger.LOG

import java.io.{BufferedWriter, File, FileWriter}
import net.sansa_stack.ml.spark.utils.FeatureExtractorModel
import net.sansa_stack.rdf.common.io.riot.error.{
  ErrorParseMode,
  WarningParseMode
}
import net.sansa_stack.rdf.spark.io.{NTripleReader, RDFReader}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.jena.graph
import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.riot.Lang
import org.apache.log4j.{Level, Logger}
import org.apache.spark.HashPartitioner
import org.apache.spark.ml.clustering.BisectingKMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.{
  CountVectorizer,
  CountVectorizerModel,
  MinHashLSH,
  MinHashLSHModel
}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, count, expr, lit, mean, stddev, udf}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.util.control.Breaks.break

/** This class gathers all the utilities needed for distributed anomaly
  * detection
  */
object DistADUtil {

  val LOG: Logger = Logger.getLogger(DistADUtil.getClass)
  val objList = List(
    "http://www.w3.org/2001/XMLSchema#decimal",
    "http://www.w3.org/2001/XMLSchema#integer",
    "http://www.w3.org/2001/XMLSchema#double",
    "http://www.w3.org/2001/XMLSchema#float",
    "http://www.w3.org/2001/XMLSchema#int",
    "http://www.w3.org/2001/XMLSchema#long",
    "http://www.w3.org/2001/XMLSchema#unsignedInt",
    "http://www.w3.org/2001/XMLSchema#unsignedLong",
    "http://www.w3.org/2001/XMLSchema#positiveInteger",
    "http://www.w3.org/2001/XMLSchema#nonNegativeInteger",
    "http://www.w3.org/2001/XMLSchema#negativeInteger",
    "http://www.w3.org/2001/XMLSchema#nonPositiveInteger"
  )

  /** Creates an Spark session and returns it
    * @return
    */
  def createSpark(): SparkSession = {
    lazy val spark = SparkSession
      .builder()
      .appName(s"Anomaly Detection")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config(
        "spark.kryo.registrator",
        String.join(
          ", ",
          "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
          "net.sansa_stack.query.spark.sparqlify.KryoRegistratorSparqlify"
        )
      )
      .config("spark.sql.crossJoin.enabled", value = true)
      .config("spark.sql.shuffle.partitions", 2000)
      .config("spark.kryoserializer.buffer.max", 2047)
      .config("spark.sql.pivotMaxValues", 100000)
      .master("local[6]")
      .getOrCreate()

    Logger.getLogger("org").setLevel(Level.ERROR)
    spark
  }

  /** Based on the input file extension, reads the file into memory in a
    * distributed manner
    * @param spark
    *   the Spark session
    * @param input
    *   the path of the input file
    * @return
    *   RDD[Triple]
    */
  def readData(spark: SparkSession, input: String): RDD[Triple] = {
    var originalDataRDD: RDD[graph.Triple] = null
    if (input.endsWith("nt")) {
      originalDataRDD = NTripleReader
        .load(
          spark,
          input,
          stopOnBadTerm = ErrorParseMode.SKIP,
          stopOnWarnings = WarningParseMode.IGNORE
        )
    } else {
      val lang = Lang.TURTLE
      originalDataRDD = spark.rdf(lang)(input)
    }
    originalDataRDD
  }

  /** Gets a literal string and decide if the literal string is a numeric
    * literal or not
    * @param x
    *   The literal String
    * @return
    *   @code{true} or @code{false}
    */
  def isNumeric(x: String): Boolean = {
    try {
      if (x.contains("^")) {
        val c = x.indexOf('^')
        val subject = x.substring(1, c - 1)

        if (isAllDigits(subject)) {
          true
        } else {
          false
        }
      } else {
        false
      }
    } catch {
      case e: Exception => false
    }
  }

  /** Checks if the given string contains only digits
    * @param x
    *   the string value
    * @return
    *   @code{true} or @code{false}
    */
  def isAllDigits(x: String): Boolean = {
    var found = true
    for (ch <- x) {
      if (!ch.isDigit) {
        found = false
      }
    }
    found
  }

  /** Gets an String and list of Strings, decide if the list contains the given
    * String
    * @param x
    *   the given string
    * @param y
    *   list of strings
    * @return
    *   @code{true} or @code{false}
    */
  def searchEdge(x: String, y: List[String]): Boolean = {
    if (x.contains("^")) {
      val c = x.indexOf('^')
      val subject = x.substring(c + 2)
      y.contains(subject)
    } else false
  }

  /** Gets a literal string value and extract the number from it
    * @param a
    *   the literal String value
    * @return
    *   the number form the literal string, "0" O.W
    */
  def getNumber(a: String): Double = {
    try {
      val c = a.indexOf('^')
      var subject = a.substring(0, c)
      subject = StringUtils.stripStart(subject, "\"")
      subject = StringUtils.stripEnd(subject, "\"")
      subject.toDouble
    } catch {
      case e: Throwable => Double.NaN
    }
  }

  /** Gets a RDD[Triple] and filter only literals
    * @param nTriplesRDD
    *   the given RDD[Triple]
    * @return
    *   a new RDD[Triple] containing only literals
    */
  def getOnlyLiteralObjects(nTriplesRDD: RDD[Triple]): RDD[Triple] =
    nTriplesRDD.filter(f => f.getObject.isLiteral)

  /** Gets a RDD[Triple] and filter only numeric literals
    * @param objLit
    *   the given RDD[Triple]
    * @return
    *   a new RDD[Triple] containing only numeric literals
    */
  def triplesWithNumericLit(objLit: RDD[Triple]): RDD[Triple] =
    objLit.filter(f => isNumeric(f.getObject.toString()))

  /** Gets a RDD[Triple] and filter only numeric literals based on the data types. It also ignores all the predicates which ends with "ID":
    * @param data
    *   the given RDD[Triple]
    * @return
    *   a new RDD[Triple] containing only numeric literals
    */
  def triplesWithNumericLitWithTypeIgnoreEndingWithID(
      data: RDD[Triple]
  ): RDD[Triple] = {

    var onlyLiteralDataRDD = triplesWithNumericLit(data)
    onlyLiteralDataRDD = onlyLiteralDataRDD.filter(
      f => searchEdge(f.getObject.toString(), objList)
    )

    onlyLiteralDataRDD = onlyLiteralDataRDD.filter(
      f => !f.getPredicate.toString().toLowerCase().endsWith("id")
    )
    onlyLiteralDataRDD
  }

  /** A UDF for converting numeric strings to double
    */
  val convertStringToDouble: UserDefinedFunction = udf({ (v: String) =>
    getNumber(v).toString.toDouble
  })

  def filterAllTriplesWhichAtLeastHaveOneNumericLiterals(
      originalDataRDD: RDD[Triple],
      onlyLiteralDataRDD: RDD[Triple]
  ): RDD[Triple] = {
    val a1 = onlyLiteralDataRDD.map(f => (f.getSubject.toString(), f))
    val a2 = originalDataRDD.map(f => (f.getSubject.toString(), f))
    val a3 = a2.join(a1)
    a3.map(f => f._2._1)
  }

  def merge[A, B](input: List[Map[A, B]]): Map[A, List[B]] = {
    input.flatten
      .groupBy { case (k, v)           => k }
      .mapValues { _.map { case (k, v) => v } }
  }

  /** Writes list[String] to a file with given path. It handles HDFS and the normal file system
    * @param data
    *   the data that should be written to a file
    * @param path
    *   the path of the output file
    */
  def writeAnomaliesToFile(data: List[String], path: String): Unit = {
    if (path.contains("hdfs://")) {
      val stringBuilder: StringBuilder = new StringBuilder()
      data.foreach(line => stringBuilder.append(line).append("\n"))
      val fsPath = new Path(path)
      val fs =
        fsPath.getFileSystem(createSpark().sparkContext.hadoopConfiguration)
      fs.delete(fsPath, true)
      val os = fs.create(new Path(path))
      os.write(stringBuilder.toString().getBytes)
      fs.close()
    } else {
      val file = new File(path)
      val bw = new BufferedWriter(new FileWriter(file))
      for (line <- data) {
        bw.write(line + "\n")
      }
      bw.close()
    }
  }

  /** Writes a dataframe to a file with given path. It handles HDFS and the normal file system
    * @param path
    *   the path of the output file
    * @param data
    *   the dataframe that should be written to a file
    */
  def writeToFile(path: String, data: DataFrame): Unit = {
    if (path.contains("hdfs://")) {
      val stringBuilder: StringBuilder = new StringBuilder()
      data
        .coalesce(1)
        .collect()
        .foreach(row => {
          stringBuilder.append(row.mkString(",")).append("\n")
        })

      val fsPath = new Path(path)
      val fs =
        fsPath.getFileSystem(createSpark().sparkContext.hadoopConfiguration)
      fs.delete(fsPath, true)
      val os = fs.create(new Path(path))
      os.write(stringBuilder.toString().getBytes)
      fs.close()
    } else {
      val tsvWithHeaderOptions: Map[String, String] = Map(
        ("delimiter", ","),
        ("header", "true")
      )
      data
        .coalesce(1)
        .write
        .mode(SaveMode.Overwrite)
        .options(tsvWithHeaderOptions)
        .csv(path)
    }

  }

  /** A function which sample the data and run clustering with different K. At
    * the end select the K with highest Silhouette value
    * @param data
    *   the given dataframe for clustering
    * @param percentage
    *   the percentage for sampling
    * @return
    *   the optimal K for clustering
    */
  def detectNumberOfClusters(data: DataFrame, percentage: Double): Int = {
    var sampledData: DataFrame = null
    if (percentage != 1f) {
      sampledData = data.sample(percentage)
    } else {
      sampledData = data
    }

    val featureExtractorModel = new FeatureExtractorModel()
      .setMode("or")
    val extractedFeaturesDataFrame: DataFrame = featureExtractorModel
      .transform(sampledData)
    val assembler: CountVectorizerModel = new CountVectorizer()
      .setInputCol("extractedFeatures")
      .setOutputCol("features")
      .fit(extractedFeaturesDataFrame)

    val transformedData = assembler.transform(extractedFeaturesDataFrame)

    var bestIndex = -1
    var maxQuality = Double.MinValue
    val evaluator = new ClusteringEvaluator()

    for (i <- 2 to 10) {
      val bkm =
        new BisectingKMeans()
          .setK(i)
          .setSeed(1L)
          .setFeaturesCol("features")

      val model = bkm.fit(transformedData)
      val predictions = model.transform(transformedData)

      val count = predictions.select("prediction").distinct().count()
      if (count < i) {
        if (count != 1) {
          return count.toInt
        } else {
          return i
        }
      }
      val silhouette = evaluator.evaluate(predictions)
      if (silhouette > maxQuality) {
        maxQuality = silhouette
        bestIndex = i
      }
      LOG.info(
        "Silhouette Coefficient for " + i + " clusters is " + silhouette
      )
    }
    bestIndex
  }

  /** Anomaly Detection method based on Interquartile Range
    * @param data
    *   a given dataframe
    * @param verbose
    *   to show more internal outputs
    * @param anomalyListSize
    *   the min value list size for considering a list for anomaly detection
    *   process
    * @return
    *   a dataframe
    */
  def iqr(
      data: DataFrame,
      verbose: Boolean,
      anomalyListSize: Int
  ): DataFrame = {
    val dataPivoted =
      data
        .groupBy("prediction", "p")
        .agg(
          count("o") as "count",
          expr("approx_percentile(o,0.25)") as ("q1"),
          expr("approx_percentile(o,0.75)") as ("q3")
        )
        .withColumn("iqr", col("q3") - col("q1"))
        .withColumn("upper", col("q3") + lit(1.5) * col("iqr"))
        .withColumn("lower", col("q1") - lit(1.5) * col("iqr"))
        .cache()

    if (verbose) {
      LOG.info(
        "result of aggregation:"
      )
      dataPivoted.show(false)
    }

    val a = data.join(
      dataPivoted,
      Seq("prediction", "p")
    )

    if (verbose) {
      LOG.info(
        "result of aggregation join:"
      )
      a.show(false)
    }

    val finalResult = a
      .filter(col("count") > anomalyListSize)
      .filter(col("o") < col("lower") || col("o") > col("upper"))
      .select("s", "p", "o")
    if (verbose) {
      LOG.info("total number of anomalies " + finalResult.count())
      finalResult.show(false)
    }
    finalResult
  }

  /** Anomaly Detection method based on Mean Absolute Deviation (MAD)
    * @param data
    *   a given dataframe
    * @param verbose
    *   to show more internal outputs
    * @param anomalyListSize
    *   the min value list size for considering a list for anomaly detection
    *   process
    * @return
    *   a dataframe
    */
  def mad(
      data: DataFrame,
      verbose: Boolean,
      anomalyListSize: Int
  ): DataFrame = {
    val dataPivoted =
      data
        .groupBy("prediction", "p")
        .agg(
          count("o") as "count",
          expr("approx_percentile(o,0.5)") as ("median")
        )

    if (verbose) {
      LOG.info(
        "result of aggregation:"
      )
      dataPivoted.show(false)
    }

    val a = data
      .join(
        dataPivoted,
        Seq("prediction", "p")
      )
      .withColumn("difference", col("o") - col("median"))

    if (verbose) {
      LOG.info(
        "result of aggregation join:"
      )
      a.show(false)
    }

    val b = a
      .groupBy("prediction", "p")
      .agg(
        expr("approx_percentile(difference,0.5)") as ("MAD")
      )

    if (verbose) {
      b.show(false)
    }

    val c = a
      .join(
        b,
        Seq("prediction", "p")
      )
      .withColumn("upper", col("median") + lit(2.5) * col("MAD"))
      .withColumn("lower", col("median") - lit(2.5) * col("MAD"))

    if (verbose) {
      c.show(false)
    }

    val finalResult = c
      .filter(col("count") > anomalyListSize)
      .filter(col("o") < col("lower") || col("o") > col("upper"))
      .select("s", "p", "o")
    if (verbose) {
      LOG.info("total number of anomalies " + finalResult.count())
      finalResult.show(false)
    }
    finalResult

  }

  /** Anomaly Detection method based on Z-Score
    * @param data
    *   a given dataframe
    * @param verbose
    *   to show more internal outputs
    * @param anomalyListSize
    *   the min value list size for considering a list for anomaly detection
    *   process
    * @return
    *   a dataframe
    */
  def zscore(
      data: DataFrame,
      verbose: Boolean,
      anomalyListSize: Int
  ): DataFrame = {
    val dataPivoted =
      data
        .groupBy("prediction", "p")
        .agg(
          count("o") as "count",
          mean("o") as "mean",
          stddev("o") as "std"
        )
    if (verbose) {
      LOG.info(
        "result of aggregation:"
      )
      dataPivoted.show(false)
    }

    var a = data.join(
      dataPivoted,
      Seq("prediction", "p")
    )

    if (verbose) {
      LOG.info(
        "result of aggregation join:"
      )
      a.show(false)
    }
    a = a.withColumn("zscore", col("o") - col("mean") / col("std"))
    val threshold: Int = 2
    val finalResult = a
      .filter(col("count") > anomalyListSize)
      .filter(org.apache.spark.sql.functions.abs(col("zscore")) > threshold)
      .select("s", "p", "o")
    if (verbose) {
      LOG.info("total number of anomalies " + finalResult.count())
      finalResult.show(false)
    }
    finalResult
  }

  /** Gets an RDD[Triple] and converts it to a dataframe
    * @param data
    *   the given RDD[Triple]
    * @return
    *   a dataframe containing s,p,o
    */
  def createDF(
      data: RDD[Triple]
  ): DataFrame = {
    import net.sansa_stack.rdf.spark.model.TripleOperations
    val df = data.toDF()
    df
  }

  /** Gets an RDD[Triple] and converts it to a dataframe with converting numeric
    * strings to double
    * @param data
    *   the given RDD[Triple]
    * @return
    *   a dataframe containing s,p,o
    */
  def createDFWithConversion(
      data: RDD[Triple]
  ): DataFrame = {
    import net.sansa_stack.rdf.spark.model.TripleOperations
    val df = data.toDF()
    df.withColumn("o", convertStringToDouble(col("o")))
  }

  /** Run BiSectingKMean clustering on a given RDD[Triple]
    * @param data
    *   the given RDD[Triple]
    * @param numberOfClusters
    *   number of clusters
    * @return
    *   a dataframe containing cluster id for each data point
    */
  def calculateBiSectingKmeanClustering(
      data: RDD[Triple],
      numberOfClusters: Int
  ): DataFrame = {
    val featureExtractorModel = new FeatureExtractorModel()
      .setMode("or")
    val extractedFeaturesDataFrame: DataFrame = featureExtractorModel
      .transform(data)
    val assembler: CountVectorizerModel = new CountVectorizer()
      .setInputCol("extractedFeatures")
      .setOutputCol("features")
      .fit(extractedFeaturesDataFrame)

    val transformedData = assembler.transform(extractedFeaturesDataFrame)

    val bkm =
      new BisectingKMeans()
        .setK(numberOfClusters)
        .setSeed(1L)
        .setFeaturesCol("features")

    val model = bkm.fit(transformedData)
    val prediction =
      model.transform(transformedData).withColumnRenamed("uri", "s")
    prediction
  }

  /** Run BiSectingKMean clustering on a given Dataframe
    * @param data
    *   the given RDD[Triple]
    * @param numberOfClusters
    *   number of clusters
    * @return
    *   a dataframe containing cluster id for each data point
    */
  def calculateBiSectingKmeanClustering(
      data: DataFrame,
      numberOfClusters: Int
  ): DataFrame = {
    val featureExtractorModel = new FeatureExtractorModel()
      .setMode("or")
    val extractedFeaturesDataFrame: DataFrame = featureExtractorModel
      .transform(data)
    val assembler: CountVectorizerModel = new CountVectorizer()
      .setInputCol("extractedFeatures")
      .setOutputCol("features")
      .fit(extractedFeaturesDataFrame)

    val transformedData = assembler.transform(extractedFeaturesDataFrame)

    val bkm =
      new BisectingKMeans()
        .setK(numberOfClusters)
        .setSeed(1L)
        .setFeaturesCol("features")

    val model = bkm.fit(transformedData)
    val prediction =
      model.transform(transformedData).withColumnRenamed("uri", "s")
    prediction
  }

  /**
    * Gets a @link{Node} and returns the local name
    * @param x a given @link{Node}
    * @return the local name
    */
  def getLocalName(x: Node): String = {
    var a = x.toString().lastIndexOf("/")
    val b = x.toString().substring(a + 1)
    b
  }

  /**
    * Gets a number and a list and checks if the list contains the number
    * @param a the given number
    * @param b the given list
    * @return @code{True} if the number is in the list, @code{false} O.W
    */
  def search(a: Double, b: Array[Double]): Boolean = {
    if (b.contains(a)) true
    else false
  }

  def calculateMinHashLSHClustering(
      partialDataRDD: RDD[Triple],
      originalData: RDD[Triple],
      config: DistADConfig
  ): DataFrame = {

    val mapSubWithTriples =
      propClustering(
        partialDataRDD
      )

    val validEntitySet =
      partialDataRDD.map(f => f.getSubject).cache().collect().toSet

    val nTriplesRDDWithValidEntities =
      originalData.filter(f => validEntitySet.contains(f.getSubject))

    val pairwiseSim: Dataset[_] = config.clusteringType match {
      case config.PARTIAL =>
        calculatePairwiseSimilarity(
          nTriplesRDDWithValidEntities,
          config.pairWiseDistanceThreshold
        )
      case config.FULL =>
        calculatePairwiseSimilarity(
          originalData,
          config.pairWiseDistanceThreshold
        )
    }

    val opiu: DataFrame = pairwiseSim
      .filter(col("datasetA.uri").isNotNull)
      .filter(col("datasetB.uri").isNotNull)
      .filter((col("datasetA.uri") =!= col("datasetB.uri")))
      .select(
        col("datasetA.uri").alias("id1"),
        col("datasetB.uri").alias("id2")
      )

    val x1 = opiu.persist(StorageLevel.MEMORY_AND_DISK)

    val x1Map = x1.rdd.map(row => {
      val id = row.getString(0)
      val value = row.getString(1)
      (id, value)
    })

    val initialSet3 = mutable.Set.empty[String]
    val addToSet3 = (s: mutable.Set[String], v: String) => s += v
    val mergePartitionSets3 =
      (p1: mutable.Set[String], p2: mutable.Set[String]) => p1 ++= p2
    val uniqueByKey3 =
      x1Map.aggregateByKey(initialSet3)(addToSet3, mergePartitionSets3)

    val k = uniqueByKey3.map(f => ((f._1, (f._2 += (f._1)).toSet)))

    val partitioner = new HashPartitioner(500)
    val mapSubWithTriplesPart = mapSubWithTriples
      .partitionBy(partitioner)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val ys = k.partitionBy(partitioner).persist(StorageLevel.MEMORY_AND_DISK)
    val joinSimSubTriples2 = ys.join(mapSubWithTriplesPart)

    val clusterOfSubjects = joinSimSubTriples2.map({
      case (s, (iter, iter1)) =>
        ((iter).toSet, iter1)
    })

    val initialSet =
      mutable.HashSet.empty[mutable.Set[(String, String, Double)]]
    val addToSet = (
        s: mutable.HashSet[mutable.Set[(String, String, Double)]],
        v: mutable.Set[(String, String, Double)]
    ) => s += v
    val mergePartitionSets = (
        p1: mutable.HashSet[mutable.Set[(String, String, Double)]],
        p2: mutable.HashSet[mutable.Set[(String, String, Double)]]
    ) => p1 ++= p2
    val uniqueByKey =
      clusterOfSubjects.aggregateByKey(initialSet)(addToSet, mergePartitionSets)

    val propCluster = uniqueByKey.map({
      case (a, (iter)) =>
        (iter.flatMap(f => f.map(f => f._2)).toSet, ((iter)))
    })

    val propDistinct = propCluster.flatMap {
      case (a, ((iter))) =>
        a.map(f => (f, ((iter.flatMap(f => f).toSet))))
    }

    val clusterOfProp = propDistinct.map({
      case (a, (iter1)) => (iter1.filter(f => f._2.equals(a)))
    })

    mapSubWithTriplesPart.unpersist()
    ys.unpersist()

    val setData: RDD[Set[(String, String, Double)]] =
      clusterOfProp.repartition(1000).persist(StorageLevel.MEMORY_AND_DISK)
    val setDataStore: RDD[Seq[(String, String, Double)]] =
      setData.map(f => f.toSeq)

    val spark = createSpark()
    import spark.implicits._
    val b: RDD[(Seq[(String, String, Double)], Long)] =
      setDataStore.zipWithIndex()
    val a: RDD[Seq[(String, String, Double, Long)]] = b.map(f => {
      f._1.map(r => {
        (r._1, r._2, r._3.toString.replace("\"", "") toDouble, f._2)
      })
    })

    val c: RDD[(String, String, Double, Long)] = a.flatMap(identity)
    c.toDF("s", "p", "o", "prediction")

  }

  private def calculatePairwiseSimilarity(
      triplesRDD: RDD[graph.Triple],
      pairWiseDistanceThreshold: Double
  ) = {
    val featureExtractorModel = new FeatureExtractorModel()
      .setMode("or")
    val extractedFeaturesDataFrame: DataFrame = featureExtractorModel
      .transform(triplesRDD)

    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("extractedFeatures")
      .setOutputCol("vectorizedFeatures")
      .fit(extractedFeaturesDataFrame)
    val tmpCvDf: DataFrame = cvModel.transform(extractedFeaturesDataFrame)

    val isNoneZeroVector =
      udf({ (v: Vector) =>
        v.numNonzeros > 0
      })

    val countVectorizedFeaturesDataFrame: DataFrame = tmpCvDf
      .filter(isNoneZeroVector(col("vectorizedFeatures")))
      .select("uri", "vectorizedFeatures")

    val countVectorizedFeaturesDataFramePartitioned =
      countVectorizedFeaturesDataFrame.cache()

    countVectorizedFeaturesDataFramePartitioned.collect()

    val minHashModel: MinHashLSHModel = new MinHashLSH()
      .setNumHashTables(3)
      .setInputCol("vectorizedFeatures")
      .setOutputCol("hashValues")
      .fit(countVectorizedFeaturesDataFramePartitioned)

    val pairwiseSim = minHashModel
      .approxSimilarityJoin(
        countVectorizedFeaturesDataFramePartitioned,
        countVectorizedFeaturesDataFramePartitioned,
        pairWiseDistanceThreshold,
        "distance"
      )

    pairwiseSim
  }

  def propClustering(
      triplesWithNumericLiteral: RDD[graph.Triple]
  ): RDD[(String, mutable.Set[(String, String, Double)])] = {

    val subMap = triplesWithNumericLiteral.map(
      f =>
        (
          f.getSubject.toString,
          (
            f.getSubject.toString,
            f.getPredicate.toString,
            DistADUtil.getNumber(f.getObject.toString())
          )
        )
    )
    val initialSet = mutable.Set.empty[(String, String, Double)]
    val addToSet =
      (s: mutable.Set[(String, String, Double)], v: (String, String, Double)) =>
        s += v
    val mergePartitionSets = (
        p1: mutable.Set[(String, String, Double)],
        p2: mutable.Set[(String, String, Double)]
    ) => p1 ++= p2
    val uniqueByKey =
      subMap.aggregateByKey(initialSet)(addToSet, mergePartitionSets)

    uniqueByKey
  }

  def propWithSubject(a: RDD[graph.Triple]): RDD[(String, String)] =
    a.map(
      f =>
        (
          DistADUtil.getLocalName(f.getSubject),
          DistADUtil.getLocalName(f.getPredicate)
        )
    )
}
