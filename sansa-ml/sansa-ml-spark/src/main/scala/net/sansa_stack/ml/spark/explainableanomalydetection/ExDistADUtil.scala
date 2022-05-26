package net.sansa_stack.ml.spark.explainableanomalydetection

import net.sansa_stack.rdf.common.io.riot.error.{
  ErrorParseMode,
  WarningParseMode
}
import net.sansa_stack.rdf.spark.io.{NTripleReader, RDFReader}
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.hadoop.fs.Path
import org.apache.jena.graph
import org.apache.jena.graph.Triple
import org.apache.jena.riot.Lang
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

/** This class gathers all the utilities needed for explainable distributed anomaly
  * detection
  */
object ExDistADUtil {

  val LOG: Logger = Logger.getLogger(ExDistADUtil.getClass)

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
      .config("spark.sql.shuffle.partitions", 200)
      .config("spark.kryoserializer.buffer.max", 2047)
      .config("spark.sql.pivotMaxValues", 100000)
//      .master("local[*]")
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

  /** Writes a dataframe to a file with given path. It handles HDFS and the normal file system
    * @param path
    *   the path of the output file
    * @param data
    *   the dataframe that should be written to a file
    */
  def writeToFile(
      path: String,
      data: DataFrame,
      explanation: String,
      fileSuffix: Int
  ): Unit = {
    val newPath: String = path + "_" + fileSuffix
    if (path.contains("hdfs://")) {
      val stringBuilder: StringBuilder =
        new StringBuilder(explanation).append("\n")
      data
        .coalesce(1)
        .collect()
        .foreach(row => {
          stringBuilder.append(row.mkString(",")).append("\n")
        })

      val fsPath = new Path(newPath)
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
        .drop("indexedFeatures")
        .coalesce(1)
        .write
        .mode(SaveMode.Overwrite)
        .options(tsvWithHeaderOptions)
        .csv(newPath)
      Files.write(
        Paths.get(newPath + File.separator + "explanation.txt"),
        explanation.getBytes(StandardCharsets.UTF_8)
      )
    }
  }

  /**
    * Dispacher function for selecting anomaly detection method
    * @param data array of double values
    * @param config the config file object
    * @return list of possible anomalous values
    */
  def anomalyDetectionMethod(
      data: Array[Double],
      config: ExDistADConfig
  ): Array[Double] = {
    config.anomalyDetectionAlgorithm match {
      case config.IQR    => iqr(data, config.anomalyListSize)
      case config.MAD    => mad(data, config.anomalyListSize)
      case config.ZSCORE => zscore(data, config.anomalyListSize)
      case _             => iqr(data, config.anomalyListSize)
    }
  }

  /**
    * Anomaly Detection method based on Interquartile Range
    * @param data array of double values
    * @param anomalyListSize The minimum number of samples required for anomaly detection method
    * @return list of possible anomalous values
    */
  def iqr(
      data: Array[Double],
      anomalyListSize: Int
  ): Array[Double] = {

    if (data.length < anomalyListSize) {
      return Array[Double]()
    }
    val listofData = data
    val c = listofData.sorted
    val arrMean = new DescriptiveStatistics()
    genericArrayOps(c).foreach(v => arrMean.addValue(v))
    val Q1 = arrMean.getPercentile(25)
    val Q3 = arrMean.getPercentile(75)
    val IQR = Q3 - Q1
    val lowerRange = Q1 - 1.5 * IQR
    val upperRange = Q3 + 1.5 * IQR
    val yse = c.filter(p => (p <= lowerRange || p >= upperRange))
    yse
  }

  /**
    * Returns the median of a array of doubles
    * @param data array of double values
    * @return the median
    */
  def median(data: Array[Double]): Double =
    data.sortWith(_ < _).drop(data.length / 2).head

  /**
    * Returns the mean of a array of doubles
    * @param data array of double values
    * @return the mean
    */
  def meanElements(data: Array[Double]): Double = data.sum / data.length

  /**
    * Returns the variance of a array of doubles
    * @param data array of double values
    * @return the variance
    */
  def variance(data: Array[Double]): Double = {
    val newData = data
    val avg = meanElements(newData)
    newData.map(_.toDouble).map(a => math.pow(a - avg, 2)).sum / newData.size
  }

  /**
    * Returns the standard deviation  of a array of doubles
    * @param data array of double values
    * @return the standard deviation
    */
  def stdDev(data: Array[Double]): Double = math.sqrt(variance(data))

  /**
    * Anomaly Detection method based on Mean Absolute Deviation (MAD)
    * @param data array of double values
    * @param anomalyListSize The minimum number of samples required for anomaly detection method
    * @return list of possible anomalous values
    */
  def mad(
      data: Array[Double],
      anomalyListSize: Int
  ): Array[Double] = {
    if (data.length < anomalyListSize) {
      return Array[Double]()
    }
    val m = median(data)
    var newData: Array[Double] = data
    newData = newData.map(p => Math.abs(p - m))
    val MAD = median(newData)
    val lowerRange = m - 2.5 * MAD
    var upperRange = m + 2.5 * MAD
    val yse = data.filter(p => (p <= lowerRange || p >= upperRange))
    yse
  }

  /**
    * Anomaly Detection method based on Mean Absolute Deviation (MAD)
    * @param data array of double values
    * @param anomalyListSize The minimum number of samples required for anomaly detection method
    * @return list of possible anomalous values
    */
  def zscore(
      data: Array[Double],
      anomalyListSize: Int
  ): Array[Double] = {
    if (data.length < anomalyListSize) {
      return Array[Double]()
    }
    val mean = meanElements(data)
    val standardDV = stdDev(data)
    val newData = data
    val newDataTuple = newData.map(p => (((p - mean) / standardDV), p))
    val threshold = 2
    val yse = newDataTuple.filter(p => (Math.abs(p._1) > threshold))
    yse.map(p => p._2)
  }

  /** Gets an RDD[Triple] and converts it to a dataframe
    * @param data the given RDD[Triple]
    * @return a dataframe containing s,p,o
    */
  def createDF(
      data: RDD[Triple]
  ): DataFrame = {
    import net.sansa_stack.rdf.spark.model.TripleOperations
    val df = data.toDF()
    df
  }

  /**
    * Gets an RDD[Triple] and converts it to a dataset
    * @param data the given RDD[Triple]
    * @return a dataset
    */
  def createDS(
      data: RDD[Triple]
  ): Dataset[Triple] = {
    import net.sansa_stack.rdf.spark.model.TripleOperations
    val ds = data.toDS()
    ds
  }

}
