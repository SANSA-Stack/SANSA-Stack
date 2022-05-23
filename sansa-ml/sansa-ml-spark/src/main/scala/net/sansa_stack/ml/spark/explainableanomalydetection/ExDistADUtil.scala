package net.sansa_stack.ml.spark.explainableanomalydetection

import net.sansa_stack.rdf.common.io.riot.error.{
  ErrorParseMode,
  WarningParseMode
}
import net.sansa_stack.rdf.spark.io.{NTripleReader, RDFReader}
import org.apache.commons.lang3.StringUtils
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.hadoop.fs.Path
import org.apache.jena.graph
import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.riot.Lang
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

/** This class gathers all the utilities needed for distributed anomaly
  * detection
  */
object ExDistADUtil {

  val LOG: Logger = Logger.getLogger(ExDistADUtil.getClass)
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
      .config("spark.sql.shuffle.partitions", 200)
      .config("spark.kryoserializer.buffer.max", 2047)
      .config("spark.sql.pivotMaxValues", 100000)
      .master("local[*]")
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

  def getValue(a: String): String = {
    try {
      if (a.contains("^")) {
        val c = a.indexOf('^')
        var subject = a.substring(0, c)
        subject = StringUtils.stripStart(subject, "\"")
        subject = StringUtils.stripEnd(subject, "\"")
        subject
      } else {
        a
      }
    } catch {
      case e: Throwable => ""
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

  val convertStringToInt: UserDefinedFunction = udf({ (v: String) =>
    getValue(v).toString.toDouble
  })

  val convertStringToString: UserDefinedFunction = udf({ (v: String) =>
    getValue(v).toString
  })

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
      data.show(false)

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

  /** Anomaly Detection method based on Interquartile Range
    */
  private def iqr(
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

  def median(data: Array[Double]): Double =
    data.sortWith(_ < _).drop(data.length / 2).head

  /** Anomaly Detection method based on Mean Absolute Deviation (MAD)*/
  private def mad(
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

  def meanElements(data: Array[Double]): Double = data.sum / data.length
  def variance(data: Array[Double]): Double = {
    val newData = data
    val avg = meanElements(newData)
    newData.map(_.toDouble).map(a => math.pow(a - avg, 2)).sum / newData.size
  }
  def stdDev(xs: Array[Double]): Double = math.sqrt(variance(xs))

  /** Anomaly Detection method based on Z-Score*/
  private def zscore(
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

  def createDS(
      data: RDD[Triple]
  ): Dataset[Triple] = {
    import net.sansa_stack.rdf.spark.model.TripleOperations
    val ds = data.toDS()
    ds
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

  def createDFWithConversionToString(
      data: RDD[Triple]
  ): DataFrame = {
    import net.sansa_stack.rdf.spark.model.TripleOperations
    val df = data.toDF()
    df.withColumn("o", convertStringToString(col("o")))
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

}
