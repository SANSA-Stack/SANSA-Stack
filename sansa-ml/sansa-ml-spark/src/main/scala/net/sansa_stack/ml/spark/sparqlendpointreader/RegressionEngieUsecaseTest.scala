package net.sansa_stack.ml.spark.sparqlendpointreader

import com.fasterxml.jackson.databind.ObjectMapper
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.jena.riot.Lang
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Row, SparkSession, functions}
import org.apache.spark.sql.functions.{asc, col, first, lit, to_timestamp}
import org.apache.spark.sql.types._

class RegressionEngieUsecaseTest {
  var host: String = ""
  var sparql: String = ""
  var port: String = ""
  var sparqlQuery: String = ""
  var outputFileName: String = ""
  var hdfsHost: String = ""

  def setOutputFileName(
      outputFileName: String
  ): RegressionEngieUsecaseTest = {
    if (outputFileName == null) {
      throw new Exception("outputFileName can not be null")
    }

    this.outputFileName = outputFileName
    this
  }

  def setHdfsHost(hdfsHost: String): RegressionEngieUsecaseTest = {
    if (hdfsHost == null) {
      throw new Exception("hdfsHost can not be null")
    }

    this.hdfsHost = hdfsHost
    this
  }

  def setHost(host: String): RegressionEngieUsecaseTest = {
    if (host == null) {
      throw new Exception("URL can not be null")
    }

    this.host = host
    this
  }

  def setSparql(sparql: String): RegressionEngieUsecaseTest = {
    if (sparql == null) {
      throw new Exception("URL can not be null")
    }
    if (sparql.trim.isEmpty) {
      throw new Exception("Sparql can not be empty")
    }

    this.sparql = sparql
    this
  }

  def setPort(port: String): RegressionEngieUsecaseTest = {
    if (port == null) {
      throw new Exception("PORT can not be null")
    }
    if (port.trim.isEmpty) {
      throw new Exception("PORT can not be empty")
    }
    if (!port.forall(Character.isDigit)) {
      throw new Exception("PORT is not valid. Your url is " + port)
    }
    this.port = port
    this
  }

  def setSparqlQuery(sparqlQuery: String): RegressionEngieUsecaseTest = {
    if (sparqlQuery == null) {
      throw new Exception("SparqlQuery can not be null")
    }
    if (sparqlQuery.trim.isEmpty) {
      throw new Exception("SparqlQuery can not be empty")
    }
    this.sparqlQuery = sparqlQuery
    this
  }

  def main(args: Array[String]): Unit = {
    run()
  }

  def run(): Unit = {

//    val sparqlEndpointReaderDeTrusty =
//      new SparqlEndpointReaderDeTrusty()
//        .setHost(host)
//        .setPort(port)
//        .setSparql(sparql)
//        .setHdfsHost(hdfsHost)
//        .setOutputFileName("output.ttl")
//        .setSparqlQuery(
//          sparqlQuery
//        )
    val fileAddress = "/home/farshad/Desktop/output.ttl" //sparqlEndpointReaderDeTrusty.getDataAddressOnHDFS()
    val spark = SparkSession.builder
      .appName(s"regression")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.crossJoin.enabled", true)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val lang = Lang.TURTLE
    val triples = spark.rdf(lang)(fileAddress)
    triples.take(5).foreach(println(_))
    println(triples.count)

    val df = triples.toDF()
    df.show(false)

    var features = df
      .groupBy("s")
      .pivot("p")
      .agg(first("o"))
      .filter(!col("http://engie/vocab/ZONE").isNull)
    features.show(false)

    features = features.withColumn(
      "http://engie/vocab/CONNECTIONS",
      col("http://engie/vocab/CONNECTIONS").cast(IntegerType)
    )
    features = features
      .withColumn(
        "newDate",
        functions.concat(
          col("http://engie/vocab/DATE"),
          lit(" "),
          col(
            "http://engie/vocab/HOUR"
          )
        )
      )
    features = features
      .withColumn(
        "newDateCasted",
        to_timestamp(col("newDate"), "yyyy-MM-dd H")
      )
      .withColumn(
        "newDateCastedLong",
        col("newDateCasted").cast(LongType)
      )

    features = features.sort(asc("newDateCastedLong"))

    features = features.drop("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")

    var indexer = new StringIndexer()
      .setInputCol("http://engie/vocab/SOURCE")
      .setOutputCol("http://engie/vocab/SOURCE_index")
      .setHandleInvalid("keep")
    features = indexer.fit(features).transform(features)

    indexer = new StringIndexer()
      .setInputCol("http://engie/vocab/ZONE")
      .setOutputCol("http://engie/vocab/ZONE_index")
      .setHandleInvalid("keep")
    features = indexer.fit(features).transform(features)

    features.show(false)

    val featureColumns = Array(
      "newDateCastedLong",
      "http://engie/vocab/SOURCE_index",
      "http://engie/vocab/ZONE_index"
    )
    val assembler = new VectorAssembler()
      .setInputCols(featureColumns)
      .setOutputCol("indexedFeatures")
      .setHandleInvalid("keep")
    val output = assembler.transform(features)
    output.show(false)

    val rf = new RandomForestRegressor()
      .setLabelCol("http://engie/vocab/CONNECTIONS")
      .setFeaturesCol("indexedFeatures")

    val model = rf.fit(output)

    var predictions = model.transform(output)

    predictions = predictions
      .select(
        "s",
        "newDateCastedLong",
        "prediction",
        "http://engie/vocab/CONNECTIONS"
      )

    var newResultJson: Map[String, Map[String, Double]] = Map(
      "prediction" -> Map()
    )
    import org.apache.spark.sql.functions._
    val c: Array[Row] =
      predictions
        .select("newDateCastedLong", "prediction")
        .sort(asc("newDateCastedLong"))
        .collect()
    c.foreach(a => {
      val b = newResultJson.get("prediction") match {
        case Some(b: Map[String, Double]) =>
          b + (a.get(0).toString -> a
            .get(1)
            .toString
            .toDouble)
        case None => Map(a.get(0).toString -> a.get(1).toString.toDouble)
      }
      newResultJson = newResultJson + ("prediction" -> b)
    })
    import com.fasterxml.jackson.module.scala.DefaultScalaModule

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val resultJson = mapper.writeValueAsString(newResultJson)

    val outputFullPath = hdfsHost + "user/root/" + outputFileName
    val path = new Path(outputFullPath)
    val conf = new Configuration()
    conf.set("fs.defaultFS", hdfsHost)
    val fs = FileSystem.get(conf)
    val os = fs.create(path)
    os.write(resultJson.toString().getBytes)
    fs.close()
    println(outputFullPath)
  }

}
