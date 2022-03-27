package net.sansa_stack.ml.spark.sparqlendpointreader

import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.jena.riot.Lang
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, first, to_timestamp}
import org.apache.spark.sql.types._

object RegressionEngieUsecaseObjectBackup {
  var host: String = ""
  var sparql: String = ""
  var port: String = ""
  var sparqlQuery: String = ""
  var outputFileName: String = ""
  var hdfsHost: String = ""

  def main(args: Array[String]): Unit = {
    host = args(0)
    port = args(1)
    sparql = args(2)
    sparqlQuery = args(3)
    outputFileName = args(4)
    hdfsHost = args(5)
    run()
  }

  def run(): Unit = {

    val sparqlEndpointReaderDeTrusty =
      new SparqlEndpointReaderDeTrusty()
        .setHost(host)
        .setPort(port)
        .setSparql(sparql)
        .setHdfsHost(hdfsHost)
        .setOutputFileName("output.ttl")
        .setSparqlQuery(
          sparqlQuery
        )
    val fileAddress = sparqlEndpointReaderDeTrusty.getDataAddressOnHDFS()
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
    features.show(false)

    features = features.withColumn(
      "http://engie/vocab/CONNECTIONS",
      col("http://engie/vocab/CONNECTIONS").cast(IntegerType)
    )
    features = features
      .withColumn(
        "http://engie/vocab/DATE",
        to_timestamp(col("http://engie/vocab/DATE"))
      )
      .withColumn(
        "http://engie/vocab/DATE",
        col("http://engie/vocab/DATE").cast(LongType)
      )
    features = features.withColumn(
      "http://engie/vocab/HOUR",
      col("http://engie/vocab/HOUR").cast(IntegerType)
    )
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
      "http://engie/vocab/DATE",
      "http://engie/vocab/HOUR",
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

    val predictions = model.transform(output)

    predictions
      .select("s", "prediction", "http://engie/vocab/CONNECTIONS")
      .show(10, false)

    val resultJson = df.toJSON.collect.mkString("[", ",", "]")

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
