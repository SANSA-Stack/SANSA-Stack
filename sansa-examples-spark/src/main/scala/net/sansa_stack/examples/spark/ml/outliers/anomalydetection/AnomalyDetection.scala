package net.sansa_stack.examples.spark.ml.outliers.anomalydetection

import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import java.net.{ URI => JavaURI }
import net.sansa_stack.ml.spark.outliers.anomalydetection.{ AnomalyDetection => AlgAnomalyDetection }
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode

object AnomalyDetection {
  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.threshold, config.anomalyListLimit, config.numofpartition, config.out)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String, threshold: Double,
          anomalyListLimit: Int,
          numofpartition:   Int, output: String): Unit = {

    println("==================================================")
    println("|        Distributed Anomaly Detection           |")
    println("==================================================")

    val spark = SparkSession.builder
      .appName(s"Anomaly Detection example ( $input )")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    //N-Triples Reader
    val lang = Lang.NTRIPLES
    val triplesRDD = spark.rdf(lang)(input).repartition(numofpartition).persist()
    //constant parameters defined
    val JSimThreshold = 0.6

    val objList = List(
      "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString",
      "http://www.w3.org/2001/XMLSchema#date")

    //clustering of subjects are on the basis of rdf:type specially object with wikidata and dbpedia.org
    //val triplesType = List("http://www.wikidata.org", "http://dbpedia.org/ontology")
    val triplesType = List("http://dbpedia.org/ontology")

    //some of the supertype which are present for most of the subject
    val listSuperType = List(
      "http://dbpedia.org/ontology/Activity", "http://dbpedia.org/ontology/Organisation",
      "http://dbpedia.org/ontology/Agent", "http://dbpedia.org/ontology/SportsLeague",
      "http://dbpedia.org/ontology/Person", "http://dbpedia.org/ontology/Athlete",
      "http://dbpedia.org/ontology/Event", "http://dbpedia.org/ontology/Place",
      "http://dbpedia.org/ontology/PopulatedPlace", "http://dbpedia.org/ontology/Region",
      "http://dbpedia.org/ontology/Species", "http://dbpedia.org/ontology/Eukaryote",
      "http://dbpedia.org/ontology/Location")
    //hypernym URI
    val hypernym = "http://purl.org/linguistics/gold/hypernym"

    val outDetection = new AlgAnomalyDetection(triplesRDD, objList, triplesType, JSimThreshold, listSuperType, spark, hypernym, numofpartition)

    val clusterOfSubject = outDetection.run()

    clusterOfSubject.take(10).foreach(println)

    val setData = clusterOfSubject.repartition(numofpartition).persist.map(f => f._2.toSeq)

    //calculating IQR and saving output to the file
    val listofDataArray = setData.collect()
    var a: Dataset[Row] = null

    for (listofDatavalue <- listofDataArray) {
      a = outDetection.iqr1(listofDatavalue, anomalyListLimit)
      if (a != null)
        a.select("dt").coalesce(1).write.format("text").mode(SaveMode.Append) save (output)
    }
    setData.unpersist()
    spark.stop()
  }

  case class Config(
    in:               String = "",
    threshold:        Double = 0.0,
    anomalyListLimit: Int    = 0,
    numofpartition:   Int    = 4,
    out:              String = "")

  val parser = new scopt.OptionParser[Config]("Anomaly Detection example") {

    head("Anomaly Detection example")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data")

    opt[Double]('t', "threshold").required().
      action((x, c) => c.copy(threshold = x)).
      text("the Jaccard Similarity value")

    opt[Int]('a', "numofpartition").required().
      action((x, c) => c.copy(numofpartition = x)).
      text("Number of partition")

    opt[Int]('c', "anomalyListLimit").required().
      action((x, c) => c.copy(anomalyListLimit = x)).
      text("the outlier List Limit")

    opt[String]('o', "output").required().valueName("<directory>").
      action((x, c) => c.copy(out = x)).
      text("the output directory")

    help("help").text("prints this usage text")
  }
}