package net.sansa_stack.examples.spark.ml.outliers.anomalydetection

import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import java.net.{ URI => JavaURI }
import net.sansa_stack.ml.spark.outliers.anomalydetection.{ AnomalyDetection => AlgAnomalyDetection, IQR }
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io._

object AnomalyDetection {
  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.out)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String, outputPath: String): Unit = {

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
    val triplesRDD = spark.rdf(lang)(input)
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

    val outDetection = new AlgAnomalyDetection(triplesRDD, objList, triplesType, JSimThreshold, listSuperType, spark, hypernym)

    val clusterOfSubject = outDetection.run()

    clusterOfSubject.foreach(println)

    val setData = clusterOfSubject.map(f => f._2)

    val listofData = clusterOfSubject.map({
      case (a, (b)) => b.map(f => (f._3.toString().toDouble)).toList
    })

    val listofDataArray = listofData.collect()

    for (listofData <- listofDataArray)
      IQR.iqr(listofData, setData)

  }

  case class Config(
    in:  String = "",
    out: String = "")

  val parser = new scopt.OptionParser[Config]("Anomaly Detection example") {

    head("Anomaly Detection example")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data")

    help("help").text("prints this usage text")
  }
}