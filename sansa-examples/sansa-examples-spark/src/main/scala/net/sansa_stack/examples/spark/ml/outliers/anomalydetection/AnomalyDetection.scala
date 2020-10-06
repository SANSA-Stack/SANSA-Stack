package net.sansa_stack.examples.spark.ml.outliers.anomalydetection

import scala.collection.mutable

import net.sansa_stack.ml.spark.outliers.anomalydetection._
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.riot.Lang
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Dataset, Row, SaveMode, SparkSession }
import org.apache.spark.storage.StorageLevel

object AnomalyDetection {
  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.threshold, config.anomalyListLimit, config.numofpartition, config.out)
      case None =>
        println(parser.usage)
    }
  }

  def run(
    input: String,
    JSimThreshold: Double,
    anomalyListLimit: Int,
    numofpartition: Int,
    output: String): Unit = {

    println("==================================================")
    println("|        Distributed Anomaly Detection           |")
    println("==================================================")

    val spark = SparkSession.builder
      .appName(s"Anomaly Detection example ( $input )")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // N-Triples Reader
    val lang = Lang.NTRIPLES
    val triplesRDD = spark.rdf(lang)(input).repartition(numofpartition).persist()

    // predicated that are not interesting for evaluation
    val wikiList = List("wikiPageRevisionID,wikiPageID")

    // filtering numeric literal having xsd type double,integer,nonNegativeInteger and squareKilometre
    val objList = List(
      "http://www.w3.org/2001/XMLSchema#double",
      "http://www.w3.org/2001/XMLSchema#integer",
      "http://www.w3.org/2001/XMLSchema#nonNegativeInteger",
      "http://dbpedia.org/datatype/squareKilometre")

    // helful for considering only Dbpedia type as their will be yago type,wikidata type also
    val triplesType = List("http://dbpedia.org/ontology")

    // some of the supertype which are present for most of the subject
    val listSuperType = List(
      "http://dbpedia.org/ontology/Activity", "http://dbpedia.org/ontology/Organisation",
      "http://dbpedia.org/ontology/Agent", "http://dbpedia.org/ontology/SportsLeague",
      "http://dbpedia.org/ontology/Person", "http://dbpedia.org/ontology/Athlete",
      "http://dbpedia.org/ontology/Event", "http://dbpedia.org/ontology/Place",
      "http://dbpedia.org/ontology/PopulatedPlace", "http://dbpedia.org/ontology/Region",
      "http://dbpedia.org/ontology/Species", "http://dbpedia.org/ontology/Eukaryote",
      "http://dbpedia.org/ontology/Location")

    // hypernym URI
    val hypernym = "http://purl.org/linguistics/gold/hypernym"

    var clusterOfSubject: RDD[(Set[(String, String, Object)])] = null
    println("AnomalyDetection-using ApproxSimilarityJoin function with the help of HashingTF ")

    val outDetection = new AnomalyWithHashingTF(triplesRDD, objList, triplesType, JSimThreshold, listSuperType, spark, hypernym, numofpartition)
    clusterOfSubject = outDetection.run()

    val setData = clusterOfSubject.repartition(1000).persist(StorageLevel.MEMORY_AND_DISK)
    val setDataStore = setData.map(f => f.toSeq)

    val setDataSize = setDataStore.filter(f => f.size > anomalyListLimit)

    val test = setDataSize.map(f => outDetection.iqr2(f, anomalyListLimit))

    val testfilter = test.filter(f => f.size > 0) // .distinct()
    val testfilterDistinct = testfilter.flatMap(f => f)
    testfilterDistinct.saveAsTextFile(output)
    setData.unpersist()

    spark.stop()
  }

  case class Config(
    in: String = "",
    threshold: Double = 0.0,
    anomalyListLimit: Int = 0,
    numofpartition: Int = 0,
    out: String = "")

  val parser = new scopt.OptionParser[Config]("SANSA -Outlier Detection") {

    head("Detecting Numerical Outliers in dataset")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains RDF data (in N-Triples format)")

    // Jaccard similarity threshold value
    opt[Double]('t', "threshold").required().
      action((x, c) => c.copy(threshold = x)).
      text("the Jaccard Similarity value")

    // number of partition
    opt[Int]('a', "numofpartition").required().
      action((x, c) => c.copy(numofpartition = x)).
      text("Number of partition")

    // List limit for calculating IQR
    opt[Int]('c', "anomalyListLimit").required().
      action((x, c) => c.copy(anomalyListLimit = x)).
      text("the outlier List Limit")

    // output file path
    opt[String]('o', "output").required().valueName("<directory>").
      action((x, c) => c.copy(out = x)).
      text("the output directory")

    help("help").text("prints this usage text")
  }
}
