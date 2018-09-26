package net.sansa_stack.ml.spark.clustering.utils

import java.io.PrintWriter
import java.util.regex.Pattern

import com.typesafe.config.{Config, ConfigFactory}
import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.jena.graph.Triple
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.riot.Lang


object mergeData extends Serializable {

  implicit val formats = DefaultFormats
  val conf = ConfigFactory.load()
  val fileWriter = new PrintWriter(conf.getString("yelp.slipo.merged_file"))

  def mergeYelpSlipoData(slipoRDD: RDD[Triple], yelpRDD: RDD[Triple], conf: Config): Unit = {
    val yelpCategories = yelpRDD.filter(triple => triple.getPredicate.hasURI("http://slipo.eu/hasYelpCategory") || triple.getPredicate.hasURI("http://slipo.eu/hasRating"))
    val mergedRDD = yelpCategories.union(slipoRDD).persist()

    /* mergedRDD.foreach(x => {
      val objStr =
        if (x.getObject.isLiteral) {
          val objectParts = x.getObject.toString().split(Pattern.quote("^^"))
          s"${objectParts(0)}^^<${x.getObject.getLiteralDatatypeURI}>"
        } else {
          //s"<${x.getObject}>"
          s""
        }
      fileWriter.println(s"<${x.getSubject}> <${x.getPredicate}> $objStr .")
    }) */
    // fileWriter.println()
    mergedRDD.saveAsNTriplesFile("data/merged_tomtom_yelp")
  }


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master(conf.getString("slipo.spark.master"))
      .config("spark.serializer", conf.getString("slipo.spark.serializer"))
      .config("spark.executor.memory", conf.getString("slipo.spark.executor.memory"))
      .config("spark.driver.memory", conf.getString("slipo.spark.driver.memory"))
      .config("spark.driver.maxResultSize", conf.getString("slipo.spark.driver.maxResultSize"))
      .appName(conf.getString("slipo.spark.app.name"))
      .getOrCreate()

    var lang = Lang.NTRIPLES
    val triples = spark.rdf(lang)("input")
    val slipoDataRDD: RDD[Triple] = NTripleReader.load(spark, conf.getString("slipo.merge.input")).persist()
    val yelpDataRDD: RDD[Triple] = NTripleReader.load(spark, conf.getString("yelp.data.input")).persist()
    mergeYelpSlipoData(slipoDataRDD, yelpDataRDD, conf)
    fileWriter.close()
  }
}
