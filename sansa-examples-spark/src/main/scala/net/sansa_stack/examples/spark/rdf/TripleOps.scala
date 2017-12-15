package net.sansa_stack.examples.spark.rdf

import java.net.{ URI => JavaURI }
import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.{ JenaSparkRDDOps, TripleRDD }
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object TripleOps {

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String): Unit = {

    val spark = SparkSession.builder
      .appName(s"Triple Ops example  $input")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    println("======================================")
    println("|        Triple Ops example       |")
    println("======================================")

    val ops = JenaSparkRDDOps(spark.sparkContext)
    import ops._

    val triplesRDD = NTripleReader.load(spark, JavaURI.create(input))

    val graph: TripleRDD = triplesRDD

    //Triples filtered by subject ( "http://dbpedia.org/resource/Charles_Dickens" )
    println("All triples related to Dickens:\n" + graph.find(URI("http://dbpedia.org/resource/Charles_Dickens"), ANY, ANY).collect().mkString("\n"))

    //Triples filtered by predicate ( "http://dbpedia.org/ontology/influenced" )
    println("All triples for predicate influenced:\n" + graph.find(ANY, URI("http://dbpedia.org/ontology/influenced"), ANY).collect().mkString("\n"))

    //Triples filtered by object ( <http://dbpedia.org/resource/Henry_James> )
    println("All triples influenced by Henry_James:\n" + graph.find(ANY, ANY, URI("<http://dbpedia.org/resource/Henry_James>")).collect().mkString("\n"))

    println("Number of triples: " + graph.find(ANY, ANY, ANY).distinct.count())
    println("Number of subjects: " + graph.getSubjects.distinct.count())
    println("Number of predicates: " + graph.getPredicates.distinct.count())
    println("Number of objects: " + graph.getPredicates.distinct.count())

    val subjects = graph.filterSubjects(_.isURI()).collect.mkString("\n")

    val predicates = graph.filterPredicates(_.isVariable()).collect.mkString("\n")
    val objects = graph.filterObjects(_.isLiteral()).collect.mkString("\n")

    //graph.getTriples.take(5).foreach(println(_))

    spark.stop

  }
  // the config object
  case class Config(in: String = "")

  // the CLI parser
  val parser = new scopt.OptionParser[Config]("Triple Ops example") {

    head(" Triple Ops example")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data (in N-Triples format)")
    help("help").text("prints this usage text")
  }
}