package net.sansa_stack.examples.spark.rdf

import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.model._
import org.apache.jena.graph.NodeFactory
import org.apache.jena.riot.Lang
import org.apache.spark.sql.SparkSession

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
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    println("======================================")
    println("|        Triple Ops example       |")
    println("======================================")

    val lang = Lang.NTRIPLES
    val triples = spark.rdf(lang)(input)

    // Triples filtered by subject ( "http://dbpedia.org/resource/Charles_Dickens" )
    println("All triples related to Dickens:\n" + triples.find(Some(NodeFactory.createURI("http://dbpedia.org/resource/Charles_Dickens")), None, None).collect().mkString("\n"))

    // Triples filtered by predicate ( "http://dbpedia.org/ontology/influenced" )
    println("All triples for predicate influenced:\n" + triples.find(None, Some(NodeFactory.createURI("http://dbpedia.org/ontology/influenced")), None).collect().mkString("\n"))

    // Triples filtered by object ( <http://dbpedia.org/resource/Henry_James> )
    println("All triples influenced by Henry_James:\n" + triples.find(None, None, Some(NodeFactory.createURI("http://dbpedia.org/resource/Henry_James"))).collect().mkString("\n"))

    println("Number of triples: " + triples.distinct.count())
    println("Number of subjects: " + triples.getSubjects.distinct.count())
    println("Number of predicates: " + triples.getPredicates.distinct.count())
    println("Number of objects: " + triples.getObjects.distinct.count())

    val subjects = triples.filterSubjects(_.isURI()).collect.mkString("\n")

    val predicates = triples.filterPredicates(_.isVariable()).collect.mkString("\n")
    val objects = triples.filterObjects(_.isLiteral()).collect.mkString("\n")

    // graph.getTriples.take(5).foreach(println(_))

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
