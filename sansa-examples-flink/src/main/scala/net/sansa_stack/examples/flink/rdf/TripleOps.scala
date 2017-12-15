package net.sansa_stack.examples.flink.rdf

import scala.collection.mutable
import org.apache.flink.api.scala.ExecutionEnvironment
import net.sansa_stack.rdf.flink.data.RDFGraphLoader
import org.apache.flink.api.scala._

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

    println("======================================")
    println("|        Triple Ops example       |")
    println("======================================")
    val env = ExecutionEnvironment.getExecutionEnvironment

    val rdfgraph = RDFGraphLoader.loadFromFile(input, env)

    rdfgraph.triples.collect().take(4).foreach(println(_))
    //Triples filtered by subject ( "http://dbpedia.org/resource/Charles_Dickens" )
    println("All triples related to Dickens:\n" + rdfgraph.find(Some("http://commons.dbpedia.org/resource/Category:Places"), None, None).collect().mkString("\n"))

    //Triples filtered by predicate ( "http://dbpedia.org/ontology/influenced" )
    println("All triples for predicate influenced:\n" + rdfgraph.find(None, Some("http://dbpedia.org/ontology/influenced"), None).collect().mkString("\n"))

    //Triples filtered by object ( <http://dbpedia.org/resource/Henry_James> )
    println("All triples influenced by Henry_James:\n" + rdfgraph.find(None, None, Some("<http://dbpedia.org/resource/Henry_James>")).collect().mkString("\n"))

    //println("Number of triples: " + rdfgraph.triples.distinct.count())
    println("Number of subjects: " + rdfgraph.getSubjects.map(_.toString).distinct().count)
    println("Number of predicates: " + rdfgraph.getPredicates.map(_.toString).distinct.count())
    println("Number of objects: " + rdfgraph.getPredicates.map(_.toString).distinct.count())

  }
  case class Config(in: String = "")

  val parser = new scopt.OptionParser[Config]("Triple Ops example") {

    head(" Triple Ops example")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data (in N-Triples format)")
    help("help").text("prints this usage text")
  }

}