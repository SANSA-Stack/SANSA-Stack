package net.sansa_stack.examples.flink.rdf

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.jena.graph.NodeFactory

import net.sansa_stack.rdf.flink.io._
import net.sansa_stack.rdf.flink.model._

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

    val triples = env.rdf(Lang.NTRIPLES)(input)

    triples.getTriples().collect().take(4).foreach(println(_))
    // Triples filtered by subject ( "http://dbpedia.org/resource/Charles_Dickens" )
    println("All triples related to Dickens:\n" + triples.find(Some(NodeFactory.createURI("http://commons.dbpedia.org/resource/Category:Places")), None, None).collect().mkString("\n"))

    // Triples filtered by predicate ( "http://dbpedia.org/ontology/influenced" )
    println("All triples for predicate influenced:\n" + triples.find(None, Some(NodeFactory.createURI("http://dbpedia.org/ontology/influenced")), None).collect().mkString("\n"))

    // Triples filtered by object ( <http://dbpedia.org/resource/Henry_James> )
    println("All triples influenced by Henry_James:\n" + triples.find(None, None, Some(NodeFactory.createURI("<http://dbpedia.org/resource/Henry_James>"))).collect().mkString("\n"))

    // println("Number of triples: " + rdfgraph.triples.distinct.count())
    println("Number of subjects: " + triples.getSubjects.map(_.toString).distinct().count)
    println("Number of predicates: " + triples.getPredicates.map(_.toString).distinct.count())
    println("Number of objects: " + triples.getPredicates.map(_.toString).distinct.count())

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
