package net.sansa_stack.examples.flink.rdf

import scala.collection.mutable

import net.sansa_stack.rdf.flink.io._
import net.sansa_stack.rdf.flink.model._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.jena.riot.Lang

object TripleReader {

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
    println("|        Triple reader example       |")
    println("======================================")

    val env = ExecutionEnvironment.getExecutionEnvironment

    val triples = env.rdf(Lang.NTRIPLES)(input)
    triples.getTriples().first(10).print()
  }

  case class Config(in: String = "")

  val parser = new scopt.OptionParser[Config]("Triple reader example") {

    head(" Triple reader example")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data (in N-Triples format)")

    help("help").text("prints this usage text")
  }
}
