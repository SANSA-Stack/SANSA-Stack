package net.sansa_stack.examples.flink.rdf

import scala.collection.mutable
import java.io.File
import org.apache.flink.api.scala.ExecutionEnvironment
import net.sansa_stack.rdf.flink.data.RDFGraphLoader
import net.sansa_stack.rdf.flink.stats.RDFStatistics

object RDFStats {

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.out)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String, output: String): Unit = {

    val rdf_stats_file = new File(input).getName

    println("======================================")
    println("|        RDF Statistic example       |")
    println("======================================")

    val env = ExecutionEnvironment.getExecutionEnvironment

    val rdfgraph = RDFGraphLoader.loadFromFile(input, env)

    // compute  criterias
    val rdf_statistics = RDFStatistics(rdfgraph, env)
    val stats = rdf_statistics.run()
    rdf_statistics.voidify(stats, rdf_stats_file, output)
  }

  case class Config(
    in:  String = "",
    out: String = "")

  // the CLI parser
  val parser = new scopt.OptionParser[Config]("RDF Dataset Statistics Example") {

    head("RDF Dataset Statistics Example")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data (in N-Triples format)")

    opt[String]('o', "out").required().valueName("<directory>").
      action((x, c) => c.copy(out = x)).
      text("the output directory")

    help("help").text("prints this usage text")
  }
}