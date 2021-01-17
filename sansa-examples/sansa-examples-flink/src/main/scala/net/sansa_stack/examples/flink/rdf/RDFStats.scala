package net.sansa_stack.examples.flink.rdf

import java.io.File

import net.sansa_stack.rdf.flink.io._
import net.sansa_stack.rdf.flink.stats._
import org.apache.flink.api.scala.ExecutionEnvironment

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

    val triples = env.rdf(Lang.NTRIPLES)(input)

    // compute stats
    val rdf_statistics = triples.stats
      .voidify(rdf_stats_file, output)
  }

  case class Config(
    in: String = "",
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
