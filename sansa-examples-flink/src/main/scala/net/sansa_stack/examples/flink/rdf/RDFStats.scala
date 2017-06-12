package net.sansa_stack.examples.flink.rdf

import scala.collection.mutable
import java.io.File
import org.apache.flink.api.scala.ExecutionEnvironment
import net.sansa_stack.rdf.flink.data.RDFGraphLoader
import net.sansa_stack.rdf.flink.stats.RDFStatistics

object RDFStats {
  def main(args: Array[String]) = {
    if (args.length < 2) {
      System.err.println(
        "Usage: RDF Statistics <input> <output>")
      System.exit(1)
    }
    val input = args(0) //"src/main/resources/rdf.nt"
    val rdf_stats_file = new File(input).getName
    val output = args(1)
    val optionsList = args.drop(1).map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => (opt -> v)
        case _             => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }
    val options = mutable.Map(optionsList: _*)

    options.foreach {
      case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
    }
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

}