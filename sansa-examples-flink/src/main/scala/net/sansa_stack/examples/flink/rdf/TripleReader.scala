package net.sansa_stack.examples.flink.rdf

import scala.collection.mutable
import org.apache.flink.api.scala.ExecutionEnvironment
import net.sansa_stack.rdf.flink.data.RDFGraphLoader

object TripleReader {
 def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println(
        "Usage: Triple reader <input>")
      System.exit(1)
    }
    val input = args(0)
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
    println("|        Triple reader example       |")
    println("======================================")

    val env = ExecutionEnvironment.getExecutionEnvironment

    val rdfgraph = RDFGraphLoader.loadFromFile(input, env)

    rdfgraph.triples.first(10).print()
  }
}
