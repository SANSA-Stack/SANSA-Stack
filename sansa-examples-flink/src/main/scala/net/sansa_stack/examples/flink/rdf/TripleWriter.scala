package net.sansa_stack.examples.flink.rdf
import scala.collection.mutable
import org.apache.flink.api.scala.ExecutionEnvironment
import net.sansa_stack.rdf.flink.data.{RDFGraphLoader, RDFGraphWriter}

object TripleWriter {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(
        "Usage: Triple writer <input> <output>")
      System.exit(1)
    }
    val input = args(0) //"src/main/resources/rdf.nt" 
    val output = args(1)
    val optionsList = args.drop(2).map { arg =>
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
    println("|        Triple writer example       |")
    println("======================================")

    val env = ExecutionEnvironment.getExecutionEnvironment

    val rdfgraph = RDFGraphLoader.loadFromFile(input, env)

    RDFGraphWriter.writeToFile(rdfgraph, output)

    env.execute(s"Triple writer example ($input)")
  }
}
