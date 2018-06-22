package net.sansa_stack.ml.flink

import scala.collection.mutable

import org.apache.flink.api.scala.ExecutionEnvironment

import net.sansa_stack.ml.flink.clustering.RDFByModularityClustering

/**
 * @author Gezim Sejdiu
 */
object App {
  def main(args: Array[String]) {
    /**
     * if (args.length < 3) {
     * System.err.println(
     * "Usage: RDFByModularityClustering <input> <output> <numIterations>")
     * System.exit(1)
     * }
     */
    val graphFile = "Clustering_sampledata.nt" // args(0)
    val outputFile = "output" // args(1)
    val numIterations = 5 // args(2).toInt
    val optionsList = args.drop(3).map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => (opt -> v)
        case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }
    val options = mutable.Map(optionsList: _*)

    options.foreach {
      case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
    }
    println("============================================")
    println("| RDF By Modularity Clustering example     |")
    println("============================================")

    val env = ExecutionEnvironment.getExecutionEnvironment

    RDFByModularityClustering(env, numIterations, graphFile, outputFile)

  }
}
