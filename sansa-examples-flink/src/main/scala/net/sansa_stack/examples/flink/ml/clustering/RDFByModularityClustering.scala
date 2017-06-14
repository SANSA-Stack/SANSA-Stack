package net.sansa_stack.examples.flink.ml.clustering
import scala.collection.mutable
import net.sansa_stack.ml.flink.clustering.{ RDFByModularityClustering => RDFByModularityClusteringAlg }
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object RDFByModularityClustering {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println(
        "Usage: RDFByModularityClustering <input> <output> <numIterations>")
      System.exit(1)
    }
    val graphFile = args(0) //"src/main/resources/Clustering_sampledata.nt" 
    val outputFile = args(1) //"src/main/resources/output"
    val numIterations = args(2).toInt // 5 
    val optionsList = args.drop(3).map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => (opt -> v)
        case _             => throw new IllegalArgumentException("Invalid argument: " + arg)
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

    RDFByModularityClusteringAlg(env, numIterations, graphFile, outputFile)

  }
}