package net.sansa_stack.examples.spark.ml.clustering

import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{ Level, Logger }
import net.sansa_stack.ml.spark.clustering.{ RDFByModularityClustering => RDFByModularityClusteringAlg }

object RDFByModularityClusteringExample {

  def main(args: Array[String]) = {
    if (args.length < 3) {
      System.err.println(
        "Usage: RDFByModularityClustering <input> <output> <numIterations>")
      System.exit(1)
    }
    val graphFile = args(0) //"src/main/resources/Clustering_sampledata.nt",
    val outputFile = args(1)
    val numIterations = args(2).toInt
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

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .appName(" RDF By Modularity Clustering example (" + graphFile + ")")
      .getOrCreate()
    Logger.getRootLogger.setLevel(Level.ERROR)

    RDFByModularityClusteringAlg(sparkSession.sparkContext, numIterations, graphFile, outputFile)

    sparkSession.stop

  }

}