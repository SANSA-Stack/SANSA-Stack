package net.sansa_stack.examples.spark.ml.clustering

import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.graphx.GraphLoader
import net.sansa_stack.ml.spark.clustering.{ RDFGraphPICClustering => RDFGraphPICClusteringAlg }

object RDFGraphPIClustering {
  def main(args: Array[String]) = {
    if (args.length < 3) {
      System.err.println(
        "Usage: RDFGraphPIClustering <input> <k> <numIterations>")
      System.exit(1)
    }
    val input = args(0) //"src/main/resources/BorderFlow_Sample1.txt"
    val k = args(1).toInt
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
    println("| Power Iteration Clustering   example     |")
    println("============================================")

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .appName(" Power Iteration Clustering example (" + input + ")")
      .getOrCreate()
    Logger.getRootLogger.setLevel(Level.ERROR)

    // Load the graph 
    val graph = GraphLoader.edgeListFile(sparkSession.sparkContext, input)

    val model = RDFGraphPICClusteringAlg(sparkSession, graph, k, numIterations).run()

    val clusters = model.assignments.collect().groupBy(_.cluster).mapValues(_.map(_.id))
    val assignments = clusters.toList.sortBy { case (k, v) => v.length }
    val assignmentsStr = assignments
      .map {
        case (k, v) =>
          s"$k -> ${v.sorted.mkString("[", ",", "]")}"
      }.mkString(",")
    val sizesStr = assignments.map {
      _._2.size
    }.sorted.mkString("(", ",", ")")
    println(s"Cluster assignments: $assignmentsStr\ncluster sizes: $sizesStr")

    sparkSession.stop
  }

}