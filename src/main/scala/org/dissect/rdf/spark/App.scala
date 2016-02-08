package org.dissect.rdf.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io.Source
import java.io.File
import org.apache.commons.io.FileUtils
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io.PrintWriter
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkConf
import org.apache.jena.riot.RiotReader
import org.apache.jena.riot.Lang
import org.dissect.rdf.spark.utils._
import org.dissect.rdf.spark.model._
import org.dissect.rdf.spark.analytics._
import org.dissect.rdf.spark.utils.Logging
import org.dissect.rdf.spark.graph.LoadGraph
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.jena.sparql.util.NodeUtils

object App extends Logging {


  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      logger.error("=> wrong parameters number")
      System.err.println("Usage: FileName <path-to-files> <output-path>")
      System.exit(1)
    }

    val fileName = args(0)
    val sparkMasterHost = if(args.length >= 2) args(1) else SparkUtils.SPARK_MASTER

    val sparkConf = new SparkConf().setAppName("BDE-readRDF").setMaster(sparkMasterHost);
    val sparkContext = new SparkContext(sparkConf)

    //val file = "C:/Users/Gezimi/Desktop/AKSW/Spark/sparkworkspace/data/nyse.nt"
    val graphLayout = LoadGraph(fileName, sparkContext)
    val graph = graphLayout.graph
    val vertexId = graphLayout.iriToId.lookup("http://fp7-pp.publicdata.eu/resource/funding/223894-999854564")

    println("VERTEX ID = " + vertexId.mkString("."))

    val landmarks = Seq[Long](1, 2, 3)
    val result = ShortestPaths.run(graph, landmarks)

    logger.info("RDFModel..........executed")


    logger.info("Graph stats: " + graph.numVertices + " - " + graph.numEdges)

    sparkContext.stop()
  }
}

