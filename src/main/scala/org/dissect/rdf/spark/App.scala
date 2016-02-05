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
    val graph = LoadGraph.apply(fileName, sparkContext)

    logger.info("RDFModel..........executed")

    sparkContext.stop()
  }
}

