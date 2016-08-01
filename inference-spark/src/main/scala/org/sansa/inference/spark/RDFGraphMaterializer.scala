package org.sansa.inference.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.sansa.inference.data.RDFTriple
import org.sansa.inference.spark.data.{RDFGraphLoader, RDFGraphWriter}
import org.sansa.inference.spark.forwardchaining.ForwardRuleReasonerRDFS

/**
  * The class to compute the RDFS materialization of a given RDF graph.
  *
  * @author Lorenz Buehmann
  *
  */
object RDFGraphMaterializer {


  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Usage: RDFGraphMaterializer <sourceFile> <targetFile>")
      System.exit(1)
    }

    val conf = new SparkConf()
    conf.registerKryoClasses(Array(classOf[RDFTriple]))

    // the SPARK config
    val session = SparkSession.builder
      .appName("SPARK Reasoning")
      .master("local[4]")
      .config("spark.eventLog.enabled", "true")
      .config("spark.hadoop.validateOutputSpecs", "false") //override output files
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config(conf)
      .getOrCreate()

    // load triples from disk
    val graph = RDFGraphLoader.loadFromFile(args(0), session.sparkContext, 4)

    // create reasoner
    val reasoner = new ForwardRuleReasonerRDFS(session.sparkContext)

    // compute inferred graph
    val inferredGraph = reasoner.apply(graph)

//    // load triples from disk
//    val graph2 = RDFGraphLoader.loadGraphDataFrameFromFile(args(0), session, 4)
//
//    // create reasoner
//    val reasoner2 = new ForwardRuleReasonerRDFSDataframe(session)
//
//    // compute inferred graph
//    val inferredGraph2 = reasoner2.apply(graph2)

    // write triples to disk
    RDFGraphWriter.writeToFile(inferredGraph, args(1))

    session.stop()
  }

}
