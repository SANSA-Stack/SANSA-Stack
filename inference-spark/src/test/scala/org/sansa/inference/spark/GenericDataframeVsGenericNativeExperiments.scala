package org.sansa.inference.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.sansa.inference.data.RDFTriple
import org.sansa.inference.rules.RuleSets
import org.sansa.inference.spark.data.{RDFGraphDataFrame, RDFGraphLoader, RDFGraphNative, RDFGraphWriter}
import org.sansa.inference.spark.forwardchaining.{ForwardRuleReasonerOptimizedNative, ForwardRuleReasonerOptimizedSQL}

/**
  * @author Lorenz Buehmann
  */
object GenericDataframeVsGenericNativeExperiments {

  val conf = new SparkConf()
  conf.registerKryoClasses(Array(classOf[RDFTriple]))

  // the SPARK config
  val session = SparkSession.builder
    .appName("GenericDataframe-Vs-GenericNative-Experiments")
    .master("local[4]")
    .config("spark.eventLog.enabled", "true")
    .config("spark.hadoop.validateOutputSpecs", "false") //override output files
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.default.parallelism", "4")
    .config("spark.sql.shuffle.partitions", "8")
    .config(conf)
    .getOrCreate()

  val rules = RuleSets.RDFS_SIMPLE.filter(r => r.getName == "rdfs7")

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: GenericDataframeVsGenericNativeExperiments <sourceFile> <targetDirectory>")
      System.exit(1)
    }

    // load triples from disk
    val graph = RDFGraphLoader.loadGraphFromFile(args(0), session, 4)

    val infGraphNative = native(graph)

    val infGraphDataframe = dataframe(new RDFGraphDataFrame(graph.toDataFrame(session)))

    println("Native: " + infGraphNative.size())
    println("Dataframe: " + infGraphDataframe.size())

    val targetDir = args(1)

    // write triples to disk
//    RDFGraphWriter.writeToFile(infGraphNative.toDataFrame(session), targetDir + "/native")
//    RDFGraphWriter.writeToFile(infGraphDataframe.toDataFrame(), targetDir + "/dataframe")

    session.stop()

  }

  def native(graph: RDFGraphNative): RDFGraphNative = {
    // create reasoner
    val reasoner = new ForwardRuleReasonerOptimizedNative(session, rules)

    // compute inferred graph
    val inferredGraph = reasoner.apply(graph)

    inferredGraph
  }

  def dataframe(graph: RDFGraphDataFrame): RDFGraphDataFrame = {
    // create reasoner
    val reasoner = new ForwardRuleReasonerOptimizedSQL(session, rules)

    // compute inferred graph
    val inferredGraph = reasoner.apply(graph)

    inferredGraph
  }
}
