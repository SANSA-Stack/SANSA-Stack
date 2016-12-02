package net.sansa_stack.inference.spark

import net.sansa_stack.inference.spark.data.RDFGraphDataFrame
import net.sansa_stack.inference.spark.forwardchaining.ForwardRuleReasonerOptimizedSQL
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import net.sansa_stack.inference.data.RDFTriple
import net.sansa_stack.inference.spark.data.{RDFGraphLoader, RDFGraphWriter}
import net.sansa_stack.inference.spark.forwardchaining.{ForwardRuleReasonerOptimizedSQL, ForwardRuleReasonerRDFSDataframe}
import net.sansa_stack.inference.utils.RuleUtils

/**
  * @author Lorenz Buehmann
  */
object GenericVsNativeExperiments {

  val conf = new SparkConf()
  conf.registerKryoClasses(Array(classOf[RDFTriple]))

  // the SPARK config
  val session = SparkSession.builder
    .appName("SPARK Reasoning")
    .master("local[4]")
    .config("spark.eventLog.enabled", "true")
    .config("spark.hadoop.validateOutputSpecs", "false") //override output files
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.default.parallelism", "4")
    .config("spark.sql.shuffle.partitions", "8")
    .config(conf)
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: GenericVsNativeExperiments <sourceFile> <targetDirectory>")
      System.exit(1)
    }

    // load triples from disk
    val graph = RDFGraphLoader.loadGraphDataFrameFromFile(args(0), session, 4)

    val infGraphNative = native(graph)

    val infGraphGeneric = generic(graph)

    println("Native: " + infGraphNative.toDataFrame().count())
    println("Generic: " + infGraphGeneric.toDataFrame().count())

    val targetDir = args(1)

    // write triples to disk
    RDFGraphWriter.writeDataframeToFile(infGraphNative.toDataFrame(), targetDir + "/native")
    RDFGraphWriter.writeDataframeToFile(infGraphGeneric.toDataFrame(), targetDir + "/generic")

    session.stop()

  }

  def native(graph: RDFGraphDataFrame): RDFGraphDataFrame = {
    // create reasoner
    val reasoner = new ForwardRuleReasonerRDFSDataframe(session)

    // compute inferred graph
    val inferredGraph = reasoner.apply(graph)

    inferredGraph
  }

  def generic(graph: RDFGraphDataFrame): RDFGraphDataFrame = {
    // create reasoner
    val rules = RuleUtils.load("rdfs-simple.rules").toSet
    val reasoner = new ForwardRuleReasonerOptimizedSQL(session, rules)

    // compute inferred graph
    val inferredGraph = reasoner.apply(graph)

    inferredGraph
  }
}
