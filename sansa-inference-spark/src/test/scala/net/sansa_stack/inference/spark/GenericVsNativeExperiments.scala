package net.sansa_stack.inference.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import net.sansa_stack.inference.data.RDFTriple
import net.sansa_stack.inference.spark.GenericDataframeVsGenericNativeExperiments.{session, sessionBuilder}
import net.sansa_stack.inference.spark.data.loader.RDFGraphLoader
import net.sansa_stack.inference.spark.data.model.RDFGraphDataFrame
import net.sansa_stack.inference.spark.data.writer.RDFGraphWriter
import net.sansa_stack.inference.utils.RuleUtils

/**
  * @author Lorenz Buehmann
  */
object GenericVsNativeExperiments {

  val conf = new SparkConf()
  conf.registerKryoClasses(Array(classOf[RDFTriple]))

  private def newSession(name: String) = SparkSession.builder
    .appName(name)
    .master("local[4]")
    .config("spark.eventLog.enabled", "true")
    .config("spark.hadoop.validateOutputSpecs", "false") // override output files
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

    var session = newSession("native SQL")

    // load triples from disk
    var graph = RDFGraphLoader.loadFromDiskAsDataFrame(session, args(0), 4)

    val infGraphNative = native(session, graph)

    println("Native: " + infGraphNative.size())
    session.stop()


//    session = sessionBuilder.appName("generic SQL").getOrCreate()
//    // load triples from disk
//    graph = RDFGraphLoader.loadFromDiskAsDataFrame(session, args(0), 4)
//
//    val infGraphGeneric = generic(session, graph)
//    println("Generic: " + infGraphGeneric.toDataFrame().count())

//
//
//    val targetDir = args(1)
//
//    // write triples to disk
//    RDFGraphWriter.writeDataframeToDisk(infGraphNative.toDataFrame(), targetDir + "/native")
//    RDFGraphWriter.writeDataframeToDisk(infGraphGeneric.toDataFrame(), targetDir + "/generic")

    session.stop()

  }

  def native(session: SparkSession, graph: RDFGraphDataFrame): RDFGraphDataFrame = {
    // create reasoner
    val reasoner = new ForwardRuleReasonerRDFSDataframe(session)

    // compute inferred graph
    val inferredGraph = reasoner.apply(graph)

    inferredGraph
  }

  def generic(session: SparkSession, graph: RDFGraphDataFrame): RDFGraphDataFrame = {
    // create reasoner
    val rules = RuleUtils.load("rdfs-simple.rules").toSet
    val reasoner = new ForwardRuleReasonerOptimizedSQL(session, rules)

    // compute inferred graph
    val inferredGraph = reasoner.apply(graph)

    inferredGraph
  }
}
