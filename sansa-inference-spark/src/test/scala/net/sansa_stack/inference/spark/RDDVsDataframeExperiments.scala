package net.sansa_stack.inference.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import net.sansa_stack.inference.data.RDFTriple
import net.sansa_stack.inference.spark.data.loader.RDFGraphLoader
import net.sansa_stack.inference.spark.data.model.{RDFGraph, RDFGraphDataFrame}
import net.sansa_stack.inference.spark.forwardchaining.ForwardRuleReasonerRDFS
import net.sansa_stack.inference.utils.{Profiler, RuleUtils}

/**
  * @author Lorenz Buehmann
  */
object RDDVsDataframeExperiments extends Profiler{

  val conf = new SparkConf()
  conf.registerKryoClasses(Array(classOf[RDFTriple]))

  // the SPARK config
  val session = SparkSession.builder
    .appName("RDD-vs-Dataframe")
    .master("local[4]")
    .config("spark.eventLog.enabled", "true")
    .config("spark.hadoop.validateOutputSpecs", "false") //override output files
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.default.parallelism", "4")
    .config("spark.sql.shuffle.partitions", "4")
    .config(conf)
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: RDDVsDataframeExperiments <sourceFile> <targetDirectory>")
      System.exit(1)
    }

    val sourcePath = args(0)

//    val infGraphRDD = profile{
//      computeRDD(sourcePath)
//    }
//    println("RDD-based: " + infGraphRDD.size())

    val infGraphDataframe = profile {
      computeDataframe(sourcePath)
    }
    println("Dataframe-based: " + infGraphDataframe.size())
    //      infGraphDataframe.toDataFrame().explain()

//
//
//    val targetDir = args(1)
//
//    // write triples to disk
//    RDFGraphWriter.writeToFile(infGraphRDD, targetDir + "/rdd")
//    RDFGraphWriter.writeToFile(infGraphDataframe.toDataFrame(), targetDir + "/dataframe")

    session.stop()

  }

  def computeRDD(sourcePath: String): RDFGraph = {
    // load triples from disk
    val graph = RDFGraphLoader.loadFromDisk(session, sourcePath, 4)

    // create reasoner
    val reasoner = new ForwardRuleReasonerRDFS(session.sparkContext)

    // compute inferred graph
    val inferredGraph = reasoner.apply(graph)

    inferredGraph
  }

  def computeDataframe(sourcePath: String): RDFGraphDataFrame = {
    // load triples from disk
    val graph = RDFGraphLoader.loadFromDiskAsDataFrame(session, sourcePath, 4)

    // create reasoner
    val reasoner = new ForwardRuleReasonerRDFSDataframe(session)

    // compute inferred graph
    val inferredGraph = reasoner.apply(graph)

    inferredGraph
  }

  def compareTransitiveClosure() = {

    val session = SparkSession.builder
      .appName("TC Comparison")
      .master("local[4]")
      .config("spark.eventLog.enabled", "true")
      .config("spark.hadoop.validateOutputSpecs", "false") // override output files
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.default.parallelism", "4")
      .config("spark.sql.shuffle.partitions", "4")
      .config(conf)
      .getOrCreate()


    def rddBased() = {

    }

    def dataframeBased() = {

    }

  }
}
