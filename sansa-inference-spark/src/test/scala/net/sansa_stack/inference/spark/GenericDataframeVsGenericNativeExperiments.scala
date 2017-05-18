package net.sansa_stack.inference.spark

import scala.collection.mutable

import org.apache.jena.vocabulary.RDFS
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import net.sansa_stack.inference.data.RDFTriple
import net.sansa_stack.inference.rules.RuleSets
import net.sansa_stack.inference.spark.data.loader.RDFGraphLoader
import net.sansa_stack.inference.spark.data.model.{RDFGraphDataFrame, RDFGraphNative}
import net.sansa_stack.inference.spark.forwardchaining.ForwardRuleReasonerOptimizedNative

/**
  * @author Lorenz Buehmann
  */
object GenericDataframeVsGenericNativeExperiments {

  val conf = new SparkConf()
  conf.registerKryoClasses(Array(classOf[RDFTriple]))

  // the SPARK config
  val sessionBuilder = SparkSession.builder
    .appName("GenericDataframe-Vs-GenericNative-Experiments")
    .master("local[4]")
    .config("spark.eventLog.enabled", "true")
    .config("spark.hadoop.validateOutputSpecs", "false") //override output files
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.default.parallelism", "4")
    .config("spark.sql.shuffle.partitions", "8")
    .config(conf)


  var session: SparkSession = null

  val names = Seq("rdfs7", "rdfs9", "rdfs2", "rdfs3", "rdfs11", "rdfs5")

  val rules = RuleSets.RDFS_SIMPLE.filter(r => names.isEmpty || names.contains(r.getName))

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: GenericDataframeVsGenericNativeExperiments <sourceFile> <targetDirectory>")
      System.exit(1)
    }

    session = sessionBuilder.appName("generic-rdd").getOrCreate()

    // load triples from disk
    var graph = RDFGraphLoader.loadFromDiskAsRDD(session, args(0), 4)//generateData(1)

    val infGraphNative = native(graph)

    println("Native: " + infGraphNative.size())

    session.stop()

    session = sessionBuilder.appName("generic-dataframe").getOrCreate()

    graph = RDFGraphLoader.loadFromDiskAsRDD(session, args(0), 4)

    val infGraphDataframe = dataframe(new RDFGraphDataFrame(graph.toDataFrame(session)))

    println("Dataframe: " + infGraphDataframe.size())

    session.stop()

    val targetDir = args(1)

    // write triples to disk
//    RDFGraphWriter.writeToFile(infGraphNative.toDataFrame(session), targetDir + "/native")
//    RDFGraphWriter.writeToFile(infGraphDataframe.toDataFrame(), targetDir + "/dataframe")



  }

  def generateData(scale: Integer): RDFGraphNative = {
    println("generating data...")
    val triples = new mutable.HashSet[RDFTriple]()
    val ns = "http://ex.org/"
    val p1 = ns + "p1"
    val p2 = ns + "p2"
    val p3 = ns + "p3"
    triples += RDFTriple(p1, RDFS.subPropertyOf.getURI, p2)
    triples += RDFTriple(p2, RDFS.subPropertyOf.getURI, p3)

    var begin = 1
    var end = 10 * scale
    for (i <- begin to end) {
      triples += RDFTriple(ns + "x" + i, p1, ns + "y" + i)
//      triples += RDFTriple(ns + "y" + i, p1, ns + "z" + i)
    }

//    begin = end + 1
//    end = begin + 10 * scale
//    for (i <- begin to end) {
//      // should not produce (?x_i, p1, ?z_i) as p1 and p2 are used
//      triples += RDFTriple(ns + "x" + i, p1, ns + "y" + i)
//      triples += RDFTriple(ns + "y" + i, p2, ns + "z" + i)
//    }
//
//    begin = end + 1
//    end = begin + 10 * scale
//    for (i <- begin to end) {
//      // should not produce (?x_i, p3, ?z_i) as p3 is not transitive
//      triples += RDFTriple(ns + "x" + i, p3, ns + "y" + i)
//      triples += RDFTriple(ns + "y" + i, p3, ns + "z" + i)
//    }

    // make RDD
    val triplesRDD = session.sparkContext.parallelize(triples.toSeq, 4)
    triplesRDD.cache()
    println(s"size:${triplesRDD.count}")
    new RDFGraphNative(triplesRDD)
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
