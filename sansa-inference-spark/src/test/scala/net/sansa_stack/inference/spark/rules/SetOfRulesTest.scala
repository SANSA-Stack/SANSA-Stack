package net.sansa_stack.inference.spark.rules

import net.sansa_stack.inference.data.JenaOps
import net.sansa_stack.inference.spark.data.loader.RDFGraphLoader
import net.sansa_stack.inference.spark.data.model.RDFGraphNative
import net.sansa_stack.inference.spark.data.writer.RDFGraphWriter
import net.sansa_stack.inference.spark.forwardchaining.triples.{ForwardRuleReasonerNaive, ForwardRuleReasonerOptimizedNative}
import net.sansa_stack.inference.utils.RuleUtils
import org.apache.jena.graph.Triple
import org.apache.jena.reasoner.rulesys.Rule
import org.apache.jena.vocabulary.{OWL2, RDF, RDFS}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * A forward chaining implementation of the RDFS entailment regime.
  *
  * @author Lorenz Buehmann
  */
object SetOfRulesTest {

  val sparkSession = SparkSession.builder
    .appName("SPARK Reasoning")
    //      .master("spark://me-ThinkPad-W510:7077")
    .master("local[4]")
    .config("spark.eventLog.enabled", "true")
    .config("spark.hadoop.validateOutputSpecs", "false") //override output files
    .config("spark.default.parallelism", "4")
    .config("spark.sql.shuffle.partitions", "8")
    //      .config("spark.jars", "/home/me/work/projects/scala/Spark-Sem-I/target/inference-spark-0.1-SNAPSHOT.jar")
    .getOrCreate()

  val sc = sparkSession.sparkContext

  def main(args: Array[String]) {
    // generate graph
    val scale = 100
    val graph = generateData(scale)
//    val graph = loadData()

    val ruleNames = Set(
      "rdfs7",
      "prp-trp",
      "rdfs9"
    )

    val numberOfTriples = graph.size()
    println("#Triples:" + numberOfTriples)

    val rules = RuleUtils.load("rdfs-simple.rules")//.filter(r => ruleNames.contains(r.getName))

//    runNaive(graph, rules)
//    runNative(graph, rules)
//    runSQL(graph, rules)

    sc.stop()
  }

  def loadData(): RDFGraphNative = {
    println("loading data...")
    val g = RDFGraphLoader.loadFromDiskAsRDD(sparkSession, "/home/me/tools/uobm_generator/preload_generated_uobm/univ_all.nt", 2)
    println("finished loading data.")
    g
  }

  private def generateData(scale: Integer) = {
    println("generating data...")
    val rdfOps = new JenaOps

    val triples = new mutable.HashSet[Triple]()
    val ns = "http://ex.org/"

    val p1 = rdfOps.makeUri(ns + "p1")
    val p2 = rdfOps.makeUri(ns + "p2")
    val p3 = rdfOps.makeUri(ns + "p3")
    val c1 = rdfOps.makeUri(ns + "c1")
    val c2 = rdfOps.makeUri(ns + "c2")
    triples += rdfOps.makeTriple(p1, rdfOps.makeUri(RDF.`type`.getURI), rdfOps.makeUri(OWL2.TransitiveProperty.getURI))
    triples += rdfOps.makeTriple(p2, rdfOps.makeUri(RDF.`type`.getURI), rdfOps.makeUri(OWL2.TransitiveProperty.getURI))
    triples += rdfOps.makeTriple(p1, rdfOps.makeUri(RDFS.subPropertyOf.getURI), p2)
    triples += rdfOps.makeTriple(c1, rdfOps.makeUri(RDFS.subClassOf.getURI), c2)

    var begin = 1
    var end = 10 * scale
    for (i <- begin to end) {
      triples += rdfOps.makeTriple(rdfOps.makeUri(ns + "x" + i), p1, rdfOps.makeUri(ns + "y" + i))
      triples += rdfOps.makeTriple(rdfOps.makeUri(ns + "y" + i), p1, rdfOps.makeUri(ns + "z" + i))
    }

    begin = end + 1
    end = begin + 10 * scale
    for (i <- begin to end) {
      // should not produce (?x_i, p1, ?z_i) as p1 and p2 are used
      triples += rdfOps.makeTriple(rdfOps.makeUri(ns + "x" + i), p1, rdfOps.makeUri(ns + "y" + i))
      triples += rdfOps.makeTriple(rdfOps.makeUri(ns + "y" + i), p2, rdfOps.makeUri(ns + "z" + i))
    }

    begin = end + 1
    end = begin + 10 * scale
    for (i <- begin to end) {
      // should not produce (?x_i, p3, ?z_i) as p3 is not transitive
      triples += rdfOps.makeTriple(rdfOps.makeUri(ns + "x" + i), p3, rdfOps.makeUri(ns + "y" + i))
      triples += rdfOps.makeTriple(rdfOps.makeUri(ns + "y" + i), p3, rdfOps.makeUri(ns + "z" + i))
    }

    // C1(c_i)
    begin = 1
    end = 10 * scale
    for (i <- begin to end) {
      triples += rdfOps.makeTriple(rdfOps.makeUri(ns + "c" + i), rdfOps.makeUri(RDF.`type`.getURI), c1)
    }

    // make RDD
    println("distributing...")
    val triplesRDD = sc.parallelize(triples.toSeq, 4)
    triplesRDD.toDebugString

    new RDFGraphNative(triplesRDD)
  }

  def runNaive(graph: RDFGraphNative, rules: Seq[Rule]): Unit = {
    val reasoner = new ForwardRuleReasonerNaive(sc, rules.toSet)
    val res = reasoner.apply(graph)
    RDFGraphWriter.writeTriplesToDisk(res.toRDD(), "/tmp/spark-tests/naive")
  }

  def runNative(graph: RDFGraphNative, rules: Seq[Rule]): Unit = {
    val reasoner = new ForwardRuleReasonerOptimizedNative(sparkSession, rules.toSet)
    val res = reasoner.apply(graph)
    RDFGraphWriter.writeTriplesToDisk(res.toRDD(), "/tmp/spark-tests/optimized-native")
  }

//  def runSQL(graph: RDFGraphNative, rules: Seq[Rule]) = {
//    // create Dataframe based graph
//    val graphDataframe = new RDFGraphDataFrame(graph.toDataFrame(sparkSession)).cache()
//
//    val reasoner = new ForwardRuleReasonerOptimizedSQL(sparkSession, rules.toSet)
//    val res = reasoner.apply(graphDataframe)
//    RDFGraphWriter.writeDataframeToDisk(res.toDataFrame(), "/tmp/spark-tests/optimized-sql")
//    reasoner.showExecutionStats()
//  }
}
