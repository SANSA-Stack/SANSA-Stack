package org.dissect.inference.rules

import org.apache.jena.vocabulary.{OWL2, RDF}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.dissect.inference.data._
import org.dissect.inference.rules.plan.{PlanExecutorNative, PlanExecutorSQL}
import org.dissect.inference.utils.RuleUtils

import scala.collection.mutable

/**
  * A forward chaining implementation of the RDFS entailment regime.
  *
  * @author Lorenz Buehmann
  */
object TransitivityRuleTest {

  def main(args: Array[String]) {
    val sparkSession = SparkSession.builder
      .master("local[4]")
      .appName("SPARK Reasoning")
      .config("spark.eventLog.enabled", "true")
      .getOrCreate()
    val sc = sparkSession.sparkContext

    // generate graph
    val triples = new mutable.HashSet[RDFTriple]()
    val ns = "http://ex.org/"
    val p1 = ns + "p1"
    val p2 = ns + "p2"
    val p3 = ns + "p3"
    triples += RDFTriple(p1, RDF.`type`.getURI, OWL2.TransitiveProperty.getURI)
    triples += RDFTriple(p2, RDF.`type`.getURI, OWL2.TransitiveProperty.getURI)

    val scale = 1

    var begin = 1
    var end = 10 * scale
    for(i <- begin to end) {
      triples += RDFTriple(ns + "x" + i, p1, ns + "y" + i)
      triples += RDFTriple(ns + "y" + i, p1, ns + "z" + i)
    }

    begin = end + 1
    end = begin + 10 * scale
    for(i <- begin to end) { // should not produce (?x_i, p1, ?z_i) as p1 and p2 are used
      triples += RDFTriple(ns + "x" + i, p1, ns + "y" + i)
      triples += RDFTriple(ns + "y" + i, p2, ns + "z" + i)
    }

    begin = end + 1
    end = begin + 10 * scale
    for(i <- begin to end) { // should not produce (?x_i, p3, ?z_i) as p3 is not transitive
      triples += RDFTriple(ns + "x" + i, p3, ns + "y" + i)
      triples += RDFTriple(ns + "y" + i, p3, ns + "z" + i)
    }

    val triplesRDD = sc.parallelize(triples.toSeq, 2)

    val graph = new RDFGraphNative(triplesRDD)

    // 1. the hard-coded reasoner

    // create reasoner
//    val reasoner = new TransitivityRuleReasoner(sc)
//
//    // compute inferred graph
//    val res1 = reasoner.apply(graph)
//
//    // write triples to disk
//    RDFGraphWriter.writeToFile(res1, "/tmp/spark-tests/built-in")


    val rules = RuleUtils.load("test.rules")
    val rule = RuleUtils.byName(rules, "prp-trp").get
    val plan = Planner.generatePlan(rule)

    // 2. the generic rule executor on native Spark structures

    val planExecutor1 = new PlanExecutorNative(sc)
    val res2 = planExecutor1.execute(plan, graph)
    RDFGraphWriter.writeToFile(res2.toRDD(), "/tmp/spark-tests/native")


    // 3. the SQL based rule executor

    // generate the SQL context
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // create a DataFrame
    val df = new RDFGraphDataFrame(graph.toDataFrame(sparkSession))
    val planExecutor2 = new PlanExecutorSQL(sparkSession)
    val res3 = planExecutor2.execute(plan, df)
    RDFGraphWriter.writeToFile(res3.toRDD(), "/tmp/spark-tests/sql")

    sc.stop()
  }

  class TransitivityRuleReasoner(sc: SparkContext) {

    def apply(graph: RDFGraph) = {
      val startTime = System.currentTimeMillis()

      val triplesRDD = graph.triples.cache()

      //  [ prp-trp: (?p rdf:type owl:TransitiveProperty) (?x ?p ?y) (?y ?p ?z) -> (?x ?p ?z) ]
      val rel1 = triplesRDD
        .filter(t =>
          t.predicate == RDF.`type`.getURI &&
            t.`object` == OWL2.TransitiveProperty.getURI)
        .map(t => (t.subject, Nil)) // -> (?p, Nil)
      println("REL1\n" + rel1.collect().mkString("\n"))

      val rel2 = triplesRDD
        .map(t => (t.predicate, (t.subject, t.`object`))) // (?p, (?x, ?y))
        .join(rel1) // (?p, ((?x, ?y), Nil))
        .map(e => RDFTriple(e._2._1._1, e._1, e._2._1._2)) // (?x, ?p, ?y)
      println("REL2\n" + rel2.collect().mkString("\n"))

      val rel3 = RDDOperations.subjPredKeyObj(rel2) // ((?y, ?p), ?z)
        .join(
        RDDOperations.objPredKeySubj(rel2)) // ((?y, ?p), ?x)
      // -> ((?y, ?p) , (?z, ?x))
      println("REL3\n" + rel3.collect().mkString("\n"))
      val result = rel3.map(e => RDFTriple(e._2._2, e._1._2, e._2._1))

      new RDFGraph(result)
    }
  }
}
