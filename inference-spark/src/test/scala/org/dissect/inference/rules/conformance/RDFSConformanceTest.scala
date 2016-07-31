package org.dissect.inference.rules.conformance

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import org.dissect.inference.data.{RDFGraph, RDFGraphWriter, RDFTriple}
import org.dissect.inference.forwardchaining.ForwardRuleReasonerRDFS
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

import scala.collection.mutable

/**
  * The class is to test the conformance of each materialization rule of RDFS(simple) entailment.
  *
  * @author Lorenz Buehmann
  *
  */
class RDFSConformanceTest extends FlatSpec with BeforeAndAfterAll {

  behavior of "comformance of RDFS(simple) entailment rules"

  val rdfsSimpleTestCaseIds = Set(
    "rdfbased-sem-rdfs-domain-cond",
    "rdfbased-sem-rdfs-range-cond",
    "rdfbased-sem-rdfs-subclass-cond",
    "rdfbased-sem-rdfs-subclass-trans",
    "rdfbased-sem-rdfs-subprop-cond",
    "rdfbased-sem-rdfs-subprop-trans")

  // the base directory of the test cases
  val testsCasesFolder = new File(this.getClass.getClassLoader.getResource("data/conformance/rdfs").getPath)

  // load the test cases
  val testCases = TestCases.loadTestCases(testsCasesFolder).filter(t => rdfsSimpleTestCaseIds.contains(t.id))

  // the SPARK config
  val conf = new SparkConf().setAppName("SPARK Reasoning")
  conf.set("spark.hadoop.validateOutputSpecs", "false")
  conf.setMaster("local[2]")
  //    conf.set("spark.eventLog.enabled", "true")

  // create SPARK context
  var sc = new SparkContext(conf)

  // create reasoner
  val reasoner = new ForwardRuleReasonerRDFS(sc)

  testCases.foreach{testCase =>
    println(testCase.id)

    testCase.id should "produce the same graph" in {
      val triples = new mutable.HashSet[RDFTriple]()

      // convert to internal triples
      val iterator = testCase.inputGraph.listStatements()
      while(iterator.hasNext) {
        val st = iterator.next()
        triples.add(RDFTriple(st.getSubject.toString, st.getPredicate.toString, st.getObject.toString))
      }

      // distribute triples
      val triplesRDD = sc.parallelize(triples.toSeq, 2)

      // create graph
      val graph = new RDFGraph(triplesRDD)

      // compute inferred graph
      val inferredGraph = reasoner.apply(graph)

      // convert to JENA model
      val inferredModel = RDFGraphWriter.convertToModel(inferredGraph)

      // compare models, i.e. the inferred model should contain exactly the triples of the conclusion graph
      inferredModel.remove(testCase.inputGraph)

      assert(inferredModel.isIsomorphicWith(testCase.outputGraph))
    }
  }

  // stop SPARK context
  override def afterAll() {
    sc.stop()
  }
}
