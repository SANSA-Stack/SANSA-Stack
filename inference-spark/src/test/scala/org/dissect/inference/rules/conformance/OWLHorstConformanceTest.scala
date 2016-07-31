package org.dissect.inference.rules.conformance

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import org.dissect.inference.data.{RDFGraph, RDFGraphWriter, RDFTriple}
import org.dissect.inference.forwardchaining.{ForwardRuleReasonerOWLHorst, ForwardRuleReasonerRDFS}
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

import scala.collection.mutable

/**
  * The class is to test the conformance of each materialization rule of OWL Horst entailment.
  *
  * @author Lorenz Buehmann
  *
  */
class OWLHorstConformanceTest extends FlatSpec with BeforeAndAfterAll {

  behavior of "comformance of RDFS(simple) entailment rules"

  val owlHorstTestCaseIds = Set(
    "rdfbased-sem-rdfs-domain-cond",
    "rdfbased-sem-rdfs-range-cond",
    "rdfbased-sem-rdfs-subclass-cond",
    "rdfbased-sem-rdfs-subclass-trans",
    "rdfbased-sem-rdfs-subprop-cond",
    "rdfbased-sem-rdfs-subprop-trans",

    "rdfbased-sem-char-functional-inst",
    "rdfbased-sem-char-inversefunc-data",
    "rdfbased-sem-char-symmetric-inst",
    "rdfbased-sem-char-transitive-inst",
    "rdfbased-sem-inv-inst",
    "rdfbased-sem-eqdis-eqclass-subclass-1",
    "rdfbased-sem-eqdis-eqclass-subclass-2",
    "rdfbased-sem-eqdis-eqprop-subprop-1",
    "rdfbased-sem-eqdis-eqprop-subprop-2",
    "rdfbased-sem-restrict-hasvalue-inst-obj",
    "rdfbased-sem-restrict-hasvalue-inst-subj",
    "rdfbased-sem-restrict-somevalues-inst-subj",
    "rdfbased-sem-restrict-allvalues-inst-obj"
  )

  // the base directory of the test cases
  val testsCasesFolder = new File(this.getClass.getClassLoader.getResource("data/conformance/owl2rl").getPath)

  // load the test cases
  val testCases = TestCases.loadTestCases(testsCasesFolder, owlHorstTestCaseIds).filter(t => owlHorstTestCaseIds.contains("*") || owlHorstTestCaseIds.contains(t.id))

  // the SPARK config
  val conf = new SparkConf().setAppName("SPARK Reasoning")
  conf.set("spark.hadoop.validateOutputSpecs", "false")
  conf.setMaster("local[2]")
  //    conf.set("spark.eventLog.enabled", "true")

  // create SPARK context
  var sc = new SparkContext(conf)

  // create reasoner
  val reasoner = new ForwardRuleReasonerOWLHorst(sc)

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
      var inferredModel = RDFGraphWriter.convertToModel(inferredGraph)
      inferredModel.setNsPrefixes(testCase.outputGraph.getNsPrefixMap)

      println("#" * 80 + "\ninput:")
      testCase.inputGraph.write(System.out, "TURTLE")

      println("#" * 80 + "\nexpected output:")
      testCase.outputGraph.write(System.out, "TURTLE")

      println("#" * 80 + "\ngot output:")
      inferredModel.write(System.out, "TURTLE")

      // compare models, i.e. the inferred model should contain exactly the triples of the conclusion graph
      inferredModel = inferredModel.remove(testCase.inputGraph)

      val correctOutput = inferredModel.containsAll(testCase.outputGraph)
      assert(correctOutput, "contains all expected triples")

      val isomorph = inferredModel.isIsomorphicWith(testCase.outputGraph)


    }
  }

  // stop SPARK context
  override def afterAll() {
    sc.stop()
  }
}
