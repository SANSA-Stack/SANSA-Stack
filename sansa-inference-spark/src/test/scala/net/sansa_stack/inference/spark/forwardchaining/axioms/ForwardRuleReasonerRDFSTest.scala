package net.sansa_stack.inference.spark.forwardchaining.axioms

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import net.sansa_stack.owl.spark.rdd.{FunctionalSyntaxOWLAxiomsRDDBuilder, OWLAxiomsRDD}
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.OWLAxiom

/**
  * Entailment pattern naming taken from
  * https://www.w3.org/TR/rdf11-mt/#patterns-of-rdfs-entailment-informative
  */
class ForwardRuleReasonerRDFSTest extends FunSuite with SharedSparkContext with DataFrameSuiteBase {
  private val resourcePath = "/forward_chaining/axioms/"
  private val defaultPrefix = "http://ex.com/default#"
  private val df = OWLManager.getOWLDataFactory

  import net.sansa_stack.owl.spark.owl._

  val reasoner = new ForwardRuleReasonerRDFS(sc, 4)

  test("RDFS Axiom Forward Chaining Rule Reasoner") {

      val input = getClass.getResource("/ont_functional.owl").getPath

      val owlAxiomsRDD: OWLAxiomsRDD = FunctionalSyntaxOWLAxiomsRDDBuilder.build(spark, input)
      val reasoner: RDD[OWLAxiom] = new ForwardRuleReasonerRDFS(sc, 4)(owlAxiomsRDD)

      assert(reasoner.count() == 16)

  }

  /**
    * rdfs2
    *   Condition:
    *     aaa rdfs:domain xxx .
    *     yyy aaa zzz .
    *   Consequence:
    *     yyy rdf:type xxx .
    */
  test("Rule rdfs2 should return correct results") {
    val cls01 = df.getOWLClass(defaultPrefix + "Cls01")
    val cls02 = df.getOWLClass(defaultPrefix + "Cls02")
    val cls03 = df.getOWLClass(defaultPrefix + "Cls03")
    val indivB = df.getOWLNamedIndividual(defaultPrefix + "indivB")
    val indivD = df.getOWLNamedIndividual(defaultPrefix + "indivD")
    val indivF = df.getOWLNamedIndividual(defaultPrefix + "indivF")

    val input = getClass.getResource(resourcePath + "test_rdfs2.owl").getPath

    val axiomsRDD = spark.owl(Syntax.FUNCTIONAL)(input)
    val reasoner = new ForwardRuleReasonerRDFS(sc, sc.defaultMinPartitions)
    val inferred: Seq[OWLAxiom] = reasoner.apply(axiomsRDD).collect()

    // Three axioms should be inferred:
    // ClassAssertion(:Cls01 :indivB)
    // ClassAssertion(:Cls02 :indivD)
    // ClassAssertion(:Cls03 :indivF)
    assert(inferred.size == 3)
    assert(inferred.contains(df.getOWLClassAssertionAxiom(cls01, indivB)))
    assert(inferred.contains(df.getOWLClassAssertionAxiom(cls02, indivD)))
    assert(inferred.contains(df.getOWLClassAssertionAxiom(cls03, indivF)))
  }

  /**
    * rdfs3
    *   Condition:
    *     aaa rdfs:range xxx .
    *     yyy aaa zzz .
    *   Consequence:
    *     zzz rdf:type xxx .
    */
  test("Rule rdfs3 should return correct results") {
    val cls01 = df.getOWLClass(defaultPrefix + "Cls01")
    val indivC = df.getOWLNamedIndividual(defaultPrefix + "indivC")

    val input = getClass.getResource(resourcePath + "test_rdfs3.owl").getPath

    val axiomsRDD = spark.owl(Syntax.FUNCTIONAL)(input)
    val reasoner = new ForwardRuleReasonerRDFS(sc, sc.defaultMinPartitions)
    val inferred: Seq[OWLAxiom] = reasoner.apply(axiomsRDD).collect()

    // One axiom should be inferred:
    // ClassAssertion(:Cls01 :indivC)
    //
    // The axiom
    //   AnnotationPropertyRange(:annProp02 :Cls03)
    // in connection with
    //   AnnotationAssertion(:annProp02 :indivF :someIRI)
    // doesn't generate a new axiom (which is consistent with what e.g. HermiT
    // does).

    assert(inferred.size == 1)
    assert(inferred.contains(df.getOWLClassAssertionAxiom(cls01, indivC)))
  }
}
