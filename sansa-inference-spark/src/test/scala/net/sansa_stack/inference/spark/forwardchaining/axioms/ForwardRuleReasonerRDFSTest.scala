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
}
