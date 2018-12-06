package net.sansa_stack.inference.spark.forwardchaining.axioms

import com.holdenkarau.spark.testing.{ SharedSparkContext, DataFrameSuiteBase }
// import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.owl.spark.rdd.FunctionalSyntaxOWLAxiomsRDDBuilder
import net.sansa_stack.owl.spark.rdd.OWLAxiomsRDD
import org.scalatest.FunSuite
import org.semanticweb.owlapi.model.OWLAxiom
import org.apache.spark.rdd.RDD

class ForwardRuleReasonerRDFSTest extends FunSuite with SharedSparkContext with DataFrameSuiteBase {

  val reasoner = new ForwardRuleReasonerRDFS(sc, 4)

  test("RDFS Axiom Forward Chaining Rule Reasoner") {

      val input = getClass.getResource("/ont_functional.owl").getPath

      val owlAxiomsRDD: OWLAxiomsRDD = FunctionalSyntaxOWLAxiomsRDDBuilder.build(spark, input)
      val reasoner: RDD[OWLAxiom] = new ForwardRuleReasonerRDFS(sc, 4)(owlAxiomsRDD)

      assert(reasoner.count() == 16)

  }
}
