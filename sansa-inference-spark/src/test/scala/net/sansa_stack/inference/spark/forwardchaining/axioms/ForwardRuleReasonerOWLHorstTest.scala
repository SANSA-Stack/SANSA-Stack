package net.sansa_stack.inference.spark.forwardchaining.axioms

import com.holdenkarau.spark.testing.{ SharedSparkContext, DataFrameSuiteBase }
import net.sansa_stack.owl.spark.rdd.FunctionalSyntaxOWLAxiomsRDDBuilder
import org.scalatest.FunSuite
import org.semanticweb.owlapi.model.OWLAxiom
import org.apache.spark.rdd.RDD

class ForwardRuleReasonerOWLHorstTest extends FunSuite with SharedSparkContext with DataFrameSuiteBase {

  val reasoner = new ForwardRuleReasonerOWLHorst(sc, 4)

  test("OWL Horst Axiom Forward Chaining Rule Reasoner") {

    val input = getClass.getResource("/ont_functional.owl").getPath

    var owlAxiomsRDD = FunctionalSyntaxOWLAxiomsRDDBuilder.build(spark, input)
    val reasoner = new ForwardRuleReasonerOWLHorst(sc, 4)
    val res: RDD[OWLAxiom] = reasoner(owlAxiomsRDD, input)

    assert(res.count() == 211)

  }
}
