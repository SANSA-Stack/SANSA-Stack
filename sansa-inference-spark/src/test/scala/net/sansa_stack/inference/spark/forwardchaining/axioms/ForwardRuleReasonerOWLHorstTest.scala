package net.sansa_stack.inference.spark.forwardchaining.axioms

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import net.sansa_stack.owl.spark.rdd.FunctionalSyntaxOWLAxiomsRDDBuilder
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.OWLAxiom

import scala.collection.JavaConverters._


/**
  * Rule names refer to the name scheme used in
  * 'RORS: Enhanced Rule-based OWL Reasoning on Spark' by Liu, Feng, Zhang,
  * Wang, Rao
  */
class ForwardRuleReasonerOWLHorstTest extends FunSuite with SharedSparkContext with DataFrameSuiteBase {
  private val resourcePath = "/forward_chaining/axioms/"
  private val defaultPrefix = "http://ex.com/default#"
  private val df = OWLManager.getOWLDataFactory

  import net.sansa_stack.owl.spark.owl._

  val reasoner = new ForwardRuleReasonerOWLHorst(sc, 4)

  test("OWL Horst Axiom Forward Chaining Rule Reasoner") {

    val input = getClass.getResource("/ont_functional.owl").getPath

    var owlAxiomsRDD = FunctionalSyntaxOWLAxiomsRDDBuilder.build(spark, input)
    val reasoner = new ForwardRuleReasonerOWLHorst(sc, 4)
    val res: RDD[OWLAxiom] = reasoner(owlAxiomsRDD)

    assert(res.count() == 211)

  }
}
