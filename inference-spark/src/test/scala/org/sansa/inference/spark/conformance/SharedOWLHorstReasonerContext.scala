package org.sansa.inference.spark.conformance

import com.holdenkarau.spark.testing.SharedSparkContext
import org.sansa.inference.spark.forwardchaining.{ForwardRuleReasoner, ForwardRuleReasonerOWLHorst}
import org.scalatest.Suite

/**
  * Test context to share an OWL Horst reasoner.
  *
  * @author Lorenz Buehmann
  */
trait SharedOWLHorstReasonerContext extends SharedSparkContext with ReasonerContextProvider{
  self: Suite =>

  @transient private var _reasoner: ForwardRuleReasonerOWLHorst = _

  override def reasoner: ForwardRuleReasoner = _reasoner

  override def beforeAll(): Unit = {
    super.beforeAll()
    _reasoner = new ForwardRuleReasonerOWLHorst(sc)
  }

}
