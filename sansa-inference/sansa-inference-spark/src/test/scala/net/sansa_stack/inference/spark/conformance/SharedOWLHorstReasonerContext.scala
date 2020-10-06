package net.sansa_stack.inference.spark.conformance

import com.holdenkarau.spark.testing.SharedSparkContext
import net.sansa_stack.inference.spark.forwardchaining.triples.{ForwardRuleReasoner, ForwardRuleReasonerOWLHorst}
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
