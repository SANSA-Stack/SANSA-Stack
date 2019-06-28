package net.sansa_stack.inference.flink.conformance

import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.scalatest.Suite

import net.sansa_stack.inference.flink.forwardchaining.ForwardRuleReasonerOWLHorst

/**
  * Test context to share an OWL Horst reasoner.
  *
  * @author Lorenz Buehmann
  */
@RunWith(classOf[Parameterized])
trait SharedOWLHorstReasonerContext
  extends SharedReasonerContext[ForwardRuleReasonerOWLHorst] {
  self: Suite =>


  override def beforeAll(): Unit = {
    super.beforeAll()
    _reasoner = new ForwardRuleReasonerOWLHorst(env)
  }
}
