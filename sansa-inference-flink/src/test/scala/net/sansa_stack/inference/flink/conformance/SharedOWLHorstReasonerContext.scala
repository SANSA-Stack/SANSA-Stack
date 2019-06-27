package net.sansa_stack.inference.flink.conformance

import net.sansa_stack.inference.flink.forwardchaining.ForwardRuleReasoner
import org.apache.flink.api.scala.ExecutionEnvironment
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import net.sansa_stack.inference.flink.forwardchaining.ForwardRuleReasonerOWLHorst
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
  * Test context to share an RDFS reasoner.
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
