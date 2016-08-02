package org.sansa.inference.flink.conformance

import org.apache.flink.api.scala.ExecutionEnvironment
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.sansa.inference.flink.forwardchaining.{ForwardRuleReasoner, ForwardRuleReasonerOWLHorst}
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
  * Test context to share an RDFS reasoner.
  *
  * @author Lorenz Buehmann
  */
@RunWith(classOf[Parameterized])
trait SharedOWLHorstReasonerContext extends BeforeAndAfterAll with ReasonerContextProvider{
  self: Suite =>

  @transient private var _reasoner: ForwardRuleReasonerOWLHorst = _

  val reasoner: ForwardRuleReasoner = _reasoner

  @transient private var _env: ExecutionEnvironment = _
  def env: ExecutionEnvironment = _env


  override def beforeAll(): Unit = {
    super.beforeAll()
    _env = ExecutionEnvironment.getExecutionEnvironment
    _reasoner = new ForwardRuleReasonerOWLHorst(env)
  }
}
