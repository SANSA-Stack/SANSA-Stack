package org.sansa.inference.flink.conformance

import org.apache.flink.api.scala.ExecutionEnvironment
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.sansa.inference.flink.forwardchaining.{ForwardRuleReasoner, ForwardRuleReasonerRDFS}
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
  * Test context to share an RDFS reasoner.
  *
  * @author Lorenz Buehmann
  */
@RunWith(classOf[Parameterized])
trait SharedRDFSReasonerContext extends BeforeAndAfterAll with ReasonerContextProvider{
  self: Suite =>

  @transient private var _reasoner: ForwardRuleReasonerRDFS = _

  val reasoner: ForwardRuleReasoner = _reasoner

  @transient private var _env: ExecutionEnvironment = _
  def env: ExecutionEnvironment = _env


  override def beforeAll(): Unit = {
    super.beforeAll()
    _env = ExecutionEnvironment.getExecutionEnvironment
    _reasoner = new ForwardRuleReasonerRDFS(env)
  }
}
