package net.sansa_stack.inference.flink.conformance

import net.sansa_stack.inference.flink.forwardchaining.{ForwardRuleReasoner, ForwardRuleReasonerRDFS}
import net.sansa_stack.inference.rules.RDFSLevel
import org.apache.flink.api.scala.ExecutionEnvironment
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
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
  def reasoner: ForwardRuleReasoner = _reasoner

  @transient private var _env: ExecutionEnvironment = _
  def env: ExecutionEnvironment = _env


  override def beforeAll(): Unit = {
    super.beforeAll()
    _env = ExecutionEnvironment.getExecutionEnvironment
    _env.getConfig.disableSysoutLogging()
    _reasoner = new ForwardRuleReasonerRDFS(env)
    _reasoner.level = RDFSLevel.SIMPLE
  }
}
