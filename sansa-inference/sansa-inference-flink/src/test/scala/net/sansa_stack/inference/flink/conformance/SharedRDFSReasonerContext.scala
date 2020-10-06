package net.sansa_stack.inference.flink.conformance

import net.sansa_stack.inference.flink.forwardchaining.{ForwardRuleReasoner, ForwardRuleReasonerRDFS}
import net.sansa_stack.inference.rules.RDFSLevel
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.test.util.AbstractTestBase
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
  * Test context to share an RDFS reasoner.
  *
  * @author Lorenz Buehmann
  */
@RunWith(classOf[Parameterized])
trait SharedRDFSReasonerContext
    extends SharedReasonerContext[ForwardRuleReasonerRDFS] {
  self: Suite =>

  override def beforeAll(): Unit = {
    super.beforeAll()
    _reasoner = new ForwardRuleReasonerRDFS(env)
    _reasoner.level = RDFSLevel.SIMPLE
  }
}
