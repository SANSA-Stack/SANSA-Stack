package net.sansa_stack.inference.spark.conformance

import com.holdenkarau.spark.testing.SharedSparkContext
import net.sansa_stack.inference.rules.RDFSLevel
import net.sansa_stack.inference.spark.forwardchaining.triples.{ForwardRuleReasoner, ForwardRuleReasonerRDFS}
import org.scalatest.Suite

/**
  * Test context to share an RDFS reasoner.
  *
  * @author Lorenz Buehmann
  */
trait SharedRDFSReasonerContext extends SharedSparkContext with ReasonerContextProvider{
  self: Suite =>

  @transient private var _reasoner: ForwardRuleReasonerRDFS = _

  override def reasoner: ForwardRuleReasoner = _reasoner

  override def beforeAll(): Unit = {
    super.beforeAll()
    _reasoner = new ForwardRuleReasonerRDFS(sc)
    _reasoner.level = RDFSLevel.SIMPLE
  }

}
