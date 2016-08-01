package org.sansa.inference.spark.conformance

import com.holdenkarau.spark.testing.SharedSparkContext
import org.sansa.inference.spark.forwardchaining.{ForwardRuleReasoner, ForwardRuleReasonerRDFS}
import org.scalatest.{BeforeAndAfterAll, Suite}

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
  }

}
