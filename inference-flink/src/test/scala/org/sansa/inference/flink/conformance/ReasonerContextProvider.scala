package org.sansa.inference.flink.conformance

import org.sansa.inference.flink.forwardchaining.ForwardRuleReasoner

/**
  * Provides a reasoner.
  *
  * @author Lorenz Buehmann
  */
trait ReasonerContextProvider {

  def reasoner: ForwardRuleReasoner

}
