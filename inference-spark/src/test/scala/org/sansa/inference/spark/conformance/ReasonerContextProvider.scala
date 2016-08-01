package org.sansa.inference.spark.conformance

import org.sansa.inference.spark.forwardchaining.ForwardRuleReasoner

/**
  * Provides a reasoner.
  *
  * @author Lorenz Buehmann
  */
trait ReasonerContextProvider {

  def reasoner: ForwardRuleReasoner

}
