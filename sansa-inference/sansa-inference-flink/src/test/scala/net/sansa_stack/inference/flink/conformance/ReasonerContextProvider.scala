package net.sansa_stack.inference.flink.conformance

import net.sansa_stack.inference.flink.forwardchaining.ForwardRuleReasoner
import net.sansa_stack.inference.flink.forwardchaining.ForwardRuleReasoner

/**
  * Provides a reasoner.
  *
  * @author Lorenz Buehmann
  */
trait ReasonerContextProvider {

  def reasoner: ForwardRuleReasoner

}
