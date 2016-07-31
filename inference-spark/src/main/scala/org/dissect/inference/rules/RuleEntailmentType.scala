package org.dissect.inference.rules

/**
  * The type of entailment of a rule, i.e. which kind of triples are involved in the entailment process.
  *
  * @author Lorenz Buehmann
  *
  */
object RuleEntailmentType extends Enumeration {

  type RuleEntailmentType = Value
  val ASSERTIONAL, TERMINOLOGICAL, HYBRID = Value

}
