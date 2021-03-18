package net.sansa_stack.inference.rules

import org.apache.jena.reasoner.rulesys.Rule

import net.sansa_stack.inference.utils.RuleUtils

/**
  * Predefined sets of rules.
  *
  * @author Lorenz Buehmann
  */
object RuleSets {

  lazy val RDFS_SIMPLE: Set[Rule] = RuleUtils.load("rules/rdfs-simple.rules").toSet

  lazy val OWL_HORST: Set[Rule] = RuleUtils.load("rules/owl_horst.rules").toSet

  lazy val OWL_RL: Set[Rule] = RuleUtils.load("rules/owl_rl.rules").toSet

}
