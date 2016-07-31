package org.dissect.inference.rules

import org.dissect.inference.utils.RuleUtils

/**
  * @author Lorenz Buehmann
  */
object PlanTest {


  def main(args: Array[String]) {
    val rules = RuleUtils.load("test.rules")

    var rule = RuleUtils.byName(rules, "rdfs2").get
//    Planner.rewrite(rule)

    rule = RuleUtils.byName(rules, "prp-trp").get
    val plan = Planner.generatePlan(rule)
    println(plan)
  }

}
