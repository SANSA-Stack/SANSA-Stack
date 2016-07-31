package org.sansa.inference.spark.rules

import org.apache.jena.reasoner.rulesys.Rule
import org.sansa.inference.spark.data.AbstractRDFGraph
import org.sansa.inference.spark.rules.plan.PlanExecutor

/**
  * A rule executor that works on Spark data structures and operations.
  *
  * @author Lorenz Buehmann
  */
class RuleExecutor[V, G <: AbstractRDFGraph[V, G]](planExecutor: PlanExecutor[V, G]) {

  val planGenerator = Planner

  def execute(rule: Rule, graph: G): G = {

    // generate execution plan
    val plan = planGenerator.generatePlan(rule)

    // apply rule
    val result = planExecutor.execute(plan, graph)

    result
  }
}
