package net.sansa_stack.inference.spark.rules

import net.sansa_stack.inference.spark.rules.plan.PlanExecutor
import org.apache.jena.reasoner.rulesys.Rule
import net.sansa_stack.inference.spark.data.AbstractRDFGraph

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
