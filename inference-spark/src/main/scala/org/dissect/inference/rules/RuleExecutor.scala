package org.dissect.inference.rules

import org.apache.jena.reasoner.rulesys.Rule
import org.apache.spark.SparkContext
import org.dissect.inference.data.{AbstractRDFGraph, RDFGraph}
import org.dissect.inference.rules.plan.{PlanExecutor, PlanExecutorNative}

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
