package net.sansa_stack.inference.spark.rules

import org.apache.jena.reasoner.rulesys.Rule

import net.sansa_stack.inference.data.RDF
import net.sansa_stack.inference.spark.data.model.AbstractRDFGraphSpark
import net.sansa_stack.inference.spark.rules.plan.PlanExecutor

/**
  * A rule executor that works on Spark data structures and operations.
  *
  * @author Lorenz Buehmann
  */
class RuleExecutor[Rdf <: RDF, D, N <: Rdf#Node, T <: Rdf#Triple, G <: AbstractRDFGraphSpark[Rdf, D, G]](
  planExecutor: PlanExecutor[Rdf, D, N, T, G]
) {

  val planGenerator = Planner

  def execute(rule: Rule, graph: G): G = {

    // generate execution plan
    val plan = planGenerator.generatePlan(rule)

    // apply rule
    val result = planExecutor.execute(plan, graph)

    result
  }
}
