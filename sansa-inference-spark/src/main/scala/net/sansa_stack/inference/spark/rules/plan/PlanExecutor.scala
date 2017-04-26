package net.sansa_stack.inference.spark.rules.plan

import org.slf4j.LoggerFactory

import net.sansa_stack.inference.spark.data.model.AbstractRDFGraph

/**
  * An executor for a rule execution plan.
  *
  * @author Lorenz Buehmann
  */
abstract class PlanExecutor[V, G <: AbstractRDFGraph[V, G]]() {
  protected val logger = com.typesafe.scalalogging.Logger(LoggerFactory.getLogger(this.getClass.getName))

  def execute(plan: Plan, graph: G): G
}
