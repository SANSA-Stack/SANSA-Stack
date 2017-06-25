package net.sansa_stack.inference.spark.rules.plan

import org.slf4j.LoggerFactory

import net.sansa_stack.inference.data.RDF
import net.sansa_stack.inference.spark.data.model.AbstractRDFGraphSpark

/**
  * An executor for a rule execution plan.
  *
  * @author Lorenz Buehmann
  */
abstract class PlanExecutor[D[C], N <: RDF#Node, T <: RDF#Triple, G <: AbstractRDFGraphSpark[D, N, T, G]]() {
  protected val logger = com.typesafe.scalalogging.Logger(LoggerFactory.getLogger(this.getClass.getName))

  def execute(plan: Plan, graph: G): G
}
