package net.sansa_stack.inference.spark.rules.plan

import org.slf4j.LoggerFactory

import net.sansa_stack.inference.data.RDF
import net.sansa_stack.inference.spark.data.model.AbstractRDFGraphSpark

/**
  * An executor for a rule execution plan.
  *
  * @author Lorenz Buehmann
  */
abstract class PlanExecutor[Rdf <: RDF, D, N <: Rdf#Node, T <: Rdf#Triple, G <: AbstractRDFGraphSpark[Rdf, D, G]]() {
  protected val logger = com.typesafe.scalalogging.Logger(LoggerFactory.getLogger(this.getClass.getName))

  def execute(plan: Plan, graph: G): G
}
