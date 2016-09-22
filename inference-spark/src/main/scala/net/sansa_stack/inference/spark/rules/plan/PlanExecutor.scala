package net.sansa_stack.inference.spark.rules.plan

import org.apache.spark.sql.{DataFrame, SQLContext}
import net.sansa_stack.inference.spark.data.RDFGraph
import net.sansa_stack.inference.spark.data.AbstractRDFGraph
import org.slf4j.LoggerFactory

/**
  * An executor for a rule execution plan.
  *
  * @author Lorenz Buehmann
  */
abstract class PlanExecutor[V, G <: AbstractRDFGraph[V, G]]() {
  protected val logger = com.typesafe.scalalogging.slf4j.Logger(LoggerFactory.getLogger(this.getClass.getName))

  def execute(plan: Plan, graph: G): G
}
