package net.sansa_stack.inference.spark.rules

import org.apache.jena.graph.{Node, Triple}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import net.sansa_stack.inference.spark.data.model.RDFGraphNative
import net.sansa_stack.inference.spark.rules.plan.PlanExecutorNative

/**
  * A rule executor that works on Spark data structures and operations.
  *
  * @author Lorenz Buehmann
  */
class RuleExecutorNative(sc: SparkContext)
  extends RuleExecutor[RDD[Triple], Node, Triple, RDFGraphNative](new PlanExecutorNative(sc)) {

}
