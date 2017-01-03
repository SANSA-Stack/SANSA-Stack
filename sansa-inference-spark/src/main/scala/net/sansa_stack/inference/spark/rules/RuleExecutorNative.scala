package net.sansa_stack.inference.spark.rules

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import net.sansa_stack.inference.data.RDFTriple
import net.sansa_stack.inference.spark.data.RDFGraphNative
import net.sansa_stack.inference.spark.rules.plan.PlanExecutorNative

/**
  * A rule executor that works on Spark data structures and operations.
  *
  * @author Lorenz Buehmann
  */
class RuleExecutorNative(sc: SparkContext)
  extends RuleExecutor[RDD[RDFTriple], RDFGraphNative](new PlanExecutorNative(sc)) {

}
