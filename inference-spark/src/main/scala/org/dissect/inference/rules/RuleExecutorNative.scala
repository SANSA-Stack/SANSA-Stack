package org.dissect.inference.rules

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.dissect.inference.data.{RDFGraphNative, RDFTriple}
import org.dissect.inference.rules.plan.PlanExecutorNative

/**
  * A rule executor that works on Spark data structures and operations.
  *
  * @author Lorenz Buehmann
  */
class RuleExecutorNative(sc: SparkContext) extends RuleExecutor[RDD[RDFTriple], RDFGraphNative](new PlanExecutorNative(sc)){

}
