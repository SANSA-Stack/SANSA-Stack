package org.dissect.inference.forwardchaining

import org.apache.jena.reasoner.rulesys.Rule
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.dissect.inference.data.{RDFGraphNative, RDFTriple}
import org.dissect.inference.rules._

import scala.language.{existentials, implicitConversions}

/**
  * An optimized implementation of the forward chaining based reasoner using Spark data structures and operations.
  *
  * @author Lorenz Buehmann
  */
class ForwardRuleReasonerOptimizedNative(sc: SparkContext, rules: Set[Rule])
  extends ForwardRuleReasonerOptimized[RDD[RDFTriple], RDFGraphNative](rules, new RuleExecutorNative(sc)){
}
