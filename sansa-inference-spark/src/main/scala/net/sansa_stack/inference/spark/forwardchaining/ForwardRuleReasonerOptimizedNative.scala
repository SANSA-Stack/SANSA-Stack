package net.sansa_stack.inference.spark.forwardchaining

import scala.language.{existentials, implicitConversions}

import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.reasoner.rulesys.Rule
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import net.sansa_stack.inference.spark.data.model.RDFGraphNative
import net.sansa_stack.inference.spark.rules.RuleExecutorNative

/**
  * An optimized implementation of the forward chaining based reasoner using Spark data structures and operations.
  *
  * @author Lorenz Buehmann
  */
class ForwardRuleReasonerOptimizedNative(sparkSession: SparkSession, rules: Set[Rule])
  extends ForwardRuleReasonerOptimized[RDD[Triple], Node, Triple, RDFGraphNative](
    sparkSession, rules, new RuleExecutorNative(sparkSession.sparkContext)) {
}
