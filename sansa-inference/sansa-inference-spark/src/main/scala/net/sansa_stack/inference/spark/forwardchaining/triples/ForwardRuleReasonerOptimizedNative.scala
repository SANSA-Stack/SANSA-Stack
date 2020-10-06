package net.sansa_stack.inference.spark.forwardchaining.triples

import net.sansa_stack.inference.data.Jena
import net.sansa_stack.inference.spark.data.model.RDFGraphNative
import net.sansa_stack.inference.spark.rules.RuleExecutorNative
import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.reasoner.rulesys.Rule
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.language.{existentials, implicitConversions}

/**
  * An optimized implementation of the forward chaining based reasoner using Spark data structures and operations.
  *
  * @author Lorenz Buehmann
  */
class ForwardRuleReasonerOptimizedNative(sparkSession: SparkSession, rules: Set[Rule])
  extends ForwardRuleReasonerOptimized[Jena, RDD[Triple], Node, Triple, RDFGraphNative](
    sparkSession, rules, new RuleExecutorNative(sparkSession.sparkContext)) {
}
