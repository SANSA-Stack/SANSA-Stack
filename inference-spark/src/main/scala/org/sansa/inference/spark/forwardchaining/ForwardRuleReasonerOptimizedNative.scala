package org.sansa.inference.spark.forwardchaining

import org.apache.jena.reasoner.rulesys.Rule
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.sansa.inference.data.RDFTriple
import org.sansa.inference.spark.data.RDFGraphNative
import org.sansa.inference.spark.rules.RuleExecutorNative

import scala.language.{existentials, implicitConversions}

/**
  * An optimized implementation of the forward chaining based reasoner using Spark data structures and operations.
  *
  * @author Lorenz Buehmann
  */
class ForwardRuleReasonerOptimizedNative(sparkSession: SparkSession, rules: Set[Rule])
  extends ForwardRuleReasonerOptimized[RDD[RDFTriple], RDFGraphNative](sparkSession, rules, new RuleExecutorNative(sparkSession.sparkContext)){
}
