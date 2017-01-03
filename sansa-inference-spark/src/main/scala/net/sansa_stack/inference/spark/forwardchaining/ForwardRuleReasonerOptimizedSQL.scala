package net.sansa_stack.inference.spark.forwardchaining

import scala.language.{existentials, implicitConversions}

import org.apache.jena.reasoner.rulesys.Rule
import org.apache.spark.sql.{DataFrame, SparkSession}

import net.sansa_stack.inference.spark.data.RDFGraphDataFrame
import net.sansa_stack.inference.spark.rules.RuleExecutorSQL

/**
  * An optimized implementation of the forward chaining based reasoner using Spark DataFrames.
  *
  * @author Lorenz Buehmann
  */
class ForwardRuleReasonerOptimizedSQL(sparkSession: SparkSession, rules: Set[Rule])
  extends ForwardRuleReasonerOptimized[DataFrame, RDFGraphDataFrame](
    sparkSession, rules, new RuleExecutorSQL(sparkSession)) {
}
