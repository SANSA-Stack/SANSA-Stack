package org.sansa.inference.spark.forwardchaining

import org.apache.jena.reasoner.rulesys.Rule
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.sansa.inference.spark.data.RDFGraphDataFrame
import org.sansa.inference.spark.rules.RuleExecutorSQL

import scala.language.{existentials, implicitConversions}

/**
  * An optimized implementation of the forward chaining based reasoner using Spark DataFrames.
  *
  * @author Lorenz Buehmann
  */
class ForwardRuleReasonerOptimizedSQL(sparkSession: SparkSession, rules: Set[Rule])
  extends ForwardRuleReasonerOptimized[DataFrame, RDFGraphDataFrame](rules, new RuleExecutorSQL(sparkSession)){
}
