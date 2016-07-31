package org.dissect.inference.forwardchaining

import org.apache.jena.reasoner.rulesys.Rule
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.dissect.inference.data.{RDFGraph, RDFGraphDataFrame, RDFGraphNative, RDFTriple}
import org.dissect.inference.rules.{RuleDependencyGraphAnalyzer, RuleDependencyGraphGenerator, RuleExecutorNative, RuleExecutorSQL}
import org.slf4j.LoggerFactory

import scala.language.{existentials, implicitConversions}
import scalax.collection.Graph
import scalax.collection.GraphEdge.DiEdge

/**
  * An optimized implementation of the forward chaining based reasoner using Spark DataFrames.
  *
  * @author Lorenz Buehmann
  */
class ForwardRuleReasonerOptimizedSQL(sparkSession: SparkSession, rules: Set[Rule])
  extends ForwardRuleReasonerOptimized[DataFrame, RDFGraphDataFrame](rules, new RuleExecutorSQL(sparkSession)){
}
