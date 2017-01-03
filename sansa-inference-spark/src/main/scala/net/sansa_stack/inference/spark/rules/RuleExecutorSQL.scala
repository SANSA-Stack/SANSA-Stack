package net.sansa_stack.inference.spark.rules

import org.apache.spark.sql.{DataFrame, SparkSession}

import net.sansa_stack.inference.spark.data.RDFGraphDataFrame
import net.sansa_stack.inference.spark.rules.plan.PlanExecutorSQL

/**
  * A rule executor that works on SQL and Spark DataFrames.
  *
  * @author Lorenz Buehmann
  */
class RuleExecutorSQL(sparkSession: SparkSession)
  extends RuleExecutor[DataFrame, RDFGraphDataFrame](new PlanExecutorSQL(sparkSession)) {

}
