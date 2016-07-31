package org.sansa.inference.spark.rules

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.sansa.inference.spark.data.RDFGraphDataFrame
import org.sansa.inference.spark.rules.plan.PlanExecutorSQL

/**
  * A rule executor that works on SQL and Spark DataFrames.
  *
  * @author Lorenz Buehmann
  */
class RuleExecutorSQL(sparkSession: SparkSession) extends RuleExecutor[DataFrame, RDFGraphDataFrame](new PlanExecutorSQL(sparkSession)){

}
