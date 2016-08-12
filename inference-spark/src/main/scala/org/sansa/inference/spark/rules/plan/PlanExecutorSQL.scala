package org.sansa.inference.spark.rules.plan

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.sansa.inference.spark.data.RDFGraphDataFrame
import org.slf4j.LoggerFactory

/**
  * An executor that works on Spark DataFrames.
  *
  * @author Lorenz Buehmann
  */
class PlanExecutorSQL(sparkSession: SparkSession) extends PlanExecutor[DataFrame, RDFGraphDataFrame]{
  override val logger = com.typesafe.scalalogging.slf4j.Logger(LoggerFactory.getLogger(this.getClass.getName))

  def execute(plan: Plan, graph: RDFGraphDataFrame): RDFGraphDataFrame = {
    // generate SQL query
    val sql = plan.toSQL
    logger.info(s"SQL Query:\n$sql")

    val tmpName = "TEMP_TRIPLES"

    // execute the query
    // we have to register the Dataframe as a temp table on which the SQL query will be executed
    graph.toDataFrame(sparkSession).createTempView(tmpName)
    val results = sparkSession.sql(sql.replace("TRIPLES", tmpName))
    // unregister the temp table
    sparkSession.sqlContext.dropTempTable(tmpName)

//    println(results.explain(true))
    new RDFGraphDataFrame(results)
  }
}
