package net.sansa_stack.query.spark.datalake

import org.apache.spark.sql.{ DataFrame, SparkSession }
import net.sansa_stack.datalake.spark.SparkExecutor
import net.sansa_stack.datalake.spark.Run

object DataLakeEngine {

  /**
   * Run the sparkAll query engine
   * @param queryFile the file containing queries or a single query
   * @param mappingsFile the mappings to the target sources
   * @param configFile configuration file for different data sources
   * @spark the Spark Session instance
   */
  def run(queryFile: String, mappingsFile: String, configFile: String, spark: SparkSession): DataFrame = {

    val executor: SparkExecutor = new SparkExecutor(spark, mappingsFile)

    val run = new Run[DataFrame](executor)

    run.application(queryFile, mappingsFile, configFile)
  }

}
