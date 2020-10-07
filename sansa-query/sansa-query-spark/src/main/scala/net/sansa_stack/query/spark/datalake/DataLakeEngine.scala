package net.sansa_stack.query.spark.datalake

import net.sansa_stack.datalake.spark.{Run, SparkExecutor}
import org.apache.spark.sql.{ DataFrame, SparkSession }

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
