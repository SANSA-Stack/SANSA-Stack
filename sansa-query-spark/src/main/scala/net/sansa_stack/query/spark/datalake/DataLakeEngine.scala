package net.sansa_stack.query.spark.DataLakeEngine

object DataLakeEngine {

  def run(sparqlQuery: String, mappingsFile: String, configFile: String, spark: SparkSession): DataFrame = {

    val executor : SparkExecutor = new SparkExecutor(spark, mappingsFile)

    val run = new Run[DataFrame](executor)

    run.application(queryFile, mappingsFile, configFile)
  }

}
