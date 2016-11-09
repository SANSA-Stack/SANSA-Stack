package net.sansa_stack.owl.spark.dataset

import net.sansa_stack.owl.spark.rdd.FunctionalSyntaxOWLExpressionsRDDBuilder
import org.apache.spark.sql.{Dataset, SparkSession}

object FunctionalSyntaxOWLExpressionsDatasetBuilder {
  def build(spark: SparkSession, filePath: String): Dataset[String] = {
    build(spark, filePath, spark.sparkContext.defaultMinPartitions)
  }

  def build(spark: SparkSession, filePath: String, numPartitions: Int): Dataset[String] = {
    val rdd = FunctionalSyntaxOWLExpressionsRDDBuilder.build(spark.sparkContext, filePath)
    import spark.implicits._
    spark.sqlContext.createDataset[String](rdd)
  }
}
