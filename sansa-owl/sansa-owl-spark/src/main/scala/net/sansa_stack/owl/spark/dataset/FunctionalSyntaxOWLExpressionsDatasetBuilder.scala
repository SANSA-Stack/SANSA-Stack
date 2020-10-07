package net.sansa_stack.owl.spark.dataset

import org.apache.spark.sql.SparkSession

import net.sansa_stack.owl.spark.rdd.FunctionalSyntaxOWLExpressionsRDDBuilder


object FunctionalSyntaxOWLExpressionsDatasetBuilder {
  def build(spark: SparkSession, filePath: String): OWLExpressionsDataset = {
    val rdd = FunctionalSyntaxOWLExpressionsRDDBuilder.build(spark, filePath)
    import spark.implicits._
    spark.createDataset[String](rdd)
  }
}
