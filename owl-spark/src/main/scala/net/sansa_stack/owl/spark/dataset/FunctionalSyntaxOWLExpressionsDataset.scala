package net.sansa_stack.owl.spark.dataset

import net.sansa_stack.owl.spark.rdd.FunctionalSyntaxOWLExpressionsRDD
import org.apache.spark.sql.{Dataset, SparkSession}

object FunctionalSyntaxOWLExpressionsDataset {
  def read(spark: SparkSession, filePath: String): Dataset[String] = {
    read(spark, filePath, spark.sparkContext.defaultMinPartitions)
  }

  def read(spark: SparkSession, filePath: String, numPartitions: Int): Dataset[String] = {
    val rdd = new FunctionalSyntaxOWLExpressionsRDD(spark.sparkContext, filePath, numPartitions)
    import spark.implicits._
    spark.sqlContext.createDataset[String](rdd)
  }
}
