package net.sansa_stack.owl.spark.dataset

import net.sansa_stack.owl.common.parsing.ManchesterSyntaxPrefixParsing
import net.sansa_stack.owl.spark.rdd.ManchesterSyntaxOWLExpressionsRDDBuilder
import org.apache.spark.sql.SparkSession


object ManchesterSyntaxOWLExpressionsDatasetBuilder extends ManchesterSyntaxPrefixParsing {
  def build(spark: SparkSession, filePath: String): OWLExpressionsDataset = {
    val rdd = ManchesterSyntaxOWLExpressionsRDDBuilder.build(spark.sparkContext, filePath)
    import spark.implicits._
    spark.sqlContext.createDataset[String](rdd)
  }
}
