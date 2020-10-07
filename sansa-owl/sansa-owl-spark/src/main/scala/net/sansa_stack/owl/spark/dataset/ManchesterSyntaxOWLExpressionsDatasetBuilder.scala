package net.sansa_stack.owl.spark.dataset

import org.apache.spark.sql.SparkSession

import net.sansa_stack.owl.common.parsing.{ ManchesterSyntaxParsing, ManchesterSyntaxPrefixParsing }
import net.sansa_stack.owl.spark.rdd.ManchesterSyntaxOWLExpressionsRDDBuilder

object ManchesterSyntaxOWLExpressionsDatasetBuilder extends ManchesterSyntaxPrefixParsing {
  def build(spark: SparkSession, filePath: String): OWLExpressionsDataset = {
    buildAndGetDefaultPrefix(spark, filePath)._1
  }

  private[dataset] def buildAndGetDefaultPrefix(spark: SparkSession, filePath: String): (OWLExpressionsDataset, String) = {
    val res =
      ManchesterSyntaxOWLExpressionsRDDBuilder.buildAndGetPrefixes(spark, filePath)
    val rdd = res._1
    val defaultPrefix = res._2.getOrElse(ManchesterSyntaxParsing._empty, ManchesterSyntaxParsing.dummyURI)

    import spark.implicits._
    (spark.createDataset[String](rdd), defaultPrefix)
  }
}
