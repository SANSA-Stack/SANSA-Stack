package net.sansa_stack.owl.spark.dataset

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SparkSession}


abstract class OWLFileDataset {
  def apply(sc: SparkContext, filePath: String, numPartitions: Int): Dataset[String]
  def apply(ss: SparkSession, filePath: String, numPartitions: Int): Dataset[String]
}
