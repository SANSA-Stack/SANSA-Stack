package net.sansa_stack.owl.spark.dataset

import net.sansa_stack.owl.spark.rdd.FunctionalSyntaxOWLFileRDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SparkSession}


object FunctionalSyntaxOWLFileDataset extends OWLFileDataset {

  def apply(sc: SparkContext, filePath: String, numPartitions: Int): Dataset[String] = {
    val ss: SparkSession =
      SparkSession.builder().appName(sc.appName).config(sc.getConf).getOrCreate()
    this(ss, filePath, numPartitions)
  }

  def apply(ss: SparkSession, filePath: String, numPartitions: Int): Dataset[String] = {
    val rdd = new FunctionalSyntaxOWLFileRDD(ss.sparkContext, filePath, numPartitions)
    import ss.implicits._
    ss.sqlContext.createDataset[String](rdd)
  }
}
