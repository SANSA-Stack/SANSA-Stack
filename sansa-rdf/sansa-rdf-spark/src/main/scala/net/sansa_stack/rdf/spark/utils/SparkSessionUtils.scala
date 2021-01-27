package net.sansa_stack.rdf.spark.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SparkSessionUtils {
  /** A seemingly clean way to unambiguously obtain a SparkSession from a given RDD */
  def getSessionFromRdd(rdd: RDD[_]): SparkSession = {
    SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
  }
}
