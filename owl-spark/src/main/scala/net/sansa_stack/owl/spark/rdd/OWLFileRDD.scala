package net.sansa_stack.owl.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext}


class OWLFilePartition(override val index: Int) extends Partition


/**
  * Abstract class to be implemented by all classes reading in OWL axioms from
  * any kind of serialization format
  */
abstract class OWLFileRDD(
                           sc: SparkContext,
                           override val filePath: String,
                           override val numPartitions: Int
                         ) extends RDD[String](sc, Nil) with OWLFileRDDTrait


/** Trait defining the main interface for classes handling OWL files */
trait OWLFileRDDTrait extends RDD[String] {
  def filePath: String
  def numPartitions: Int
  var ontURI: String = null

  override protected def getPartitions: Array[Partition] = {
    val partitions = new Array[Partition](numPartitions)

    for (i <- 0 until numPartitions) {
      partitions(i) = new OWLFilePartition(i)
    }

    partitions
  }
}
