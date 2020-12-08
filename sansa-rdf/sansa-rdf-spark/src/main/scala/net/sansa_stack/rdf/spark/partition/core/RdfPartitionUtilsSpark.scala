package net.sansa_stack.rdf.spark.partition.core

import scala.reflect.ClassTag

import org.apache.jena.graph.Triple
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import net.sansa_stack.rdf.common.partition.core.{RdfPartition, RdfPartitioner, RdfPartitionerDefault}

object RdfPartitionUtilsSpark extends Serializable {

  val logger = com.typesafe.scalalogging.Logger(RdfPartitionUtilsSpark.getClass)

  implicit def partitionGraph[P <: RdfPartition : ClassTag](graphRdd: RDD[Triple],
                                                            partitioner: RdfPartitioner[P] = RdfPartitionerDefault): Map[P, RDD[Row]] = {
    Map(partitionGraphArray(graphRdd, partitioner): _*)
  }

  implicit def partitionGraphArray[P <: RdfPartition : ClassTag](graphRdd: RDD[Triple],
                                                                 partitioner: RdfPartitioner[P] = RdfPartitionerDefault): Array[(P, RDD[Row])] = {
    logger.info("started vertical partitioning of the data ...")
    val partitions = graphRdd.map(partitioner.fromTriple).distinct.collect

    val array = partitions map { p =>
      (
        p,
        graphRdd
          .filter(p.matches)
          .map(t => Row(p.layout.fromTriple(t).productIterator.toList: _*))
//          .map(t => Row(p.layout.fromTriple(t).productIterator.toList: _*))
          .persist())
    }
    logger.info("... finished vertical partitioning of the data.")

    array
  }
}
