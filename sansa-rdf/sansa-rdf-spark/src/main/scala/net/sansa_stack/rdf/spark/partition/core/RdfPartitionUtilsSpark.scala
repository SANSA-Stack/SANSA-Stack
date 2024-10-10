package net.sansa_stack.rdf.spark.partition.core

import net.sansa_stack.rdf.common.partition.core.RdfPartitioner
import org.apache.jena.graph.Triple
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.reflect.ClassTag

object RdfPartitionUtilsSpark
  extends Serializable {

  val logger = com.typesafe.scalalogging.Logger(RdfPartitionUtilsSpark.getClass)

  implicit def partitionGraph[S: ClassTag](graphRdd: RDD[Triple],
                                           partitioner: RdfPartitioner[S],
                                           literalLanguageTagStrategy: LiteralLanguageTagStrategy.Value = LiteralLanguageTagStrategy.SINGLE_COLUMN)
  : Map[S, RDD[Row]] = {
    Map(partitionGraphArray(graphRdd, partitioner, literalLanguageTagStrategy): _*)
  }

  implicit def partitionGraphArray[S: ClassTag](graphRdd: RDD[Triple],
                                                partitioner: RdfPartitioner[S],
                                                literalLanguageTagStrategy: LiteralLanguageTagStrategy.Value): Array[(S, RDD[Row])] = {
    logger.info("started vertical partitioning of the data ...")
    var partitions = graphRdd.map(partitioner.fromTriple).distinct().collect()

    if (literalLanguageTagStrategy == LiteralLanguageTagStrategy.SINGLE_COLUMN) {
      partitions = partitioner.aggregate(partitions).toArray
    }

    val array = partitions map { p =>
      (
        p,
        graphRdd
          .filter(t => partitioner.matches(p, t))
          .map(t => Row(partitioner.determineLayout(p).fromTriple(t).productIterator.toList: _*))
          //          .map(t => Row(p.layout.fromTriple(t).productIterator.toList: _*))
          .persist())
    }
    logger.info("... finished vertical partitioning of the data.")

    array
  }
}
