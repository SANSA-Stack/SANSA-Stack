package net.sansa_stack.rdf.flink.partition.core

import scala.reflect.ClassTag

import net.sansa_stack.rdf.common.partition.core.{ RdfPartition, RdfPartitioner, RdfPartitionerDefault }
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.types.Row
import org.apache.jena.graph.Triple

object RdfPartitionUtilsFlink extends Serializable {

  implicit def partitionGraph[T <: RdfPartition: TypeInformation: ClassTag](graphRdd: DataSet[Triple], partitioner: RdfPartitioner[T] = RdfPartitionerDefault): Map[T, DataSet[Product]] = {
    val map = Map(partitionGraphArray(graphRdd, partitioner): _*)
    map
  }

  implicit def partitionGraphArray[T <: RdfPartition: TypeInformation: ClassTag](graphRdd: DataSet[Triple], partitioner: RdfPartitioner[T] = RdfPartitionerDefault): Seq[(T, DataSet[Product])] = {
    val partitions = graphRdd.map(x => partitioner.fromTriple(x)).distinct.collect
    val array = partitions map { p =>
      (
        p,
        graphRdd
        .filter(t => p.matches(t))
        // .map(t => new Row(p.layout.fromTriple(t).productArity)))
        .map(t => p.layout.fromTriple(t))
      // .persist())
      )
    }
    array
  }
}
