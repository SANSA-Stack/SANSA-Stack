package net.sansa_stack.rdf.flink.partition.core

import net.sansa_stack.rdf.common.partition.core.RdfPartitioner
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.jena.graph.Triple

import scala.reflect.ClassTag

object RdfPartitionUtilsFlink extends Serializable {

  implicit def partitionGraph[S: TypeInformation: ClassTag](graphRdd: DataSet[Triple], partitioner: RdfPartitioner[S]): Map[S, DataSet[Product]] = {
    val map = Map(partitionGraphArray(graphRdd, partitioner): _*)
    map
  }

  implicit def partitionGraphArray[S: TypeInformation: ClassTag](graphRdd: DataSet[Triple], partitioner: RdfPartitioner[S]): Seq[(S, DataSet[Product])] = {
    val partitions = graphRdd.map(x => partitioner.fromTriple(x)).distinct.collect
    val array = partitions map { p =>
      (
        p,
        graphRdd
        .filter(t => partitioner.matches(p, t))
        // .map(t => new Row(p.layout.fromTriple(t).productArity)))
        .map(t => partitioner.determineLayout(p).fromTriple(t))
      // .persist())
      )
    }
    array
  }
}
